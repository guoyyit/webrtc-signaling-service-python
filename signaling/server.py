"""Signaling coordinator: message routing, ordered forwarding and tracing.

The server is transport-agnostic: it operates on Session objects and
produces outbound frames. A thin WebSocket adapter (in ``main.py``) moves
frames on and off the wire. All decisions use an injectable clock so
timeouts and retries are deterministic in tests.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Any, Mapping, Optional

from .auth import AuthManager
from .models import (
    HOST_ONLY_MESSAGES,
    VIEWER_FORBIDDEN_MESSAGES,
    LinkMode,
    MemberStatus,
    MessageType,
    Role,
    SignalingMessage,
    UnknownMessageTypeError,
)
from .negotiation import NegotiationTracker
from .relay import RelayCoordinator
from .room import RoomFullError, RoomManager
from .session import Session, SessionManager

logger = logging.getLogger("signaling")


@dataclass
class ServerEvent:
    """An outbound event produced by processing one inbound frame."""

    kind: str  # "message" | "close"
    message: Optional[SignalingMessage] = None
    session_id: Optional[str] = None
    reason: str = ""


@dataclass
class SignalingServer:
    """Coordinates rooms, sessions, negotiations and relay state."""

    config: Any
    auth: AuthManager
    rooms: RoomManager
    sessions: SessionManager
    negotiations: NegotiationTracker
    relay: RelayCoordinator
    clock: Any = time.monotonic
    _seq_counters: dict[str, int] = field(default_factory=dict)

    # -- connection lifecycle --------------------------------------------

    def open_session(
        self,
        token: str,
        room_id: str,
        session_id: str,
    ) -> tuple[Optional[Session], list[ServerEvent]]:
        """Authenticate and register a new connection.

        Returns (session, events). On failure the session is None and an
        error frame is returned.
        """
        now = self.clock()
        claims = self.auth.verify_token(token, room_id, now)
        if claims is None:
            self._trace("auth_rejected", room_id=room_id, session_id=session_id)
            return None, [self._error(None, "unauthorized", "invalid or expired credential")]

        room = self.rooms.get_room(room_id)
        if room is None:
            return None, [self._error(None, "room_not_found", f"room {room_id} does not exist")]

        # Idempotent membership: a repeated join / reconnect maps to one
        # member. If the member already has a live connection it is replaced.
        events: list[ServerEvent] = []
        old = self.sessions.get_by_member(room_id, claims.member_id)
        if old is not None:
            old.close()
            self.sessions.remove(old.session_id)
            events.append(
                ServerEvent(kind="close", session_id=old.session_id, reason="replaced")
            )
            self._trace("session_replaced", room_id=room_id, member_id=claims.member_id)

        try:
            self.rooms.join_room(room_id, claims.member_id, claims.role, now)
        except RoomFullError:
            return None, [self._error(None, "room_full", f"room {room_id} is at capacity")]
        self.rooms.set_status(room_id, claims.member_id, MemberStatus.ONLINE)

        session = self.sessions.create(
            session_id, claims.member_id, room_id, claims.role, now
        )

        snapshot = SignalingMessage(
            type=MessageType.JOINED,
            room_id=room_id,
            sender_id="server",
            target_id=claims.member_id,
            payload={
                "members": [
                    {
                        "member_id": m.member_id,
                        "role": m.role.value,
                        "status": m.status.value,
                    }
                    for m in self.rooms.list_members(room_id)
                ]
            },
            seq=self._next_seq(room_id),
            timestamp=now,
        )
        if not session.enqueue(snapshot):
            return None, [self._error(None, "internal", "session unavailable")]
        events.append(ServerEvent(kind="message", message=snapshot))

        # Notify other members; the new member already has the snapshot.
        joined = SignalingMessage(
            type=MessageType.MEMBER_JOINED,
            room_id=room_id,
            sender_id="server",
            target_id=None,
            payload={"member_id": claims.member_id, "role": claims.role.value},
            seq=self._next_seq(room_id),
            timestamp=now,
        )
        isolated = self.sessions.broadcast(room_id, joined, exclude=claims.member_id)
        events.append(ServerEvent(kind="message", message=joined))
        for member_id in isolated:
            events.extend(self._evict(room_id, member_id, now, reason="slow_consumer"))

        self._trace("member_joined", room_id=room_id, member_id=claims.member_id, role=claims.role.value)
        return session, events

    def close_session(self, session_id: str, reason: str = "") -> list[ServerEvent]:
        """Tear down a connection, emitting member-left events."""
        session = self.sessions.remove(session_id)
        if session is None:
            return []
        return self._remove_member(session.room_id, session.member_id, self.clock(), reason)

    def _remove_member(
        self, room_id: str, member_id: str, now: float, reason: str
    ) -> list[ServerEvent]:
        events: list[ServerEvent] = []
        # Identify the live session before removing membership so the
        # transport can close the matching websocket.
        sess = self.sessions.get_by_member(room_id, member_id)
        if sess is not None:
            sess.close()
            self.sessions.remove(sess.session_id)
        self.rooms.leave_room(room_id, member_id)
        self.relay.remove_peer(room_id, member_id)

        left = SignalingMessage(
            type=MessageType.MEMBER_LEFT,
            room_id=room_id,
            sender_id="server",
            target_id=None,
            payload={"member_id": member_id, "reason": reason},
            seq=self._next_seq(room_id),
            timestamp=now,
        )
        isolated = self.sessions.broadcast(room_id, left)
        events.append(ServerEvent(kind="message", message=left))
        for mid in isolated:
            if mid != member_id:
                events.extend(self._evict(room_id, mid, now, reason="slow_consumer"))

        events.append(
            ServerEvent(
                kind="close",
                session_id=sess.session_id if sess is not None else None,
                reason=reason,
            )
        )
        self._trace("member_left", room_id=room_id, member_id=member_id, reason=reason)
        return events

    def _evict(
        self, room_id: str, member_id: str, now: float, reason: str
    ) -> list[ServerEvent]:
        """Evict a slow/unresponsive member deterministically."""
        session = self.sessions.get_by_member(room_id, member_id)
        if session is None:
            return []
        session.close()
        sid = session.session_id
        self.sessions.remove(sid)
        self._trace("member_evicted", room_id=room_id, member_id=member_id, reason=reason)
        events = self._remove_member(room_id, member_id, now, reason)
        events.append(ServerEvent(kind="close", session_id=sid, reason=reason))
        return events

    # -- inbound frame handling -------------------------------------------

    def handle_frame(
        self,
        session_id: str,
        raw: Mapping[str, Any],
    ) -> list[ServerEvent]:
        """Process one raw frame from a session."""
        session = self.sessions.get(session_id)
        if session is None or session.closed:
            return []

        try:
            msg = SignalingMessage.from_wire(raw)
        except UnknownMessageTypeError as exc:
            self._trace("unknown_message_type", session_id=session_id, error=str(exc))
            return [self._error(session, "unknown_message_type", str(exc))]
        except (ValueError, KeyError) as exc:
            self._trace("bad_frame", session_id=session_id, error=str(exc))
            return [self._error(session, "bad_frame", "malformed frame")]

        now = self.clock()
        msg.room_id = session.room_id
        msg.sender_id = session.member_id

        # Role-based access control.
        if msg.type in HOST_ONLY_MESSAGES and session.role != Role.HOST:
            self._trace(
                "forbidden_control",
                room_id=session.room_id,
                member_id=session.member_id,
                type=msg.type.value,
            )
            return [self._error(session, "forbidden", f"{msg.type.value} requires host role")]
        if msg.type in VIEWER_FORBIDDEN_MESSAGES and session.role == Role.VIEWER:
            self._trace(
                "forbidden_send",
                room_id=session.room_id,
                member_id=session.member_id,
                type=msg.type.value,
            )
            return [self._error(session, "forbidden", f"viewers cannot send {msg.type.value}")]

        if msg.type == MessageType.PING:
            session.mark_heartbeat(now)
            pong = SignalingMessage(
                type=MessageType.PONG,
                room_id=session.room_id,
                sender_id="server",
                target_id=session.member_id,
                payload={"echo": msg.payload.get("echo")},
                seq=self._next_seq(session.room_id),
                timestamp=now,
            )
            session.enqueue(pong)
            return [ServerEvent(kind="message", message=pong)]

        if msg.type == MessageType.JOIN:
            # Idempotent re-join: re-issue the member snapshot.
            return self._snapshot(session, now)

        if msg.type == MessageType.LEAVE:
            return self._remove_member(session.room_id, session.member_id, now, "leave")

        if msg.type in (
            MessageType.OFFER,
            MessageType.ANSWER,
            MessageType.ICE_CANDIDATE,
            MessageType.RENEGOTIATE,
        ):
            return self._handle_negotiation(session, msg, now)

        if msg.type == MessageType.LINK_REPORT:
            return self._handle_link_report(session, msg, now)

        if msg.type == MessageType.KICK:
            return self._handle_kick(session, msg, now)

        # Any other server-only type sent by a client is invalid.
        self._trace("invalid_message_type", session_id=session_id, type=msg.type.value)
        return [self._error(session, "invalid_message_type", f"clients cannot send {msg.type.value}")]

    def _snapshot(self, session: Session, now: float) -> list[ServerEvent]:
        snapshot = SignalingMessage(
            type=MessageType.JOINED,
            room_id=session.room_id,
            sender_id="server",
            target_id=session.member_id,
            payload={
                "members": [
                    {
                        "member_id": m.member_id,
                        "role": m.role.value,
                        "status": m.status.value,
                    }
                    for m in self.rooms.list_members(session.room_id)
                ]
            },
            seq=self._next_seq(session.room_id),
            timestamp=now,
        )
        session.enqueue(snapshot)
        return [ServerEvent(kind="message", message=snapshot)]

    def _handle_negotiation(
        self, session: Session, msg: SignalingMessage, now: float
    ) -> list[ServerEvent]:
        target = msg.target_id
        if not target:
            return [self._error(session, "bad_target", "target_id is required")]
        if target == session.member_id:
            return [self._error(session, "invalid_target", "cannot negotiate with yourself")]
        if not self.rooms.get_room(session.room_id).has_member(target):
            return [self._error(session, "target_not_found", f"member {target} is not in the room")]

        events: list[ServerEvent] = []
        decision = None
        if msg.type == MessageType.OFFER:
            decision = self.negotiations.on_offer(
                session.room_id, session.member_id, target, msg.seq, now
            )
        elif msg.type == MessageType.ANSWER:
            decision = self.negotiations.on_answer(
                session.room_id, session.member_id, target, msg.seq, now
            )
        elif msg.type == MessageType.ICE_CANDIDATE:
            decision = self.negotiations.on_ice_candidate(
                session.room_id, session.member_id, target, msg.seq, now
            )
        elif msg.type == MessageType.RENEGOTIATE:
            decision = self.negotiations.on_renegotiate(
                session.room_id, session.member_id, target, now
            )

        if decision is not None and not decision.forward and decision.abandon_target is None:
            # Duplicate / stale frame: drop without disturbing negotiation.
            self._trace(
                "frame_dropped",
                room_id=session.room_id,
                type=msg.type.value,
                reason=decision.reason,
            )
            return events

        if decision is not None and decision.abandon_target:
            # The abandoned peer must know which counterparty it lost to:
            # if the sender itself is the loser, the peer is the target;
            # otherwise the sender is the winner and the target is the peer.
            peer = target if decision.abandon_target == session.member_id else session.member_id
            abandoned = SignalingMessage(
                type=MessageType.NEGOTIATION_ABANDONED,
                room_id=session.room_id,
                sender_id="server",
                target_id=decision.abandon_target,
                payload={
                    "peer": peer,
                    "reason": "glare",
                    "impolite": self._impolite_id(session.member_id, target),
                },
                seq=self._next_seq(session.room_id),
                timestamp=now,
            )
            if self.sessions.send_to(session.room_id, decision.abandon_target, abandoned):
                events.append(ServerEvent(kind="message", message=abandoned))
            self._trace(
                "negotiation_abandoned",
                room_id=session.room_id,
                member_id=decision.abandon_target,
                reason=decision.reason,
            )

        if decision is not None and decision.forward:
            # Preserve the client's negotiation seq so peers can correlate
            # offers/answers; ordering is provided by FIFO enqueue in the
            # single-threaded processing loop.
            forwarded = SignalingMessage(
                type=msg.type,
                room_id=session.room_id,
                sender_id=session.member_id,
                target_id=target,
                payload=msg.payload,
                seq=msg.seq,
                timestamp=now,
            )
            if not self.sessions.send_to(session.room_id, target, forwarded):
                return [self._error(session, "member_unavailable", f"member {target} is unavailable")]
            events.append(ServerEvent(kind="message", message=forwarded))

        return events

    @staticmethod
    def _impolite_id(a: str, b: str) -> str:
        return a if a < b else b

    def _handle_link_report(
        self, session: Session, msg: SignalingMessage, now: float
    ) -> list[ServerEvent]:
        target = msg.target_id
        if not target:
            return [self._error(session, "bad_target", "target_id is required")]
        mode_raw = msg.payload.get("mode")
        try:
            mode = LinkMode(mode_raw)
        except ValueError:
            return [self._error(session, "bad_frame", "mode must be 'direct' or 'relay'")]

        state = self.relay.report(session.room_id, session.member_id, target, mode, now)
        if state is None:
            return []  # duplicate report

        link_msg = SignalingMessage(
            type=MessageType.LINK_STATE,
            room_id=session.room_id,
            sender_id="server",
            target_id=None,
            payload={
                "peer_a": state.peer_a,
                "peer_b": state.peer_b,
                "mode": state.mode.value,
                "reported_by": session.member_id,
            },
            seq=self._next_seq(session.room_id),
            timestamp=now,
        )
        isolated = self.sessions.broadcast(session.room_id, link_msg)
        events = [ServerEvent(kind="message", message=link_msg)]
        for member_id in isolated:
            events.extend(self._evict(session.room_id, member_id, now, reason="slow_consumer"))
        self._trace(
            "link_state_changed",
            room_id=session.room_id,
            peer_a=state.peer_a,
            peer_b=state.peer_b,
            mode=state.mode.value,
        )
        return events

    def _handle_kick(self, session: Session, msg: SignalingMessage, now: float) -> list[ServerEvent]:
        target = msg.payload.get("target") or msg.target_id
        if not target:
            return [self._error(session, "bad_target", "target is required")]
        if target == session.member_id:
            return [self._error(session, "invalid_target", "cannot kick yourself")]
        if not self.rooms.get_room(session.room_id).has_member(target):
            return [self._error(session, "target_not_found", f"member {target} is not in the room")]

        target_session = self.sessions.get_by_member(session.room_id, target)
        events: list[ServerEvent] = []
        if target_session is not None:
            target_session.close()
            self.sessions.remove(target_session.session_id)
            events.append(ServerEvent(kind="close", session_id=target_session.session_id, reason="kicked"))
        events.extend(self._remove_member(session.room_id, target, now, "kicked"))
        self._trace("member_kicked", room_id=session.room_id, host=session.member_id, target=target)
        return events

    def tick(self, now: Optional[float] = None) -> list[ServerEvent]:
        """Deterministic maintenance: heartbeats and negotiation timeouts."""
        now = self.clock() if now is None else now
        events: list[ServerEvent] = []

        # Heartbeat timeouts.
        for session in self.sessions.tick(now):
            session.close()
            events.extend(
                self._remove_member(
                    session.room_id, session.member_id, now, "heartbeat_timeout"
                )
            )
            events.append(
                ServerEvent(
                    kind="close",
                    session_id=session.session_id,
                    reason="heartbeat_timeout",
                )
            )

        # Negotiation timeouts: end both sides so they can retry.
        for room_id, peer_a, peer_b in self.negotiations.tick(now):
            state = self.negotiations.get(room_id, peer_a, peer_b)
            reason = "failed" if state and state.phase.value == "failed" else "timeout"
            for member_id in (peer_a, peer_b):
                notice = SignalingMessage(
                    type=MessageType.NEGOTIATION_ABANDONED,
                    room_id=room_id,
                    sender_id="server",
                    target_id=member_id,
                    payload={"peer": peer_b if member_id == peer_a else peer_a, "reason": reason},
                    seq=self._next_seq(room_id),
                    timestamp=now,
                )
                if self.sessions.send_to(room_id, member_id, notice):
                    events.append(ServerEvent(kind="message", message=notice))
            self._trace("negotiation_timeout", room_id=room_id, peer_a=peer_a, peer_b=peer_b, reason=reason)

        return events

    # -- helpers ------------------------------------------------------------

    def _next_seq(self, room_id: str) -> int:
        seq = self._seq_counters.get(room_id, 0) + 1
        self._seq_counters[room_id] = seq
        return seq

    def _error(self, session: Optional[Session], code: str, message: str) -> ServerEvent:
        target = session.member_id if session else None
        room_id = session.room_id if session else ""
        frame = SignalingMessage(
            type=MessageType.ERROR,
            room_id=room_id,
            sender_id="server",
            target_id=target,
            payload={"code": code, "message": message},
            seq=self._next_seq(room_id) if room_id else 0,
        )
        if session is not None and not session.closed:
            session.enqueue(frame)
        return ServerEvent(kind="message", message=frame)

    def _trace(self, event: str, **fields: Any) -> None:
        logger.info("signaling event=%s %s", event, " ".join(f"{k}={v}" for k, v in fields.items()))
