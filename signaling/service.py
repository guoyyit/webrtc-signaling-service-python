"""Top-level service coordinating rooms, auth and message dispatch."""

from __future__ import annotations

import time
from typing import Any

from .auth import AuthError, TokenIssuer
from .config import SignalingConfig
from .connection import Connection
from .models import ErrorCode, MemberStatus, MessageType, Role
from .negotiation import AnswerVerdict, OfferVerdict
from .room import Room

#: message types that are forwarded to exactly one target peer
_TARGETED = {
    MessageType.OFFER,
    MessageType.ANSWER,
    MessageType.ICE,
    MessageType.RENEGOTIATE,
    MessageType.RELAY_DATA,
}

#: control messages restricted to privileged roles
_CONTROL = {MessageType.RELAY_START, MessageType.RELAY_STOP}
_CONTROL_ROLES = {Role.HOST, Role.PARTICIPANT}


class ServiceError(Exception):
    """Raised for join-time failures; carries a wire error code."""

    def __init__(self, code: ErrorCode, detail: str) -> None:
        super().__init__(detail)
        self.code = code
        self.detail = detail


class SignalingService:
    """Entry point used by transports (WebSocket) and tests."""

    def __init__(self, config: SignalingConfig | None = None) -> None:
        self.config = config or SignalingConfig()
        self._issuer = TokenIssuer(self.config.token_secret, self.config.token_ttl_s)
        self._rooms: dict[str, Room] = {}
        self.audit_log: list[dict[str, Any]] = []

    # ---- helpers ------------------------------------------------------

    @property
    def issuer(self) -> TokenIssuer:
        return self._issuer

    def room(self, name: str) -> Room | None:
        return self._rooms.get(name)

    def _audit(self, now: float, room: str, client_id: str, action: str, detail: str) -> None:
        self.audit_log.append(
            {"now": now, "room": room, "client_id": client_id,
             "action": action, "detail": detail}
        )

    def _room_for(self, name: str) -> Room:
        room = self._rooms.get(name)
        if room is None:
            room = Room(name, self.config)
            self._rooms[name] = room
        return room

    @staticmethod
    def _error(code: ErrorCode, detail: str) -> dict[str, Any]:
        return {"type": MessageType.ERROR.value, "code": code.value, "detail": detail}

    # ---- connection lifecycle ------------------------------------------

    async def connect(
        self, room_name: str, token: str, connection: Connection, now: float | None = None
    ) -> dict[str, Any]:
        """Authenticate and join a room.  Returns the join ack payload."""
        now = time.time() if now is None else now
        try:
            claims = self._issuer.validate(token, room_name, now)
        except AuthError as exc:
            code = (ErrorCode.TOKEN_EXPIRED if "expired" in str(exc)
                    else ErrorCode.AUTH_FAILED)
            self._audit(now, room_name, "?", "auth_failed", str(exc))
            raise ServiceError(code, str(exc)) from exc

        room = self._room_for(room_name)
        if room.get(claims.client_id) is None and room.is_full():
            self._audit(now, room_name, claims.client_id, "room_full", "join rejected")
            raise ServiceError(ErrorCode.ROOM_FULL, "room is full")

        member, is_new = await room.join(claims.client_id, claims.role, connection, now)
        if is_new:
            await self._broadcast_presence(
                room,
                {
                    "type": MessageType.PEER_JOINED.value,
                    "member": {"client_id": member.client_id, "role": member.role.value},
                },
                exclude=member.client_id,
            )
        ack = {
            "type": "join_ack",
            "room": room_name,
            "client_id": member.client_id,
            "role": member.role.value,
            "rejoined": not is_new,
            "members": [
                {"client_id": m.client_id, "role": m.role.value}
                for m in room.members()
                if m.client_id != member.client_id
            ],
        }
        await room.send_to(member.client_id, ack)
        return ack

    async def disconnect(
        self,
        room_name: str,
        client_id: str,
        now: float | None = None,
        connection: Connection | None = None,
    ) -> None:
        now = time.time() if now is None else now
        room = self._rooms.get(room_name)
        if room is None:
            return
        member = room.get(client_id)
        if member is None:
            return
        # a stale handler (e.g. replaced by a rejoin) must not remove the
        # member that a newer connection already re-attached
        if connection is not None and member.connection is not connection:
            return
        if await room.leave(client_id):
            await self._broadcast_presence(
                room,
                {"type": MessageType.PEER_LEFT.value, "client_id": client_id},
            )

    async def _broadcast_presence(
        self, room: Room, data: dict[str, Any], exclude: str | None = None
    ) -> None:
        isolated = await room.broadcast(data, exclude=exclude)
        for client_id in isolated:
            self._audit(time.time(), room.name, client_id, "isolated", "slow consumer")
            await self._broadcast_presence(
                room,
                {"type": MessageType.PEER_LEFT.value, "client_id": client_id},
            )

    # ---- message dispatch ------------------------------------------------

    async def handle(
        self, room_name: str, client_id: str, raw: dict[str, Any], now: float | None = None
    ) -> None:
        now = time.time() if now is None else now
        room = self._rooms.get(room_name)
        if room is None or room.get(client_id) is None:
            return
        room.touch(client_id, now)

        try:
            mtype = MessageType(str(raw.get("type")))
        except ValueError:
            await room.send_to(
                client_id,
                self._error(ErrorCode.UNKNOWN_MESSAGE, f"unknown type: {raw.get('type')!r}"),
            )
            return

        if mtype is MessageType.PING:
            await room.send_to(client_id, {"type": MessageType.PONG.value})
            return
        if mtype is MessageType.LEAVE:
            await self.disconnect(room_name, client_id, now)
            return
        if mtype in _CONTROL:
            await self._handle_control(room, client_id, mtype, raw, now)
            return
        if mtype in _TARGETED:
            await self._handle_targeted(room, client_id, mtype, raw, now)
            return
        # known but not handled here (e.g. client sends PONG)
        if mtype is MessageType.PONG:
            return
        await room.send_to(
            client_id, self._error(ErrorCode.UNKNOWN_MESSAGE, f"unsupported type: {mtype.value}")
        )

    async def _handle_control(
        self, room: Room, client_id: str, mtype: MessageType,
        raw: dict[str, Any], now: float,
    ) -> None:
        member = room.get(client_id)
        assert member is not None
        if member.role not in _CONTROL_ROLES:
            self._audit(now, room.name, client_id, "forbidden",
                        f"{mtype.value} denied for role {member.role.value}")
            await room.send_to(
                client_id,
                self._error(ErrorCode.FORBIDDEN, f"{mtype.value} not allowed for your role"),
            )
            return
        target = raw.get("target")
        if not isinstance(target, str) or room.get(target) is None:
            await room.send_to(client_id, self._error(ErrorCode.UNKNOWN_PEER, "unknown target"))
            return
        changed = (room.relay.start(client_id, target) if mtype is MessageType.RELAY_START
                   else room.relay.stop(client_id, target))
        if changed:
            a, b = sorted((client_id, target))
            # degradation / recovery is visible to every member
            await self._broadcast_presence(
                room,
                {"type": "relay_state", "a": a, "b": b,
                 "active": mtype is MessageType.RELAY_START},
            )

    async def _handle_targeted(
        self, room: Room, client_id: str, mtype: MessageType,
        raw: dict[str, Any], now: float,
    ) -> None:
        target = raw.get("target")
        if not isinstance(target, str) or room.get(target) is None:
            await room.send_to(client_id, self._error(ErrorCode.UNKNOWN_PEER, "unknown target"))
            return
        seq = int(raw.get("seq") or 0)
        payload = raw.get("payload") if isinstance(raw.get("payload"), dict) else {}

        if mtype is MessageType.OFFER:
            verdict = room.negotiation.on_offer(client_id, target, seq, now, payload)
            if verdict is OfferVerdict.IGNORE_STALE:
                return  # duplicate / out-of-order: drop silently
            if verdict is OfferVerdict.ROLLBACK_SELF:
                room.negotiation.on_rollback(client_id, target)
                await room.send_to(client_id, {
                    "type": MessageType.GLARE_RESOLVED.value,
                    "action": "rollback", "winner": target, "peer": target,
                })
                return
            if verdict is OfferVerdict.FORWARD_AND_ROLLBACK_PEER:
                room.negotiation.on_rollback(target, client_id)
                await room.send_to(target, {
                    "type": MessageType.GLARE_RESOLVED.value,
                    "action": "rollback", "winner": client_id, "peer": client_id,
                })
                # the winner may already hold the loser's earlier offer;
                # tell it explicitly to ignore that offer
                await room.send_to(client_id, {
                    "type": MessageType.GLARE_RESOLVED.value,
                    "action": "ignore_peer_offer", "winner": client_id, "peer": target,
                })
            await self._forward(room, client_id, target, mtype, seq, payload)
            return

        if mtype is MessageType.ANSWER:
            verdict = room.negotiation.on_answer(client_id, target, seq)
            if verdict is AnswerVerdict.IGNORE_STALE:
                return
            await self._forward(room, client_id, target, mtype, seq, payload)
            return

        if mtype is MessageType.RELAY_DATA:
            if not room.relay.is_relaying(client_id, target):
                await room.send_to(
                    client_id,
                    self._error(ErrorCode.FORBIDDEN, "relay not active for this pair"),
                )
                return
            await self._forward(room, client_id, target, mtype, seq, payload)
            return

        # ICE / RENEGOTIATE: pure forwarding
        await self._forward(room, client_id, target, mtype, seq, payload)

    async def _forward(
        self, room: Room, sender: str, target: str,
        mtype: MessageType, seq: int, payload: dict[str, Any],
    ) -> None:
        wire = {
            "type": mtype.value,
            "room": room.name,
            "sender": sender,
            "target": target,
            "seq": seq,
            "server_seq": room.next_seq(),
            "payload": payload,
        }
        isolated = await room.send_to(target, wire)
        if isolated is not None:
            self._audit(time.time(), room.name, isolated, "isolated", "slow consumer")
            await self._broadcast_presence(
                room, {"type": MessageType.PEER_LEFT.value, "client_id": isolated}
            )

    # ---- periodic maintenance ---------------------------------------------

    async def broadcast_pings(self) -> None:
        """Queue a heartbeat ping for every member of every room."""
        for room in list(self._rooms.values()):
            await self._broadcast_presence(room, {"type": MessageType.PING.value})

    async def sweep(self, now: float | None = None) -> None:
        """Heartbeat timeouts and negotiation retries for all rooms."""
        now = time.time() if now is None else now
        for room in list(self._rooms.values()):
            for client_id in await room.check_heartbeat(now):
                self._audit(now, room.name, client_id, "timeout", "heartbeat expired")
                await self._broadcast_presence(
                    room, {"type": MessageType.PEER_LEFT.value, "client_id": client_id}
                )
            retries, failures = room.negotiation.check_timeouts(now)
            for sender, peer, seq, payload in retries:
                if room.get(sender) is not None and room.get(peer) is not None:
                    await self._forward(room, sender, peer, MessageType.OFFER, seq, payload)
            for sender, peer in failures:
                self._audit(now, room.name, sender, "negotiation_failed", f"peer={peer}")
                await room.send_to(sender, self._error(
                    ErrorCode.BAD_SEQUENCE, f"negotiation with {peer} failed after retries"))
