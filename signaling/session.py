"""Per-connection session with slow-member isolation.

Each session owns a bounded outbound queue. When a member consumes too
slowly the queue fills up; instead of blocking the room the session is
marked as isolated and eventually closed deterministically. Heartbeats are
tracked with an injectable clock so timeout behaviour is reproducible.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from typing import Deque, Optional

from .models import Role, SignalingMessage


@dataclass
class Session:
    """A single client connection."""

    session_id: str
    member_id: str
    room_id: str
    role: Role
    queue_size: int
    outbox: Deque[SignalingMessage] = field(default_factory=deque)
    closed: bool = False
    isolated: bool = False
    last_heartbeat: float = 0.0
    last_seq_sent: int = 0

    def enqueue(self, message: SignalingMessage) -> bool:
        """Append a message to the outbound queue.

        Returns False when the queue is full (slow consumer); the caller is
        expected to isolate/close the session rather than block.
        """
        if self.closed or self.isolated:
            return False
        if len(self.outbox) >= self.queue_size:
            # Slow consumer: isolate instead of blocking the room.
            self.isolated = True
            return False
        self.outbox.append(message)
        self.last_seq_sent = message.seq
        return True

    def poll_outbox(self, max_items: Optional[int] = None) -> list[SignalingMessage]:
        """Drain up to ``max_items`` messages from the outbox."""
        if max_items is None:
            items = list(self.outbox)
            self.outbox.clear()
            return items
        drained: list[SignalingMessage] = []
        while self.outbox and len(drained) < max_items:
            drained.append(self.outbox.popleft())
        return drained

    def mark_heartbeat(self, now: float) -> None:
        self.last_heartbeat = now

    def is_timed_out(self, now: float, heartbeat_timeout: float) -> bool:
        return (now - self.last_heartbeat) > heartbeat_timeout

    def close(self) -> None:
        self.closed = True


class SessionManager:
    """Registry of live sessions with deterministic heartbeat checks."""

    def __init__(self, queue_size: int, heartbeat_timeout: float) -> None:
        self._queue_size = queue_size
        self._heartbeat_timeout = heartbeat_timeout
        self._sessions: dict[str, Session] = {}

    def create(
        self,
        session_id: str,
        member_id: str,
        room_id: str,
        role: Role,
        now: float,
    ) -> Session:
        session = Session(
            session_id=session_id,
            member_id=member_id,
            room_id=room_id,
            role=role,
            queue_size=self._queue_size,
            last_heartbeat=now,
        )
        self._sessions[session_id] = session
        return session

    def get(self, session_id: str) -> Optional[Session]:
        return self._sessions.get(session_id)

    def all_sessions(self) -> list[Session]:
        """All live sessions (for transport draining)."""
        return list(self._sessions.values())

    def get_by_member(self, room_id: str, member_id: str) -> Optional[Session]:
        for session in self._sessions.values():
            if session.room_id == room_id and session.member_id == member_id:
                return session
        return None

    def remove(self, session_id: str) -> Optional[Session]:
        return self._sessions.pop(session_id, None)

    def tick(self, now: float) -> list[Session]:
        """Return sessions that have timed out and remove them."""
        timed_out: list[Session] = []
        for session in list(self._sessions.values()):
            if session.is_timed_out(now, self._heartbeat_timeout):
                timed_out.append(session)
                del self._sessions[session.session_id]
        return timed_out

    def broadcast(
        self,
        room_id: str,
        message: SignalingMessage,
        exclude: Optional[str] = None,
    ) -> list[str]:
        """Enqueue a message to every session in a room.

        Returns the list of member ids that were isolated (queue full) so the
        server can evict them.
        """
        isolated: list[str] = []
        for session in list(self._sessions.values()):
            if session.room_id != room_id or session.closed:
                continue
            if exclude and session.member_id == exclude:
                continue
            if not session.enqueue(message):
                isolated.append(session.member_id)
        return isolated

    def send_to(self, room_id: str, target_id: str, message: SignalingMessage) -> bool:
        """Enqueue to a single member; False if absent or isolated."""
        session = self.get_by_member(room_id, target_id)
        if session is None or session.closed or session.isolated:
            return False
        return session.enqueue(message)
