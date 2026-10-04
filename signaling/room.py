"""Room membership, ordered forwarding and slow-member isolation."""

from __future__ import annotations

import asyncio
import itertools
from dataclasses import dataclass, field
from typing import Any

from .config import SignalingConfig
from .connection import Connection
from .models import MemberInfo, MemberStatus, Role
from .negotiation import NegotiationTracker
from .relay import RelayManager


@dataclass
class Member:
    """Server-side state of one room member."""

    client_id: str
    role: Role
    connection: Connection
    outbox: asyncio.Queue[dict[str, Any]]
    joined_at: float
    last_seen: float
    status: MemberStatus = MemberStatus.ONLINE


class Room:
    """A single signaling room.

    The membership set is exactly the set of online members: leaving or
    disconnecting removes the member, so membership and online status
    can never diverge.  Re-joining with the same client id replaces the
    previous connection in place and never creates a duplicate member.
    """

    def __init__(self, name: str, config: SignalingConfig) -> None:
        self.name = name
        self._config = config
        self._members: dict[str, Member] = {}
        self._seq = itertools.count(1)
        self.negotiation = NegotiationTracker(
            timeout_s=config.negotiation_timeout_s,
            max_retries=config.max_negotiation_retries,
        )
        self.relay = RelayManager()

    # ---- membership -------------------------------------------------

    def is_full(self) -> bool:
        return len(self._members) >= self._config.max_room_capacity

    def get(self, client_id: str) -> Member | None:
        return self._members.get(client_id)

    def members(self) -> list[MemberInfo]:
        return [
            MemberInfo(client_id=m.client_id, role=m.role, status=m.status)
            for m in self._members.values()
        ]

    def member_ids(self) -> list[str]:
        return sorted(self._members)

    async def join(
        self, client_id: str, role: Role, connection: Connection, now: float
    ) -> tuple[Member, bool]:
        """Add or re-attach a member.  Returns (member, is_new)."""
        existing = self._members.get(client_id)
        if existing is not None:
            # rejoin: replace the connection, keep identity and role
            old_connection = existing.connection
            existing.connection = connection
            existing.role = role
            existing.last_seen = now
            existing.status = MemberStatus.ONLINE
            # drop stale queued messages bound for the old connection
            existing.outbox = asyncio.Queue(maxsize=self._config.max_pending_messages)
            await old_connection.close(code=4000, reason="replaced by rejoin")
            return existing, False
        member = Member(
            client_id=client_id,
            role=role,
            connection=connection,
            outbox=asyncio.Queue(maxsize=self._config.max_pending_messages),
            joined_at=now,
            last_seen=now,
        )
        self._members[client_id] = member
        return member, True

    async def leave(self, client_id: str) -> bool:
        """Remove a member.  Returns True if the member was present."""
        member = self._members.pop(client_id, None)
        if member is None:
            return False
        member.status = MemberStatus.OFFLINE
        self.negotiation.drop_member(client_id)
        self.relay.drop_member(client_id)
        await member.connection.close(code=1000, reason="left")
        return True

    # ---- forwarding ---------------------------------------------------

    def next_seq(self) -> int:
        return next(self._seq)

    def _enqueue(self, member: Member, data: dict[str, Any]) -> bool:
        try:
            member.outbox.put_nowait(data)
            return True
        except asyncio.QueueFull:
            return False

    async def send_to(self, target_id: str, data: dict[str, Any]) -> str | None:
        """Queue a message for one member only.

        Returns the client id if the member had to be isolated because
        its queue overflowed (slow consumer), else None.
        """
        member = self._members.get(target_id)
        if member is None:
            return None
        if not self._enqueue(member, data):
            return await self._isolate(member, reason="slow consumer")
        return None

    async def broadcast(
        self, data: dict[str, Any], exclude: str | None = None
    ) -> list[str]:
        """Queue a message for every member except ``exclude``.

        Returns ids of members isolated due to queue overflow.
        """
        isolated: list[str] = []
        for member in list(self._members.values()):
            if member.client_id == exclude:
                continue
            if not self._enqueue(member, data):
                dropped = await self._isolate(member, reason="slow consumer")
                isolated.append(dropped)
        return isolated

    async def _isolate(self, member: Member, reason: str) -> str:
        """Detach a slow member so it cannot block the room."""
        self._members.pop(member.client_id, None)
        member.status = MemberStatus.OFFLINE
        self.negotiation.drop_member(member.client_id)
        self.relay.drop_member(member.client_id)
        await member.connection.close(code=4001, reason=reason)
        return member.client_id

    # ---- heartbeat ----------------------------------------------------

    def touch(self, client_id: str, now: float) -> None:
        member = self._members.get(client_id)
        if member is not None:
            member.last_seen = now

    async def check_heartbeat(self, now: float) -> list[str]:
        """Remove members whose last sign of life is too old.

        The timeout comparison is a pure function of ``now`` and
        ``last_seen``, so the decision is deterministic and reproducible.
        """
        timed_out = [
            m
            for m in self._members.values()
            if now - m.last_seen > self._config.heartbeat_timeout_s
        ]
        removed: list[str] = []
        for member in timed_out:
            removed.append(await self._isolate(member, reason="heartbeat timeout"))
        return removed
