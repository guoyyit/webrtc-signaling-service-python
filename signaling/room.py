"""Room and member management.

Membership is idempotent: a client that joins twice or reconnects after a
drop resolves to exactly one member. Online state is derived from live
sessions so it can never disagree with the actual connection registry.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

from .models import Member, MemberStatus, Role, Room


class RoomFullError(Exception):
    """Raised when a join would exceed room capacity."""


class RoomNotFoundError(Exception):
    """Raised when operating on a room that does not exist."""


class RoomExistsError(Exception):
    """Raised when creating a room whose id is already taken."""


@dataclass
class RoomManager:
    """Creates rooms and tracks membership."""

    default_capacity: int
    _rooms: dict[str, Room] = field(default_factory=dict)

    def create_room(
        self,
        room_id: str,
        capacity: Optional[int] = None,
        now: Optional[float] = None,
    ) -> Room:
        """Create a room; raises if it already exists."""
        if room_id in self._rooms:
            raise RoomExistsError(f"room already exists: {room_id}")
        room_capacity = self.default_capacity if capacity is None else capacity
        if room_capacity < 1:
            raise ValueError("capacity must be >= 1")
        room = Room(room_id=room_id, capacity=room_capacity)
        self._rooms[room_id] = room
        return room

    def get_room(self, room_id: str) -> Optional[Room]:
        return self._rooms.get(room_id)

    def join_room(
        self,
        room_id: str,
        member_id: str,
        role: Role,
        now: Optional[float] = None,
    ) -> Member:
        """Add a member, returning the existing member on duplicate join.

        Raises ``RoomFullError`` when the room is at capacity.
        """
        room = self._rooms.get(room_id)
        if room is None:
            raise RoomNotFoundError(f"room not found: {room_id}")

        existing = room.members.get(member_id)
        if existing is not None:
            # Idempotent: a repeated join / reconnect maps to one member.
            return existing

        if len(room.members) >= room.capacity:
            raise RoomFullError(f"room {room_id} is full")

        member = Member(member_id=member_id, role=role)
        room.members[member_id] = member
        return member

    def leave_room(self, room_id: str, member_id: str) -> bool:
        """Remove a member; False if the member was not present."""
        room = self._rooms.get(room_id)
        if room is None:
            return False
        if member_id not in room.members:
            return False
        del room.members[member_id]
        return True

    def set_status(
        self,
        room_id: str,
        member_id: str,
        status: MemberStatus,
    ) -> None:
        room = self._rooms.get(room_id)
        if room is None:
            raise RoomNotFoundError(f"room not found: {room_id}")
        member = room.members.get(member_id)
        if member is None:
            raise KeyError(f"member not found: {member_id}")
        member.status = status

    def list_members(self, room_id: str) -> list[Member]:
        room = self._rooms.get(room_id)
        if room is None:
            return []
        return list(room.members.values())

    def member_count(self, room_id: str) -> int:
        room = self._rooms.get(room_id)
        return 0 if room is None else len(room.members)
