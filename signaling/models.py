"""Core data types for the signaling service."""

from __future__ import annotations

import enum
import time
from dataclasses import dataclass, field
from typing import Any, Mapping, Optional


class Role(str, enum.Enum):
    """Room role; controls which control messages a member may send."""

    HOST = "host"
    MEMBER = "member"
    VIEWER = "viewer"


class MemberStatus(str, enum.Enum):
    ONLINE = "online"
    OFFLINE = "offline"


class LinkMode(str, enum.Enum):
    """Transport mode for a peer pair."""

    DIRECT = "direct"
    RELAY = "relay"


class NegotiationPhase(str, enum.Enum):
    """Lifecycle of a peer-pair negotiation."""

    IDLE = "idle"
    OFFER_PENDING = "offer_pending"
    ANSWER_PENDING = "answer_pending"
    CONNECTED = "connected"
    FAILED = "failed"


class MessageType(str, enum.Enum):
    """Wire message types.

    Client -> server messages are prefixed by direction in comments.
    """

    # client -> server
    JOIN = "join"
    LEAVE = "leave"
    OFFER = "offer"
    ANSWER = "answer"
    ICE_CANDIDATE = "ice_candidate"
    RENEGOTIATE = "renegotiate"
    LINK_REPORT = "link_report"
    PING = "ping"
    KICK = "kick"

    # server -> client
    JOINED = "joined"
    ERROR = "error"
    MEMBER_JOINED = "member_joined"
    MEMBER_LEFT = "member_left"
    MEMBER_STATUS = "member_status"
    NEGOTIATION_ABANDONED = "negotiation_abandoned"
    LINK_STATE = "link_state"
    PONG = "pong"

    @classmethod
    def from_raw(cls, value: str) -> Optional["MessageType"]:
        """Resolve a raw string, returning None for unknown types."""
        if not isinstance(value, str):
            return None
        try:
            return cls(value)
        except ValueError:
            return None


class UnknownMessageTypeError(ValueError):
    """Raised when a wire frame carries an unrecognized message type."""


@dataclass
class SignalingMessage:
    """A single signaling frame on the wire."""

    type: MessageType
    room_id: str
    sender_id: str
    target_id: Optional[str] = None
    payload: Mapping[str, Any] = field(default_factory=dict)
    seq: int = 0
    timestamp: float = field(default_factory=time.monotonic)

    def to_wire(self) -> dict[str, Any]:
        return {
            "type": self.type.value,
            "room_id": self.room_id,
            "sender_id": self.sender_id,
            "target_id": self.target_id,
            "payload": dict(self.payload),
            "seq": self.seq,
            "timestamp": self.timestamp,
        }

    @classmethod
    def from_wire(cls, raw: Mapping[str, Any]) -> "SignalingMessage":
        """Parse a raw mapping.

        Raises ``UnknownMessageTypeError`` for unrecognized types so the
        server can reject the frame without affecting other members.
        """
        raw_type = raw.get("type")
        msg_type = MessageType.from_raw(raw_type)
        if msg_type is None:
            raise UnknownMessageTypeError(f"unknown message type: {raw_type!r}")

        payload = raw.get("payload") or {}
        if not isinstance(payload, Mapping):
            raise ValueError("payload must be an object")

        return cls(
            type=msg_type,
            room_id=str(raw.get("room_id", "")),
            sender_id=str(raw.get("sender_id", "")),
            target_id=raw.get("target_id"),
            payload=dict(payload),
            seq=int(raw.get("seq", 0) or 0),
            timestamp=float(raw.get("timestamp", time.monotonic())),
        )


@dataclass
class Member:
    """A room member."""

    member_id: str
    role: Role
    status: MemberStatus = MemberStatus.ONLINE
    joined_at: float = field(default_factory=time.monotonic)


@dataclass
class Room:
    """A signaling room."""

    room_id: str
    capacity: int
    members: dict[str, Member] = field(default_factory=dict)
    created_at: float = field(default_factory=time.monotonic)

    def has_member(self, member_id: str) -> bool:
        return member_id in self.members

    def online_count(self) -> int:
        return sum(1 for m in self.members.values() if m.status == MemberStatus.ONLINE)


@dataclass
class TokenClaims:
    """Verified credential claims."""

    room_id: str
    member_id: str
    role: Role
    issued_at: float
    expires_at: float

    def is_expired(self, now: float) -> bool:
        return now >= self.expires_at


# Control messages that require elevated (host) privileges.
HOST_ONLY_MESSAGES: frozenset[MessageType] = frozenset({MessageType.KICK})

# Messages that viewers are never allowed to send.
VIEWER_FORBIDDEN_MESSAGES: frozenset[MessageType] = frozenset(
    {
        MessageType.OFFER,
        MessageType.ANSWER,
        MessageType.ICE_CANDIDATE,
        MessageType.RENEGOTIATE,
        MessageType.LINK_REPORT,
        MessageType.KICK,
    }
)
