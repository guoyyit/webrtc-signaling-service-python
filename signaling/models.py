"""Message and member model definitions (types only for now)."""

from __future__ import annotations

import enum
from dataclasses import dataclass, field
from typing import Any


class Role(enum.Enum):
    """Access role of a room member."""

    HOST = "host"
    PARTICIPANT = "participant"
    VIEWER = "viewer"


class MessageType(enum.Enum):
    """Known signaling message types."""

    JOIN = "join"
    LEAVE = "leave"
    PEER_JOINED = "peer_joined"
    PEER_LEFT = "peer_left"
    OFFER = "offer"
    ANSWER = "answer"
    ICE = "ice"
    RENEGOTIATE = "renegotiate"
    GLARE_RESOLVED = "glare_resolved"
    RELAY_START = "relay_start"
    RELAY_STOP = "relay_stop"
    RELAY_DATA = "relay_data"
    MEMBER_STATE = "member_state"
    PING = "ping"
    PONG = "pong"
    ERROR = "error"


class ErrorCode(enum.Enum):
    AUTH_FAILED = "auth_failed"
    TOKEN_EXPIRED = "token_expired"
    ROOM_FULL = "room_full"
    FORBIDDEN = "forbidden"
    UNKNOWN_PEER = "unknown_peer"
    UNKNOWN_MESSAGE = "unknown_message"
    BAD_SEQUENCE = "bad_sequence"


class MemberStatus(enum.Enum):
    ONLINE = "online"
    OFFLINE = "offline"


@dataclass
class Message:
    """A signaling message in canonical form."""

    type: MessageType
    room: str = ""
    sender: str = ""
    target: str | None = None
    seq: int = 0
    payload: dict[str, Any] = field(default_factory=dict)


@dataclass
class MemberInfo:
    """Public view of a room member."""

    client_id: str
    role: Role
    status: MemberStatus
