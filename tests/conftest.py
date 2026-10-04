"""Shared pytest fixtures and helpers."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Optional

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from signaling.auth import AuthManager
from signaling.config import SignalingConfig
from signaling.models import Role
from signaling.negotiation import NegotiationTracker
from signaling.relay import RelayCoordinator
from signaling.room import RoomManager
from signaling.server import SignalingServer
from signaling.session import SessionManager


class FakeClock:
    """Deterministic injectable clock."""

    def __init__(self, start: float = 1000.0) -> None:
        self.t = start

    def __call__(self) -> float:
        return self.t

    def advance(self, dt: float) -> float:
        self.t += dt
        return self.t


def make_server(
    clock: Optional[FakeClock] = None,
    room_capacity: int = 8,
    negotiation_timeout: float = 10.0,
    max_retries: int = 2,
    heartbeat_timeout: float = 15.0,
    send_queue_size: int = 8,
    token_ttl: float = 3600.0,
) -> SignalingServer:
    cfg = SignalingConfig(
        room_capacity=room_capacity,
        token_ttl=token_ttl,
        negotiation_timeout=negotiation_timeout,
        max_retries=max_retries,
        heartbeat_interval=5.0,
        heartbeat_timeout=heartbeat_timeout,
        send_queue_size=send_queue_size,
        send_timeout=2.0,
    )
    auth = AuthManager("test-secret", token_ttl)
    rooms = RoomManager(default_capacity=room_capacity)
    sessions = SessionManager(
        queue_size=send_queue_size, heartbeat_timeout=heartbeat_timeout
    )
    negotiations = NegotiationTracker(negotiation_timeout, max_retries)
    relay = RelayCoordinator()
    return SignalingServer(
        config=cfg,
        auth=auth,
        rooms=rooms,
        sessions=sessions,
        negotiations=negotiations,
        relay=relay,
        clock=clock or FakeClock(),
    )


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture
def server(clock: FakeClock) -> SignalingServer:
    return make_server(clock=clock)


def join(
    server: SignalingServer,
    room_id: str,
    member_id: str,
    role: Role = Role.MEMBER,
    session_id: Optional[str] = None,
    now: Optional[float] = None,
) -> str:
    """Create a room if needed, join a member, drain and return session id."""
    if server.rooms.get_room(room_id) is None:
        server.rooms.create_room(room_id)
    token = server.auth.issue_token(room_id, member_id, role, now=now or server.clock())
    sid = session_id or f"sess-{member_id}"
    session, events = server.open_session(token, room_id, sid)
    assert session is not None, f"join failed for {member_id}: {events}"
    session.poll_outbox()
    return sid


def frames_of(session) -> list[tuple[str, dict]]:
    return [(m.type.value, dict(m.payload)) for m in session.poll_outbox()]
