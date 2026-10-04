"""Shared test helpers: fake connections and an async runner."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from signaling.config import SignalingConfig
from signaling.models import Role
from signaling.service import SignalingService


def run(coro):
    """Run a coroutine in a fresh event loop (no pytest-asyncio needed)."""
    return asyncio.run(coro)


class FakeConn:
    """Connection double: records sent messages and close calls."""

    def __init__(self) -> None:
        self.sent: list[dict[str, Any]] = []
        self.closed: tuple[int, str] | None = None

    async def send_json(self, data: dict[str, Any]) -> None:
        self.sent.append(data)

    async def close(self, code: int = 1000, reason: str = "") -> None:
        self.closed = (code, reason)


def drain(service: SignalingService, room: str, client_id: str) -> list[dict[str, Any]]:
    """Move all queued messages of a member into its fake connection."""
    member = service.room(room).get(client_id)  # type: ignore[union-attr]
    assert member is not None, f"{client_id} not in room"
    conn = member.connection
    assert isinstance(conn, FakeConn)
    while not member.outbox.empty():
        conn.sent.append(member.outbox.get_nowait())
    out, conn.sent = conn.sent[:], []
    return out


@pytest.fixture()
def config() -> SignalingConfig:
    return SignalingConfig(
        max_room_capacity=4,
        negotiation_timeout_s=5.0,
        max_negotiation_retries=2,
        heartbeat_interval_s=5.0,
        heartbeat_timeout_s=15.0,
        max_pending_messages=4,
        token_ttl_s=100.0,
        token_secret="test-secret",
    )


@pytest.fixture()
def service(config: SignalingConfig) -> SignalingService:
    return SignalingService(config)


async def join(service: SignalingService, room: str, client_id: str,
               role: Role = Role.PARTICIPANT, now: float = 0.0,
               conn: FakeConn | None = None) -> FakeConn:
    conn = conn or FakeConn()
    token = service.issuer.issue(room, client_id, role, now=now - 1.0)
    await service.connect(room, token, conn, now=now)
    return conn
