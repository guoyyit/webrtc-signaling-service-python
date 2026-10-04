"""Slow-member isolation, deterministic heartbeat timeout, reconnect."""

from __future__ import annotations

from signaling.config import SignalingConfig
from signaling.models import Role
from signaling.service import SignalingService
from tests.conftest import FakeConn, drain, join, run


def _fast_config() -> SignalingConfig:
    return SignalingConfig(max_room_capacity=8, max_pending_messages=2,
                           heartbeat_timeout_s=10.0, token_secret="test-secret")


def test_slow_member_isolated_without_blocking_room():
    async def scenario():
        svc = SignalingService(_fast_config())
        await join(svc, "r", "alice", Role.HOST, now=0.0)
        slow_conn = await join(svc, "r", "slow", now=0.0)
        drain(svc, "r", "alice")  # healthy members keep draining
        await join(svc, "r", "carol", now=0.0)
        drain(svc, "r", "alice")
        drain(svc, "r", "carol")
        room = svc.room("r")
        # "slow" never drains: its outbox already holds join_ack + peer_joined
        assert room.get("slow").outbox.qsize() == 2
        await svc.handle("r", "alice",
                         {"type": "ice", "target": "slow", "seq": 1,
                          "payload": {}}, now=1.0)
        assert room.get("slow") is None
        assert slow_conn.closed is not None and slow_conn.closed[0] == 4001
        # the room keeps working: alice <-> carol forwarding unaffected
        await svc.handle("r", "alice",
                         {"type": "offer", "target": "carol", "seq": 1,
                          "payload": {"sdp": "A"}}, now=2.0)
        carol_msgs = drain(svc, "r", "carol")
        assert any(m["type"] == "peer_left" and m["client_id"] == "slow"
                   for m in carol_msgs)
        assert any(m["type"] == "offer" for m in carol_msgs)
        # others were told the slow member left
        assert any(m["type"] == "peer_left" and m["client_id"] == "slow"
                   for m in drain(svc, "r", "alice"))

    run(scenario())


def test_heartbeat_timeout_is_deterministic():
    async def scenario():
        svc = SignalingService(_fast_config())  # timeout = 10s
        await join(svc, "r", "alice", now=0.0)
        await join(svc, "r", "bob", now=0.0)
        drain(svc, "r", "alice")
        # bob goes silent at t=0; alice stays responsive
        svc.room("r").touch("alice", 10.0)
        await svc.sweep(now=10.0)   # bob: 10.0 - 0.0 = 10.0, not > 10.0
        assert svc.room("r").member_ids() == ["alice", "bob"]
        svc.room("r").touch("alice", 10.1)
        await svc.sweep(now=10.1)   # bob: 10.1 > 10.0 -> timed out
        assert svc.room("r").member_ids() == ["alice"]
        assert any(m["type"] == "peer_left" and m["client_id"] == "bob"
                   for m in drain(svc, "r", "alice"))
        assert any(a["action"] == "timeout" and a["client_id"] == "bob"
                   for a in svc.audit_log)

    run(scenario())


def test_any_message_counts_as_liveness():
    async def scenario():
        svc = SignalingService(_fast_config())
        await join(svc, "r", "alice", now=0.0)
        await join(svc, "r", "bob", now=0.0)
        drain(svc, "r", "bob")
        # bob pings at t=9 -> survives the t=10 sweep
        await svc.handle("r", "bob", {"type": "ping"}, now=9.0)
        svc.room("r").touch("alice", 9.0)
        await svc.sweep(now=10.0)
        assert svc.room("r").member_ids() == ["alice", "bob"]
        assert [m["type"] for m in drain(svc, "r", "bob")] == ["pong"]

    run(scenario())


def test_reconnect_replaces_dead_connection_and_restores_forwarding():
    async def scenario():
        svc = SignalingService(_fast_config())
        dead = await join(svc, "r", "alice", Role.HOST, now=0.0)
        await join(svc, "r", "bob", now=0.0)
        # alice's connection dies silently; she reconnects with the same id
        fresh = await join(svc, "r", "alice", Role.HOST, now=5.0)
        assert dead.closed == (4000, "replaced by rejoin")
        assert svc.room("r").member_ids() == ["alice", "bob"]
        drain(svc, "r", "alice")
        drain(svc, "r", "bob")
        # forwarding works again on the new connection
        await svc.handle("r", "bob",
                         {"type": "offer", "target": "alice", "seq": 1,
                          "payload": {"sdp": "B"}}, now=6.0)
        msgs = drain(svc, "r", "alice")
        assert [m["type"] for m in msgs] == ["offer"]
        assert fresh.closed is None

    run(scenario())


def test_disconnect_then_reconnect_full_cycle():
    async def scenario():
        svc = SignalingService(_fast_config())
        await join(svc, "r", "alice", now=0.0)
        await join(svc, "r", "bob", now=0.0)
        drain(svc, "r", "bob")  # clear join traffic
        await svc.disconnect("r", "alice", now=1.0)
        assert svc.room("r").member_ids() == ["bob"]
        await join(svc, "r", "alice", now=2.0)
        # bob observed exactly one leave and one join
        bob = drain(svc, "r", "bob")
        assert [m["type"] for m in bob] == ["peer_left", "peer_joined"]

    run(scenario())
