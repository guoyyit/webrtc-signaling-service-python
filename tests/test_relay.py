"""Relay fallback, recovery, visibility and data-channel continuity."""

from __future__ import annotations

from signaling.models import Role
from tests.conftest import drain, join, run


async def _setup(service):
    await join(service, "r", "alice", Role.HOST, now=0.0)
    await join(service, "r", "bob", now=0.0)
    await join(service, "r", "carol", now=0.0)
    for cid in ("alice", "bob", "carol"):
        drain(service, "r", cid)


def test_relay_fallback_visible_to_whole_room(service):
    async def scenario():
        await _setup(service)
        await service.handle("r", "alice",
                             {"type": "relay_start", "target": "bob"}, now=1.0)
        # every member (including an uninvolved one) sees the degradation
        for cid in ("alice", "bob", "carol"):
            msgs = drain(service, "r", cid)
            assert any(m["type"] == "relay_state" and m["active"] is True
                       and {m["a"], m["b"]} == {"alice", "bob"} for m in msgs), cid
        assert service.room("r").relay.is_relaying("alice", "bob")

    run(scenario())


def test_relay_recovery_visible_and_data_flows_while_relaying(service):
    async def scenario():
        await _setup(service)
        await service.handle("r", "alice",
                             {"type": "relay_start", "target": "bob"}, now=1.0)
        for cid in ("alice", "bob", "carol"):
            drain(service, "r", cid)
        # relayed data reaches only the peer
        await service.handle("r", "alice",
                             {"type": "relay_data", "target": "bob", "seq": 1,
                              "payload": {"chunk": 1}}, now=2.0)
        assert [m["type"] for m in drain(service, "r", "bob")] == ["relay_data"]
        assert drain(service, "r", "carol") == []
        # recovery: direct connection works again
        await service.handle("r", "alice",
                             {"type": "relay_stop", "target": "bob"}, now=3.0)
        for cid in ("alice", "bob", "carol"):
            msgs = drain(service, "r", cid)
            assert any(m["type"] == "relay_state" and m["active"] is False
                       for m in msgs), cid
        assert not service.room("r").relay.is_relaying("alice", "bob")

    run(scenario())


def test_relay_transition_does_not_interrupt_membership_or_channels(service):
    async def scenario():
        await _setup(service)
        # establish a negotiation before degrading
        await service.handle("r", "alice",
                             {"type": "offer", "target": "bob", "seq": 1,
                              "payload": {"sdp": "A"}}, now=1.0)
        await service.handle("r", "bob",
                             {"type": "answer", "target": "alice", "seq": 1,
                              "payload": {"sdp": "B"}}, now=2.0)
        drain(service, "r", "alice")
        drain(service, "r", "bob")
        # degrade then recover: membership untouched, no one disconnected
        await service.handle("r", "alice",
                             {"type": "relay_start", "target": "bob"}, now=3.0)
        await service.handle("r", "alice",
                             {"type": "relay_stop", "target": "bob"}, now=4.0)
        assert service.room("r").member_ids() == ["alice", "bob", "carol"]
        drain(service, "r", "alice")
        drain(service, "r", "bob")
        drain(service, "r", "carol")
        # the established pair can keep signaling afterwards
        await service.handle("r", "alice",
                             {"type": "ice", "target": "bob", "seq": 2,
                              "payload": {"cand": "c"}}, now=5.0)
        assert [m["type"] for m in drain(service, "r", "bob")] == ["ice"]

    run(scenario())


def test_relay_data_rejected_when_relay_not_active(service):
    async def scenario():
        await _setup(service)
        await service.handle("r", "alice",
                             {"type": "relay_data", "target": "bob", "seq": 1,
                              "payload": {}}, now=1.0)
        errs = drain(service, "r", "alice")
        assert errs[0]["type"] == "error" and errs[0]["code"] == "forbidden"
        assert drain(service, "r", "bob") == []

    run(scenario())


def test_relay_state_cleared_when_member_leaves(service):
    async def scenario():
        await _setup(service)
        await service.handle("r", "alice",
                             {"type": "relay_start", "target": "bob"}, now=1.0)
        await service.disconnect("r", "bob", now=2.0)
        assert service.room("r").relay.pairs() == []

    run(scenario())
