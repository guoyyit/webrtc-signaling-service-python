"""Glare resolution, ordered/targeted forwarding, dedup, timeout retries."""

from __future__ import annotations

from signaling.models import Role
from tests.conftest import drain, join, run


async def _setup(service):
    await join(service, "r", "alice", Role.HOST, now=0.0)
    await join(service, "r", "bob", now=0.0)
    await join(service, "r", "carol", now=0.0)
    for cid in ("alice", "bob", "carol"):
        drain(service, "r", cid)


def test_targeted_offer_reaches_only_the_target(service):
    async def scenario():
        await _setup(service)
        await service.handle("r", "alice",
                             {"type": "offer", "target": "bob", "seq": 1,
                              "payload": {"sdp": "A"}}, now=1.0)
        bob = drain(service, "r", "bob")
        assert [m["type"] for m in bob] == ["offer"]
        assert bob[0]["sender"] == "alice" and bob[0]["target"] == "bob"
        assert drain(service, "r", "carol") == []  # no broadcast to others

    run(scenario())


def test_glare_polite_side_wins_and_impolite_rolls_back(service):
    async def scenario():
        await _setup(service)
        # both offer at the same time; "bob" > "alice" so bob is polite
        await service.handle("r", "alice",
                             {"type": "offer", "target": "bob", "seq": 1,
                              "payload": {"sdp": "A"}}, now=1.0)
        await service.handle("r", "bob",
                             {"type": "offer", "target": "alice", "seq": 1,
                              "payload": {"sdp": "B"}}, now=1.0)
        alice = drain(service, "r", "alice")
        bob = drain(service, "r", "bob")
        # alice (impolite) is told to roll back; bob's offer is forwarded
        assert any(m["type"] == "glare_resolved" and m["action"] == "rollback"
                   and m["winner"] == "bob" for m in alice)
        assert any(m["type"] == "offer" and m["sender"] == "bob" for m in alice)
        # bob's earlier in-flight offer from alice is explicitly voided:
        # bob is told to ignore it and must never answer it
        assert any(m["type"] == "glare_resolved"
                   and m["action"] == "ignore_peer_offer" for m in bob)
        # alice answers bob's offer: single winner, no deadlock
        await service.handle("r", "alice",
                             {"type": "answer", "target": "bob", "seq": 1,
                              "payload": {"sdp": "ans"}}, now=2.0)
        assert [m["type"] for m in drain(service, "r", "bob")] == ["answer"]

    run(scenario())


def test_glare_loser_can_reoffer_after_rollback(service):
    async def scenario():
        await _setup(service)
        await service.handle("r", "alice",
                             {"type": "offer", "target": "bob", "seq": 1,
                              "payload": {}}, now=1.0)
        await service.handle("r", "bob",
                             {"type": "offer", "target": "alice", "seq": 1,
                              "payload": {}}, now=1.0)
        drain(service, "r", "alice")
        drain(service, "r", "bob")
        await service.handle("r", "alice",
                             {"type": "answer", "target": "bob", "seq": 1,
                              "payload": {}}, now=2.0)
        drain(service, "r", "bob")
        # negotiation finished; alice may start a fresh one
        await service.handle("r", "alice",
                             {"type": "offer", "target": "bob", "seq": 2,
                              "payload": {"sdp": "A2"}}, now=3.0)
        bob = drain(service, "r", "bob")
        assert [m["type"] for m in bob] == ["offer"] and bob[0]["seq"] == 2

    run(scenario())


def test_duplicate_and_stale_messages_do_not_corrupt_state(service):
    async def scenario():
        await _setup(service)
        offer = {"type": "offer", "target": "bob", "seq": 1, "payload": {"sdp": "A"}}
        await service.handle("r", "alice", dict(offer), now=1.0)
        await service.handle("r", "alice", dict(offer), now=1.1)  # duplicate
        await service.handle("r", "alice", dict(offer), now=1.2)  # duplicate again
        assert len([m for m in drain(service, "r", "bob") if m["type"] == "offer"]) == 1
        await service.handle("r", "bob",
                             {"type": "answer", "target": "alice", "seq": 1,
                              "payload": {}}, now=2.0)
        await service.handle("r", "bob",
                             {"type": "answer", "target": "alice", "seq": 1,
                              "payload": {}}, now=2.1)  # duplicate answer
        assert len([m for m in drain(service, "r", "alice") if m["type"] == "answer"]) == 1
        # stale answer after completion is ignored, state stays clean
        await service.handle("r", "bob",
                             {"type": "answer", "target": "alice", "seq": 2,
                              "payload": {}}, now=3.0)
        assert drain(service, "r", "alice") == []

    run(scenario())


def test_unknown_message_type_does_not_disrupt_others(service):
    async def scenario():
        await _setup(service)
        await service.handle("r", "alice", {"type": "teleport"}, now=1.0)
        err = drain(service, "r", "alice")
        assert err[0]["type"] == "error" and err[0]["code"] == "unknown_message"
        # normal forwarding still works for everyone
        await service.handle("r", "bob",
                             {"type": "ice", "target": "carol", "seq": 1,
                              "payload": {"cand": "x"}}, now=2.0)
        assert [m["type"] for m in drain(service, "r", "carol")] == ["ice"]

    run(scenario())


def test_negotiation_timeout_retries_then_fails(service):
    async def scenario():
        await _setup(service)
        await service.handle("r", "alice",
                             {"type": "offer", "target": "bob", "seq": 1,
                              "payload": {"sdp": "A"}}, now=0.0)
        assert len([m for m in drain(service, "r", "bob") if m["type"] == "offer"]) == 1
        # no answer arrives: offer is re-forwarded on each sweep, then fails
        await service.sweep(now=6.0)
        await service.sweep(now=12.0)
        assert len([m for m in drain(service, "r", "bob") if m["type"] == "offer"]) == 2
        # keep all members' heartbeats alive so only the negotiation times out
        for cid in ("alice", "bob", "carol"):
            service.room("r").touch(cid, 17.0)
        await service.sweep(now=18.0)
        assert drain(service, "r", "bob") == []  # retries exhausted
        errs = [m for m in drain(service, "r", "alice") if m["type"] == "error"]
        assert errs and "failed" in errs[0]["detail"]
        assert any(a["action"] == "negotiation_failed" for a in service.audit_log)
        # both sides can negotiate again afterwards
        await service.handle("r", "bob",
                             {"type": "offer", "target": "alice", "seq": 1,
                              "payload": {}}, now=20.0)
        assert [m["type"] for m in drain(service, "r", "alice")] == ["offer"]

    run(scenario())
