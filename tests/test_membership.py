"""Duplicate join, rejoin after disconnect, capacity, presence consistency."""

from __future__ import annotations

from signaling.models import Role
from signaling.service import ServiceError
from tests.conftest import FakeConn, drain, join, run


def test_duplicate_join_does_not_create_duplicate_member(service):
    async def scenario():
        c1 = await join(service, "r", "alice", Role.HOST, now=0.0)
        await join(service, "r", "bob", now=1.0)
        drain(service, "r", "alice")  # clear join traffic

        # alice rejoins with a new connection (same client id)
        c2 = await join(service, "r", "alice", Role.HOST, now=2.0)
        assert service.room("r").member_ids() == ["alice", "bob"]
        # old connection was replaced, no duplicate peer_joined broadcast
        assert c1.closed == (4000, "replaced by rejoin")
        bob_msgs = drain(service, "r", "bob")
        assert not any(m["type"] == "peer_joined" for m in bob_msgs)
        # rejoin ack carries the current member list
        acks = [m for m in drain(service, "r", "alice") if m["type"] == "join_ack"]
        assert acks and acks[0]["rejoined"] is True
        assert {m["client_id"] for m in acks[0]["members"]} == {"bob"}

    run(scenario())


def test_rejoin_after_disconnect_has_no_duplicate(service):
    async def scenario():
        await join(service, "r", "alice", now=0.0)
        await join(service, "r", "bob", now=1.0)
        await service.disconnect("r", "alice", now=2.0)
        assert service.room("r").member_ids() == ["bob"]
        # bob sees alice leave, then rejoin exactly once
        assert any(m["type"] == "peer_left" for m in drain(service, "r", "bob"))
        await join(service, "r", "alice", now=3.0)
        assert service.room("r").member_ids() == ["alice", "bob"]
        joined = [m for m in drain(service, "r", "bob") if m["type"] == "peer_joined"]
        assert len(joined) == 1 and joined[0]["member"]["client_id"] == "alice"

    run(scenario())


def test_room_capacity_enforced(service):
    async def scenario():
        for i, cid in enumerate(["a", "b", "c", "d"]):
            await join(service, "r", cid, now=float(i))
        try:
            await join(service, "r", "e", now=5.0)
            raise AssertionError("join beyond capacity must fail")
        except ServiceError as exc:
            assert exc.code.value == "room_full"
        # a member already in the room may still rejoin when full
        await join(service, "r", "a", now=6.0)
        assert len(service.room("r").member_ids()) == 4

    run(scenario())


def test_membership_equals_online_set(service):
    async def scenario():
        await join(service, "r", "alice", now=0.0)
        await join(service, "r", "bob", now=1.0)
        infos = service.room("r").members()
        assert all(m.status.value == "online" for m in infos)
        await service.disconnect("r", "bob", now=2.0)
        assert [m.client_id for m in service.room("r").members()] == ["alice"]

    run(scenario())
