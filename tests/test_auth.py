"""Per-room auth, role-based control messages, auditability."""

from __future__ import annotations

from signaling.models import Role
from signaling.service import ServiceError
from tests.conftest import FakeConn, drain, join, run


def test_invalid_token_rejected_and_audited(service):
    async def scenario():
        try:
            await service.connect("r", "not-a-token", FakeConn(), now=0.0)
            raise AssertionError("must reject")
        except ServiceError as exc:
            assert exc.code.value == "auth_failed"
        assert service.room("r") is None or service.room("r").member_ids() == []
        assert any(a["action"] == "auth_failed" for a in service.audit_log)

    run(scenario())


def test_expired_token_rejected(service):
    async def scenario():
        token = service.issuer.issue("r", "alice", Role.HOST, now=0.0)
        try:
            await service.connect("r", token, FakeConn(), now=1000.0)
            raise AssertionError("must reject")
        except ServiceError as exc:
            assert exc.code.value == "token_expired"

    run(scenario())


def test_token_for_other_room_rejected(service):
    async def scenario():
        token = service.issuer.issue("room-a", "alice", Role.HOST, now=0.0)
        try:
            await service.connect("room-b", token, FakeConn(), now=1.0)
            raise AssertionError("must reject")
        except ServiceError as exc:
            assert exc.code.value == "auth_failed"

    run(scenario())


def test_viewer_control_message_rejected_and_audited(service):
    async def scenario():
        await join(service, "r", "alice", Role.HOST, now=0.0)
        await join(service, "r", "victor", Role.VIEWER, now=0.0)
        drain(service, "r", "alice")
        drain(service, "r", "victor")
        await service.handle("r", "victor",
                             {"type": "relay_start", "target": "alice"}, now=1.0)
        errs = drain(service, "r", "victor")
        assert errs[0]["type"] == "error" and errs[0]["code"] == "forbidden"
        # no relay state was created, nobody else was notified
        assert service.room("r").relay.pairs() == []
        assert drain(service, "r", "alice") == []
        # the violation is traceable
        hits = [a for a in service.audit_log
                if a["action"] == "forbidden" and a["client_id"] == "victor"]
        assert hits and "relay_start" in hits[0]["detail"]

    run(scenario())


def test_host_and_participant_may_send_control_messages(service):
    async def scenario():
        await join(service, "r", "alice", Role.HOST, now=0.0)
        await join(service, "r", "bob", Role.PARTICIPANT, now=0.0)
        await service.handle("r", "bob",
                             {"type": "relay_start", "target": "alice"}, now=1.0)
        assert service.room("r").relay.is_relaying("alice", "bob")

    run(scenario())
