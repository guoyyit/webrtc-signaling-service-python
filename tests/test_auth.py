"""Credential issuance, expiry and role-based access tests."""

from __future__ import annotations

from signaling.models import Role

from conftest import join, make_server


def test_token_roundtrip(server):
    token = server.auth.issue_token("r1", "alice", Role.HOST, now=1000.0)
    claims = server.auth.verify_token(token, "r1", now=1000.0)
    assert claims is not None
    assert claims.member_id == "alice"
    assert claims.role == Role.HOST
    assert claims.room_id == "r1"


def test_expired_token_rejected(server):
    token = server.auth.issue_token("r1", "alice", Role.MEMBER, now=1000.0)
    assert server.auth.verify_token(token, "r1", now=1000.0 + 3600 + 1) is None


def test_token_for_other_room_rejected(server):
    token = server.auth.issue_token("r1", "alice", Role.MEMBER, now=1000.0)
    assert server.auth.verify_token(token, "r2", now=1000.0) is None


def test_tampered_token_rejected(server):
    token = server.auth.issue_token("r1", "alice", Role.MEMBER, now=1000.0)
    header, payload, sig = token.split(".")
    tampered = f"{header}.{payload}.{'a' * len(sig)}"
    assert server.auth.verify_token(tampered, "r1", now=1000.0) is None


def test_garbage_token_rejected(server):
    assert server.auth.verify_token("not.a.jwt", "r1", now=1000.0) is None
    assert server.auth.verify_token("garbage", "r1", now=1000.0) is None
    assert server.auth.verify_token("", "r1", now=1000.0) is None


def test_open_session_rejects_expired_token(server, clock):
    server.rooms.create_room("r1")
    token = server.auth.issue_token("r1", "alice", Role.MEMBER, now=clock())
    clock.advance(3600 + 1)
    session, events = server.open_session(token, "r1", "s1")
    assert session is None
    assert events[0].message.payload["code"] == "unauthorized"
    assert server.rooms.member_count("r1") == 0


def test_open_session_rejects_unknown_room(server):
    token = server.auth.issue_token("nope", "alice", Role.MEMBER, now=1000.0)
    session, events = server.open_session(token, "nope", "s1")
    assert session is None
    assert events[0].message.payload["code"] == "room_not_found"


def test_host_only_kick_requires_host(server):
    server.rooms.create_room("r1")
    join(server, "r1", "bob", Role.MEMBER, session_id="bob")
    join(server, "r1", "alice", Role.HOST, session_id="alice")
    # A member cannot kick.
    events = server.handle_frame(
        "bob", {"type": "kick", "payload": {"target": "alice"}}
    )
    bob = server.sessions.get("bob")
    codes = [m.payload.get("code") for m in bob.poll_outbox() if m.type.value == "error"]
    assert "forbidden" in codes
    # Host can kick.
    events = server.handle_frame(
        "alice", {"type": "kick", "payload": {"target": "bob"}}
    )
    assert server.sessions.get("bob") is None
    assert server.rooms.member_count("r1") == 1


def test_viewer_cannot_send_offer(server):
    server.rooms.create_room("r1")
    join(server, "r1", "alice", Role.MEMBER, session_id="alice")
    join(server, "r1", "vic", Role.VIEWER, session_id="vic")
    server.handle_frame("vic", {"type": "offer", "target_id": "alice", "seq": 1})
    vic = server.sessions.get("vic")
    codes = [m.payload.get("code") for m in vic.poll_outbox() if m.type.value == "error"]
    assert "forbidden" in codes
    # No offer reached alice (only the join broadcast was queued).
    alice = server.sessions.get("alice")
    types = [m.type.value for m in alice.poll_outbox()]
    assert "offer" not in types
    assert "answer" not in types
    assert "ice_candidate" not in types
