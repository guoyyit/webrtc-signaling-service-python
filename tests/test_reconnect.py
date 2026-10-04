"""Disconnect / reconnect consistency tests."""

from __future__ import annotations

from signaling.models import MessageType, Role

from conftest import join


def test_disconnect_and_rejoin_single_member(server):
    server.rooms.create_room("r1")
    join(server, "r1", "a", session_id="a")
    join(server, "r1", "b", session_id="b")
    for sid in ("a", "b"):
        server.sessions.get(sid).poll_outbox()

    # b disconnects.
    server.close_session("b", reason="disconnect")
    assert server.rooms.member_count("r1") == 1
    a = server.sessions.get("a")
    assert any(m.type == MessageType.MEMBER_LEFT and m.payload["member_id"] == "b"
               for m in a.poll_outbox())

    # b rejoins with a fresh credential: exactly one member, sees room state.
    token = server.auth.issue_token("r1", "b", Role.MEMBER, now=server.clock())
    session, events = server.open_session(token, "r1", "b-new")
    assert session is not None
    assert server.rooms.member_count("r1") == 2
    members = events[0].message.payload["members"]
    assert {m["member_id"] for m in members} == {"a", "b"}
    # a sees b rejoin.
    assert any(m.type == MessageType.MEMBER_JOINED and m.payload["member_id"] == "b"
               for m in a.poll_outbox())


def test_no_duplicate_members_on_repeated_join(server):
    server.rooms.create_room("r1")
    join(server, "r1", "a", session_id="a")
    # Same member joins again with a new session (e.g. reconnect before drop).
    token = server.auth.issue_token("r1", "a", Role.MEMBER, now=server.clock())
    session, _ = server.open_session(token, "r1", "a-second")
    assert session is not None
    assert server.rooms.member_count("r1") == 1
    assert server.sessions.get("a") is None  # old connection replaced
    assert server.sessions.get("a-second") is not None


def test_offer_to_offline_member_fails_then_succeeds_after_return(server):
    server.rooms.create_room("r1")
    join(server, "r1", "a", session_id="a")
    join(server, "r1", "b", session_id="b")
    for sid in ("a", "b"):
        server.sessions.get(sid).poll_outbox()

    server.close_session("b", reason="disconnect")
    a = server.sessions.get("a")
    a.poll_outbox()

    # b is gone: offer cannot be delivered.
    server.handle_frame(
        "a",
        {"type": "offer", "target_id": "b", "seq": 1, "payload": {"sdp": "x"}},
    )
    assert any(m.payload.get("code") in ("target_not_found", "member_unavailable")
               for m in a.poll_outbox())

    # b returns.
    token = server.auth.issue_token("r1", "b", Role.MEMBER, now=server.clock())
    server.open_session(token, "r1", "b-new")
    a.poll_outbox()

    # Negotiation works again.
    server.handle_frame(
        "a",
        {"type": "offer", "target_id": "b", "seq": 2, "payload": {"sdp": "y"}},
    )
    b = server.sessions.get("b-new")
    assert any(m.type == MessageType.OFFER and m.seq == 2 for m in b.poll_outbox())


def test_relay_state_cleared_on_disconnect(server):
    server.rooms.create_room("r1")
    join(server, "r1", "a", session_id="a")
    join(server, "r1", "b", session_id="b")
    for sid in ("a", "b"):
        server.sessions.get(sid).poll_outbox()
    server.handle_frame(
        "a",
        {"type": "link_report", "target_id": "b", "payload": {"mode": "relay"}},
    )
    assert server.relay.get("r1", "a", "b") is not None
    server.close_session("b", reason="disconnect")
    assert server.relay.get("r1", "a", "b") is None
