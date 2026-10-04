"""Ordered forwarding and malformed-frame tolerance tests."""

from __future__ import annotations

from signaling.models import MessageType

from conftest import join


def _setup(server):
    server.rooms.create_room("r1")
    join(server, "r1", "a", session_id="a")
    join(server, "r1", "b", session_id="b")
    for sid in ("a", "b"):
        server.sessions.get(sid).poll_outbox()
    return server.sessions.get("a"), server.sessions.get("b")


def test_unknown_message_type_does_not_interrupt_others(server):
    a, b = _setup(server)
    # a sends an unrecognized type: rejected with error.
    server.handle_frame("a", {"type": "frobnicate", "target_id": "b"})
    assert any(m.type == MessageType.ERROR for m in a.poll_outbox())
    # a's subsequent valid offer still reaches b: forwarding not interrupted.
    server.handle_frame(
        "a",
        {"type": "offer", "target_id": "b", "seq": 1, "payload": {"sdp": "x"}},
    )
    assert any(m.type == MessageType.OFFER for m in b.poll_outbox())


def test_malformed_frame_does_not_break_session(server):
    a, b = _setup(server)
    # Not a mapping / missing fields: error, session survives.
    server.handle_frame("a", {"type": "offer"})  # missing target_id
    assert any(m.type == MessageType.ERROR for m in a.poll_outbox())
    server.handle_frame("a", {"type": "offer", "target_id": "b", "seq": 1})
    assert any(m.type == MessageType.OFFER for m in b.poll_outbox())


def test_forwarding_preserves_order(server):
    a, b = _setup(server)
    server.handle_frame(
        "a",
        {"type": "offer", "target_id": "b", "seq": 1, "payload": {"sdp": "x"}},
    )
    b.poll_outbox()
    for seq in (2, 3, 4, 5):
        server.handle_frame(
            "a",
            {"type": "ice_candidate", "target_id": "b", "seq": seq,
             "payload": {"n": seq}},
        )
    frames = [m for m in b.poll_outbox() if m.type == MessageType.ICE_CANDIDATE]
    assert [m.seq for m in frames] == [2, 3, 4, 5]


def test_targeted_message_never_broadcasts(server):
    a, b = _setup(server)
    join(server, "r1", "c", session_id="c")
    c = server.sessions.get("c")
    c.poll_outbox()
    a.poll_outbox()
    b.poll_outbox()
    server.handle_frame(
        "a",
        {"type": "offer", "target_id": "b", "seq": 1, "payload": {}},
    )
    assert [m for m in c.poll_outbox() if m.type == MessageType.OFFER] == []
    assert [m for m in b.poll_outbox() if m.type == MessageType.OFFER]


def test_leave_notifies_room(server):
    a, b = _setup(server)
    server.handle_frame("a", {"type": "leave"})
    assert server.sessions.get("a") is None
    left = [m for m in b.poll_outbox() if m.type == MessageType.MEMBER_LEFT]
    assert any(m.payload["member_id"] == "a" for m in left)
    assert server.rooms.member_count("r1") == 1


def test_ping_returns_pong(server):
    a, _ = _setup(server)
    server.handle_frame("a", {"type": "ping", "payload": {"echo": "abc"}})
    pong = [m for m in a.poll_outbox() if m.type == MessageType.PONG]
    assert len(pong) == 1
    assert pong[0].payload["echo"] == "abc"


def test_forbidden_control_is_traceable(server, caplog):
    import logging

    a, b = _setup(server)
    with caplog.at_level(logging.INFO, logger="signaling"):
        server.handle_frame(
            "a", {"type": "kick", "payload": {"target": "b"}}
        )
    assert any("forbidden_control" in rec.message for rec in caplog.records)
