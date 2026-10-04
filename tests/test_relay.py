"""Relay fallback degradation and recovery tests."""

from __future__ import annotations

from signaling.models import LinkMode, MessageType

from conftest import join


def _setup_three(server):
    server.rooms.create_room("r1")
    join(server, "r1", "a", session_id="a")
    join(server, "r1", "b", session_id="b")
    join(server, "r1", "c", session_id="c")
    for sid in ("a", "b", "c"):
        server.sessions.get(sid).poll_outbox()


def _report(server, sender, target, mode):
    return server.handle_frame(
        sender.session_id,
        {"type": "link_report", "target_id": target.member_id,
         "payload": {"mode": mode}},
    )


def test_relay_degradation_notifies_room(server):
    _setup_three(server)
    a, b, c = (server.sessions.get(s) for s in ("a", "b", "c"))
    _report(server, a, b, "relay")

    for s in (a, b, c):
        frames = [m for m in s.poll_outbox() if m.type == MessageType.LINK_STATE]
        assert len(frames) == 1
        assert frames[0].payload["mode"] == "relay"
        assert frames[0].payload["peer_a"] == "a"
        assert frames[0].payload["peer_b"] == "b"
    # Link state recorded.
    assert server.relay.get("r1", "a", "b") == LinkMode.RELAY


def test_duplicate_relay_report_ignored(server):
    _setup_three(server)
    a, b = server.sessions.get("a"), server.sessions.get("b")
    _report(server, a, b, "relay")
    for s in (a, b):
        s.poll_outbox()
    _report(server, a, b, "relay")  # duplicate
    for s in (a, b):
        assert [m for m in s.poll_outbox() if m.type == MessageType.LINK_STATE] == []


def test_recovery_to_direct_notifies_room(server):
    _setup_three(server)
    a, b, c = (server.sessions.get(s) for s in ("a", "b", "c"))
    _report(server, a, b, "relay")
    for s in (a, b, c):
        s.poll_outbox()
    _report(server, a, b, "direct")
    for s in (a, b, c):
        frames = [m for m in s.poll_outbox() if m.type == MessageType.LINK_STATE]
        assert len(frames) == 1
        assert frames[0].payload["mode"] == "direct"
    assert server.relay.get("r1", "a", "b") == LinkMode.DIRECT


def test_degradation_does_not_interrupt_other_pairs(server):
    _setup_three(server)
    a, b, c = (server.sessions.get(s) for s in ("a", "b", "c"))
    # a<->b degrade to relay.
    _report(server, a, b, "relay")
    for s in (a, b, c):
        s.poll_outbox()
    # c<->a data channel still negotiates normally.
    server.handle_frame(
        "c",
        {"type": "offer", "target_id": "a", "seq": 1, "payload": {"sdp": "x"}},
    )
    offer = [m for m in a.poll_outbox() if m.type == MessageType.OFFER]
    assert len(offer) == 1
    # c<->b link is untouched.
    assert server.relay.get("r1", "c", "b") is None


def test_link_state_cleared_when_peer_leaves(server):
    _setup_three(server)
    a, b = server.sessions.get("a"), server.sessions.get("b")
    _report(server, a, b, "relay")
    a.poll_outbox()
    server.close_session("b", reason="leave")
    assert server.relay.get("r1", "a", "b") is None


def test_invalid_link_mode_rejected(server):
    _setup_three(server)
    a, b = server.sessions.get("a"), server.sessions.get("b")
    _report(server, a, b, "bogus")
    errors = [m for m in a.poll_outbox() if m.type == MessageType.ERROR]
    assert errors and errors[0].payload["code"] == "bad_frame"
    assert server.relay.get("r1", "a", "b") is None
