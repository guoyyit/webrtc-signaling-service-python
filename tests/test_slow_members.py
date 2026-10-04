"""Slow consumer isolation and heartbeat timeout tests."""

from __future__ import annotations

from signaling.models import MessageType, SignalingMessage

from conftest import join, make_server


def _msg(room_id, seq):
    return SignalingMessage(
        type=MessageType.OFFER,
        room_id=room_id,
        sender_id="server",
        target_id=None,
        payload={"n": seq},
        seq=seq,
    )


def test_slow_consumer_is_isolated(server):
    server = make_server(send_queue_size=2)
    server.rooms.create_room("r1")
    join(server, "r1", "a", session_id="a")
    join(server, "r1", "b", session_id="b")
    join(server, "r1", "c", session_id="c")
    for sid in ("a", "b", "c"):
        server.sessions.get(sid).poll_outbox()

    b = server.sessions.get("b")
    # Fill b's bounded queue.
    assert server.sessions.send_to("r1", "b", _msg("r1", 1)) is True
    assert server.sessions.send_to("r1", "b", _msg("r1", 2)) is True
    # Third message: queue full -> isolated, not blocked.
    assert server.sessions.send_to("r1", "b", _msg("r1", 3)) is False
    assert b.isolated is True

    # The room keeps working for everyone else: a<->c link report flows.
    events = server.handle_frame(
        "a",
        {"type": "link_report", "target_id": "c", "payload": {"mode": "relay"}},
    )
    c = server.sessions.get("c")
    assert any(m.type == MessageType.LINK_STATE for m in c.poll_outbox())
    # b was evicted and a member_left notice was sent to the room.
    assert server.sessions.get("b") is None
    assert server.rooms.member_count("r1") == 2
    a = server.sessions.get("a")
    left = [m for m in a.poll_outbox() if m.type == MessageType.MEMBER_LEFT]
    assert any(m.payload["member_id"] == "b" for m in left)


def test_slow_member_does_not_block_forwarding(server):
    server = make_server(send_queue_size=2)
    server.rooms.create_room("r1")
    join(server, "r1", "a", session_id="a")
    for sid in ("a",):
        server.sessions.get(sid).poll_outbox()
    join(server, "r1", "b", session_id="b")
    for sid in ("a", "b"):
        server.sessions.get(sid).poll_outbox()
    join(server, "r1", "c", session_id="c")
    for sid in ("a", "b", "c"):
        server.sessions.get(sid).poll_outbox()

    # Fill b's queue so it is isolated.
    server.sessions.send_to("r1", "b", _msg("r1", 1))
    server.sessions.send_to("r1", "b", _msg("r1", 2))
    server.sessions.send_to("r1", "b", _msg("r1", 3))

    # a -> c negotiation still delivered.
    server.handle_frame(
        "a",
        {"type": "offer", "target_id": "c", "seq": 1, "payload": {"sdp": "x"}},
    )
    c = server.sessions.get("c")
    assert any(m.type == MessageType.OFFER for m in c.poll_outbox())


def test_heartbeat_timeout_is_deterministic(server, clock):
    server = make_server(clock=clock, heartbeat_timeout=15)
    server.rooms.create_room("r1")
    join(server, "r1", "a", session_id="a")
    # b joins later so it stays alive when a times out.
    clock.advance(10)
    join(server, "r1", "b", session_id="b")
    for sid in ("a", "b"):
        server.sessions.get(sid).poll_outbox()

    # Exactly at the boundary: alive (> is strict).
    clock.advance(5)
    server.tick()
    assert server.sessions.get("a") is not None

    # Past the boundary: evicted; b is still alive and sees the leave.
    clock.advance(0.1)
    server.tick()
    assert server.sessions.get("a") is None
    b = server.sessions.get("b")
    left = [m for m in b.poll_outbox() if m.type == MessageType.MEMBER_LEFT]
    assert any(m.payload["reason"] == "heartbeat_timeout" for m in left)
    assert server.rooms.member_count("r1") == 1


def test_ping_keeps_member_alive(server, clock):
    server = make_server(clock=clock, heartbeat_timeout=15)
    server.rooms.create_room("r1")
    join(server, "r1", "a", session_id="a")
    join(server, "r1", "b", session_id="b")
    for sid in ("a", "b"):
        server.sessions.get(sid).poll_outbox()

    clock.advance(10)
    server.handle_frame("a", {"type": "ping", "payload": {"echo": 1}})
    a = server.sessions.get("a")
    assert any(m.type == MessageType.PONG for m in a.poll_outbox())
    clock.advance(10)  # 20s since join, but only 10s since ping
    server.tick()
    assert server.sessions.get("a") is not None


def test_timed_out_member_can_rejoin(server, clock):
    server = make_server(clock=clock, heartbeat_timeout=15)
    server.rooms.create_room("r1")
    join(server, "r1", "a", session_id="a")
    clock.advance(16)
    server.tick()
    assert server.rooms.member_count("r1") == 0
    # Rejoin with a fresh credential: no duplicate members.
    token = server.auth.issue_token("r1", "a", "member" and __import__("signaling.models", fromlist=["Role"]).Role.MEMBER, now=clock())
    session, _ = server.open_session(token, "r1", "a-new")
    assert session is not None
    assert server.rooms.member_count("r1") == 1
