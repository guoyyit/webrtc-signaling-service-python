"""Negotiation state machine, glare resolution and timeout tests."""

from __future__ import annotations

from signaling.models import MessageType, NegotiationPhase

from conftest import join, make_server


def _setup(server):
    server.rooms.create_room("r1")
    join(server, "r1", "a", session_id="a")
    join(server, "r1", "b", session_id="b")
    a, b = server.sessions.get("a"), server.sessions.get("b")
    a.poll_outbox()
    b.poll_outbox()
    return a, b


def _offer(server, sender, target, seq, now=None):
    return server.handle_frame(
        sender.session_id,
        {"type": "offer", "target_id": target.member_id, "seq": seq,
         "payload": {"sdp": f"offer-{seq}"}},
    )


def _answer(server, sender, target, seq):
    return server.handle_frame(
        sender.session_id,
        {"type": "answer", "target_id": target.member_id, "seq": seq,
         "payload": {"sdp": f"answer-{seq}"}},
    )


def test_offer_answer_flow(server):
    a, b = _setup(server)
    _offer(server, a, b, 1)
    assert [m.type.value for m in b.poll_outbox()] == ["offer"]
    _answer(server, b, a, 1)
    assert [m.type.value for m in a.poll_outbox()] == ["answer"]
    state = server.negotiations.get("r1", "a", "b")
    assert state.phase == NegotiationPhase.CONNECTED


def test_duplicate_offer_is_dropped(server):
    a, b = _setup(server)
    _offer(server, a, b, 1)
    _offer(server, a, b, 1)  # duplicate
    frames = [m for m in b.poll_outbox() if m.type == MessageType.OFFER]
    assert len(frames) == 1


def test_duplicate_answer_is_dropped(server):
    a, b = _setup(server)
    _offer(server, a, b, 1)
    _answer(server, b, a, 1)
    _answer(server, b, a, 1)  # duplicate
    frames = [m for m in a.poll_outbox() if m.type == MessageType.ANSWER]
    assert len(frames) == 1


def test_answer_without_offer_does_not_break_negotiation(server):
    a, b = _setup(server)
    _answer(server, b, a, 1)  # no pending offer
    assert [m.type.value for m in a.poll_outbox()] == []
    # Normal negotiation still works afterwards.
    _offer(server, a, b, 1)
    assert [m.type.value for m in b.poll_outbox()] == ["offer"]


def test_glare_polite_side_abandons(server):
    a, b = _setup(server)
    # Both offer at the same time. a < b, so a (impolite) wins.
    _offer(server, a, b, 1)
    _offer(server, b, a, 1)

    a_frames = [m for m in a.poll_outbox()]
    b_frames = [m for m in b.poll_outbox()]

    # a's offer was forwarded to b.
    assert any(m.type == MessageType.OFFER and m.sender_id == "a" for m in b_frames)
    # b must receive explicit abandonment so it can end and re-initiate.
    abandoned = [m for m in b_frames if m.type == MessageType.NEGOTIATION_ABANDONED]
    assert len(abandoned) == 1
    assert abandoned[0].payload["peer"] == "a"
    assert abandoned[0].payload["reason"] == "glare"
    # a is never told to abandon.
    assert not any(m.type == MessageType.NEGOTIATION_ABANDONED for m in a_frames)

    # b ends its abandoned negotiation and answers a's offer -> connected.
    _answer(server, b, a, 1)
    state = server.negotiations.get("r1", "a", "b")
    assert state.phase == NegotiationPhase.CONNECTED


def test_glare_winner_is_deterministic_regardless_of_order(server):
    a, b = _setup(server)
    # b offers first this time; a still wins because a < b.
    _offer(server, b, a, 1)
    _offer(server, a, b, 1)

    b_frames = [m for m in b.poll_outbox()]
    a_frames = [m for m in a.poll_outbox()]
    assert any(m.type == MessageType.NEGOTIATION_ABANDONED for m in b_frames)
    assert not any(m.type == MessageType.NEGOTIATION_ABANDONED for m in a_frames)
    assert any(m.type == MessageType.OFFER and m.sender_id == "a" for m in b_frames)


def test_abandoned_side_can_reoffer_after_ending(server):
    a, b = _setup(server)
    _offer(server, a, b, 1)
    _offer(server, b, a, 1)
    b.poll_outbox()
    # b abandons by answering a's offer (polite peer yields then accepts).
    _answer(server, b, a, 1)
    # Renegotiation: b can initiate again after the round ends.
    server.handle_frame(
        "b",
        {"type": "renegotiate", "target_id": "a", "seq": 2, "payload": {}},
    )
    state = server.negotiations.get("r1", "a", "b")
    assert state.phase == NegotiationPhase.IDLE
    _offer(server, b, a, 3)
    assert any(m.type == MessageType.OFFER and m.sender_id == "b"
               for m in a.poll_outbox())


def test_ice_candidates_forwarded_in_order(server):
    a, b = _setup(server)
    _offer(server, a, b, 1)
    _answer(server, b, a, 1)
    for seq in (1, 2, 3):
        server.handle_frame(
            "a",
            {"type": "ice_candidate", "target_id": "b", "seq": seq,
             "payload": {"candidate": f"c{seq}"}},
        )
    frames = [m for m in b.poll_outbox() if m.type == MessageType.ICE_CANDIDATE]
    assert [m.seq for m in frames] == [1, 2, 3]
    # Duplicate candidate is dropped and enqueues nothing new.
    server.handle_frame(
        "a",
        {"type": "ice_candidate", "target_id": "b", "seq": 2, "payload": {}},
    )
    assert [m for m in b.poll_outbox() if m.type == MessageType.ICE_CANDIDATE] == []


def test_negotiation_timeout_notifies_both_sides(server, clock):
    a, b = _setup(server)
    _offer(server, a, b, 1)
    clock.advance(server.config.negotiation_timeout + 1)
    events = server.tick()

    a_frames = [m for m in a.poll_outbox()]
    b_frames = [m for m in b.poll_outbox()]
    assert any(m.type == MessageType.NEGOTIATION_ABANDONED and m.payload["reason"] == "timeout"
               for m in a_frames)
    assert any(m.type == MessageType.NEGOTIATION_ABANDONED and m.payload["reason"] == "timeout"
               for m in b_frames)
    # After timeout the pair can retry.
    state = server.negotiations.get("r1", "a", "b")
    assert state.phase == NegotiationPhase.IDLE
    assert state.retries == 1


def test_retries_exhausted_marks_failed(server, clock):
    server = make_server(clock=clock, max_retries=1, negotiation_timeout=5)
    a, b = _setup(server)
    _offer(server, a, b, 1)
    clock.advance(6)
    server.tick()
    _offer(server, a, b, 2)  # retry
    clock.advance(6)
    server.tick()
    state = server.negotiations.get("r1", "a", "b")
    assert state.phase == NegotiationPhase.FAILED
    a_frames = [m for m in a.poll_outbox()]
    assert any(m.payload["reason"] == "failed" for m in a_frames)


def test_negotiation_message_reaches_only_target(server):
    a, b = _setup(server)
    join(server, "r1", "c", session_id="c")
    c = server.sessions.get("c")
    c.poll_outbox()
    a.poll_outbox()
    b.poll_outbox()
    _offer(server, a, b, 1)
    # c must not see a's offer.
    assert [m.type.value for m in c.poll_outbox()] == []
    assert [m.type.value for m in b.poll_outbox()] == ["offer"]
