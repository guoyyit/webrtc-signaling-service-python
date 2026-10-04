"""End-to-end HTTP + WebSocket transport tests via FastAPI TestClient."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from main import app


@pytest.fixture
def client():
    with TestClient(app) as c:
        yield c


def _create_room(client, room_id=None, capacity=None):
    resp = client.post("/rooms", json={"room_id": room_id, "capacity": capacity})
    assert resp.status_code == 200, resp.text
    return resp.json()["room_id"]


def _token(client, room_id, member_id, role="member"):
    resp = client.post(f"/rooms/{room_id}/tokens", json={"member_id": member_id, "role": role})
    assert resp.status_code == 200, resp.text
    return resp.json()["token"]


def test_health(client):
    assert client.get("/health").json() == {"status": "ok"}


def test_create_room_and_token(client):
    room_id = _create_room(client)
    token = _token(client, room_id, "alice", "host")
    assert isinstance(token, str) and token.count(".") == 2


def test_duplicate_room_returns_409(client):
    _create_room(client, room_id="dup")
    resp = client.post("/rooms", json={"room_id": "dup"})
    assert resp.status_code == 409


def test_websocket_join_and_offer_answer(client):
    room_id = _create_room(client)
    tok_a = _token(client, room_id, "alice", "host")
    tok_b = _token(client, room_id, "bob", "member")

    with client.websocket_connect(f"/ws/{room_id}?token={tok_a}") as ws_a:
        joined = ws_a.receive_json()
        assert joined["type"] == "joined"
        assert {m["member_id"] for m in joined["payload"]["members"]} == {"alice"}

        with client.websocket_connect(f"/ws/{room_id}?token={tok_b}") as ws_b:
            # alice sees bob arrive; bob gets snapshot.
            assert ws_a.receive_json()["type"] == "member_joined"
            joined_b = ws_b.receive_json()
            assert joined_b["type"] == "joined"
            assert {m["member_id"] for m in joined_b["payload"]["members"]} == {"alice", "bob"}

            # alice -> bob offer (targeted).
            ws_a.send_json({"type": "offer", "target_id": "bob", "seq": 1,
                            "payload": {"sdp": "offer"}})
            offer = ws_b.receive_json()
            assert offer["type"] == "offer"
            assert offer["sender_id"] == "alice"
            assert offer["target_id"] == "bob"

            # bob -> alice answer.
            ws_b.send_json({"type": "answer", "target_id": "alice", "seq": 1,
                            "payload": {"sdp": "answer"}})
            answer = ws_a.receive_json()
            assert answer["type"] == "answer"
            assert answer["sender_id"] == "bob"


def test_relay_degradation_reaches_room(client):
    room_id = _create_room(client)
    tok_a = _token(client, room_id, "alice")
    tok_b = _token(client, room_id, "bob")

    with client.websocket_connect(f"/ws/{room_id}?token={tok_a}") as ws_a:
        ws_a.receive_json()  # joined
        with client.websocket_connect(f"/ws/{room_id}?token={tok_b}") as ws_b:
            ws_a.receive_json()  # member_joined
            ws_b.receive_json()  # joined

            ws_a.send_json({"type": "link_report", "target_id": "bob",
                            "payload": {"mode": "relay"}})
            state_a = ws_a.receive_json()
            state_b = ws_b.receive_json()
            assert state_a["type"] == "link_state"
            assert state_a["payload"]["mode"] == "relay"
            assert state_b["type"] == "link_state"


def test_invalid_token_rejected(client):
    room_id = _create_room(client)
    with client.websocket_connect(f"/ws/{room_id}?token=garbage") as ws:
        msg = ws.receive_json()
        assert msg["type"] == "error"
        assert msg["payload"]["code"] == "unauthorized"


def test_viewer_forbidden_over_websocket(client):
    room_id = _create_room(client)
    tok_v = _token(client, room_id, "vic", "viewer")
    with client.websocket_connect(f"/ws/{room_id}?token={tok_v}") as ws:
        ws.receive_json()  # joined
        ws.send_json({"type": "offer", "target_id": "nobody", "seq": 1})
        msg = ws.receive_json()
        assert msg["type"] == "error"
        assert msg["payload"]["code"] == "forbidden"


def test_unknown_type_does_not_kick_connection(client):
    room_id = _create_room(client)
    tok_a = _token(client, room_id, "alice")
    tok_b = _token(client, room_id, "bob")
    with client.websocket_connect(f"/ws/{room_id}?token={tok_a}") as ws_a:
        ws_a.receive_json()
        with client.websocket_connect(f"/ws/{room_id}?token={tok_b}") as ws_b:
            ws_a.receive_json()
            ws_b.receive_json()
            ws_a.send_json({"type": "frobnicate", "target_id": "bob"})
            err = ws_a.receive_json()
            assert err["type"] == "error"
            # Connection survives and still forwards.
            ws_a.send_json({"type": "offer", "target_id": "bob", "seq": 1})
            assert ws_b.receive_json()["type"] == "offer"
