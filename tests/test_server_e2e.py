"""End-to-end test over real WebSockets: join, negotiate, reconnect."""

from __future__ import annotations

import asyncio
import json
import time

import uvicorn
from websockets.asyncio.client import connect

from signaling.config import SignalingConfig
from signaling.models import Role
from signaling.server import create_app
from tests.conftest import run

PORT = 8977


async def _recv_json(ws, timeout=5.0):
    return json.loads(await asyncio.wait_for(ws.recv(), timeout))


def test_websocket_join_negotiate_reconnect():
    async def scenario():
        cfg = SignalingConfig(heartbeat_interval_s=3600.0, token_secret="e2e")
        app = create_app(cfg)
        svc = app.state.service
        server = uvicorn.Server(
            uvicorn.Config(app, host="127.0.0.1", port=PORT, log_level="error"))
        task = asyncio.create_task(server.serve())
        await asyncio.sleep(0.8)
        try:
            ta = svc.issuer.issue("r", "alice", Role.HOST, now=time.time())
            tb = svc.issuer.issue("r", "bob", Role.PARTICIPANT, now=time.time())
            url_a = f"ws://127.0.0.1:{PORT}/ws/r?token={ta}"
            url_b = f"ws://127.0.0.1:{PORT}/ws/r?token={tb}"

            async with connect(url_a) as wa, connect(url_b) as wb:
                assert (await _recv_json(wa))["client_id"] == "alice"
                assert (await _recv_json(wb))["type"] == "join_ack"
                assert (await _recv_json(wa))["type"] == "peer_joined"
                # negotiate
                await wa.send(json.dumps(
                    {"type": "offer", "target": "bob", "seq": 1,
                     "payload": {"sdp": "A"}}))
                offer = await _recv_json(wb)
                assert offer["type"] == "offer" and offer["sender"] == "alice"
                await wb.send(json.dumps(
                    {"type": "answer", "target": "alice", "seq": 1,
                     "payload": {"sdp": "B"}}))
                assert (await _recv_json(wa))["type"] == "answer"
                # bob reconnects with the same id: no duplicate member
                async with connect(url_b) as wb2:
                    ack = await _recv_json(wb2)
                    assert ack["rejoined"] is True
                    assert svc.room("r").member_ids() == ["alice", "bob"]
                    # old socket of bob was replaced; alice still connected
                    await wb2.send(json.dumps(
                        {"type": "ice", "target": "alice", "seq": 2,
                         "payload": {"cand": "c"}}))
                    ice = await _recv_json(wa)
                    assert ice["type"] == "ice" and ice["sender"] == "bob"
                # wb2 closed -> alice sees bob leave exactly once
                left = await _recv_json(wa)
                assert left["type"] == "peer_left" and left["client_id"] == "bob"
        finally:
            server.should_exit = True
            await task

    run(scenario())
