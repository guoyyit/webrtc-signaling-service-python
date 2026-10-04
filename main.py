"""FastAPI entry point: HTTP management endpoints and WebSocket transport."""

from __future__ import annotations

import asyncio
import logging
import os
import uuid
from contextlib import asynccontextmanager
from typing import Any, Optional

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from pydantic import BaseModel

from signaling.auth import AuthManager
from signaling.config import SignalingConfig, load_config
from signaling.negotiation import NegotiationTracker
from signaling.relay import RelayCoordinator
from signaling.room import RoomExistsError, RoomManager
from signaling.server import ServerEvent, SignalingServer
from signaling.session import SessionManager

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s"
)
logger = logging.getLogger("signaling.transport")


def build_server(config: Optional[SignalingConfig] = None) -> SignalingServer:
    """Wire up the signaling coordinator."""
    config = config or load_config()
    secret = os.environ.get("SIGNALING_SECRET", "dev-only-change-me")
    auth = AuthManager(secret=secret, token_ttl=config.token_ttl)
    rooms = RoomManager(default_capacity=config.room_capacity)
    sessions = SessionManager(
        queue_size=config.send_queue_size,
        heartbeat_timeout=config.heartbeat_timeout,
    )
    negotiations = NegotiationTracker(
        negotiation_timeout=config.negotiation_timeout,
        max_retries=config.max_retries,
    )
    relay = RelayCoordinator()
    return SignalingServer(
        config=config,
        auth=auth,
        rooms=rooms,
        sessions=sessions,
        negotiations=negotiations,
        relay=relay,
    )


class CreateRoomRequest(BaseModel):
    room_id: Optional[str] = None
    capacity: Optional[int] = None


class CreateTokenRequest(BaseModel):
    member_id: str
    role: str = "member"


# -- websocket transport ---------------------------------------------------

class ConnectionRegistry:
    """Maps session ids to live websockets and drains outbound queues."""

    def __init__(self) -> None:
        self._websockets: dict[str, WebSocket] = {}

    def register(self, session_id: str, websocket: WebSocket) -> None:
        self._websockets[session_id] = websocket

    def unregister(self, session_id: str) -> None:
        self._websockets.pop(session_id, None)

    async def drain(self, server: SignalingServer) -> None:
        """Push all queued frames to their websockets in FIFO order."""
        for session in server.sessions.all_sessions():  # noqa: SLF001
            websocket = self._websockets.get(session.session_id)
            if websocket is None:
                continue
            for frame in session.poll_outbox():
                try:
                    await websocket.send_json(frame.to_wire())
                except Exception:  # noqa: BLE001
                    logger.warning("failed to send frame to %s", session.session_id)

    async def close_session(self, session_id: str, reason: str) -> None:
        websocket = self._websockets.pop(session_id, None)
        if websocket is not None:
            try:
                await websocket.close(code=4000, reason=reason)
            except Exception:  # noqa: BLE001
                pass


async def maintenance_loop(
    server: SignalingServer, registry: ConnectionRegistry, interval: float
) -> None:
    """Periodically run deterministic tick maintenance and drain queues."""
    while True:
        await asyncio.sleep(interval)
        try:
            events = server.tick()
            await registry.drain(server)
            for event in events:
                if event.kind == "close" and event.session_id:
                    await registry.close_session(event.session_id, event.reason)
        except Exception:  # noqa: BLE001
            logger.exception("maintenance tick failed")


@asynccontextmanager
async def lifespan(app: FastAPI):
    server = build_server()
    registry = ConnectionRegistry()
    app.state.server = server
    app.state.registry = registry
    task = asyncio.create_task(
        maintenance_loop(server, registry, interval=max(1.0, server.config.heartbeat_interval / 3))
    )
    try:
        yield
    finally:
        task.cancel()


app = FastAPI(title="WebRTC Signaling Service", lifespan=lifespan)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/rooms")
def create_room(req: CreateRoomRequest) -> dict[str, Any]:
    server: SignalingServer = app.state.server
    room_id = req.room_id or uuid.uuid4().hex
    try:
        room = server.rooms.create_room(room_id, capacity=req.capacity)
    except RoomExistsError:
        from fastapi import HTTPException

        raise HTTPException(status_code=409, detail="room already exists")
    return {"room_id": room.room_id, "capacity": room.capacity}


@app.post("/rooms/{room_id}/tokens")
def create_token(room_id: str, req: CreateTokenRequest) -> dict[str, Any]:
    server: SignalingServer = app.state.server
    if server.rooms.get_room(room_id) is None:
        from fastapi import HTTPException

        raise HTTPException(status_code=404, detail="room not found")
    try:
        role = req.role
        from signaling.models import Role

        token_role = Role(role)
    except ValueError:
        from fastapi import HTTPException

        raise HTTPException(status_code=400, detail="invalid role")
    token = server.auth.issue_token(room_id, req.member_id, token_role)
    return {"token": token, "room_id": room_id, "member_id": req.member_id, "role": role}


@app.get("/rooms/{room_id}")
def get_room(room_id: str) -> dict[str, Any]:
    server: SignalingServer = app.state.server
    room = server.rooms.get_room(room_id)
    if room is None:
        from fastapi import HTTPException

        raise HTTPException(status_code=404, detail="room not found")
    return {
        "room_id": room.room_id,
        "capacity": room.capacity,
        "members": [
            {"member_id": m.member_id, "role": m.role.value, "status": m.status.value}
            for m in room.members.values()
        ],
    }


@app.websocket("/ws/{room_id}")
async def websocket_endpoint(websocket: WebSocket, room_id: str) -> None:
    server: SignalingServer = app.state.server
    registry: ConnectionRegistry = app.state.registry
    token = websocket.query_params.get("token", "")
    session_id = uuid.uuid4().hex

    await websocket.accept()
    session, events = server.open_session(token, room_id, session_id)

    if session is None:
        # Authentication / capacity failure: send the error frame and close.
        for event in events:
            if event.message is not None:
                try:
                    await websocket.send_json(event.message.to_wire())
                except Exception:  # noqa: BLE001
                    pass
        await websocket.close(code=4001, reason=events[0].reason if events else "rejected")
        return

    registry.register(session_id, websocket)
    await registry.drain(server)
    for event in events:
        if event.kind == "close" and event.session_id:
            await registry.close_session(event.session_id, event.reason)

    try:
        while True:
            raw = await websocket.receive_json()
            events = server.handle_frame(session_id, raw)
            await registry.drain(server)
            for event in events:
                if event.kind == "close":
                    if event.session_id == session_id:
                        await websocket.close(code=4000, reason=event.reason)
                        return
                    if event.session_id:
                        await registry.close_session(event.session_id, event.reason)
    except WebSocketDisconnect:
        pass
    except Exception:  # noqa: BLE001
        logger.exception("websocket error for session %s", session_id)
    finally:
        registry.unregister(session_id)
        server.close_session(session_id, reason="disconnect")
        await registry.drain(server)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=8000)
