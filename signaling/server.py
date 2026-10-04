"""FastAPI WebSocket transport for the signaling service."""

from __future__ import annotations

import asyncio
import contextlib
import json
import time
from collections.abc import AsyncIterator

from fastapi import FastAPI, WebSocket, WebSocketDisconnect

from .config import SignalingConfig
from .connection import Connection
from .service import ServiceError, SignalingService


class WebSocketConnection:
    """Adapts a FastAPI WebSocket to the Connection protocol."""

    def __init__(self, ws: WebSocket) -> None:
        self._ws = ws

    async def send_json(self, data: dict) -> None:
        await self._ws.send_text(json.dumps(data))

    async def close(self, code: int = 1000, reason: str = "") -> None:
        with contextlib.suppress(Exception):
            await self._ws.close(code=code, reason=reason)


def create_app(config: SignalingConfig | None = None) -> FastAPI:
    config = config or SignalingConfig()
    service = SignalingService(config)

    @contextlib.asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        async def sweep_loop() -> None:
            while True:
                await asyncio.sleep(config.heartbeat_interval_s)
                await service.broadcast_pings()
                await service.sweep()

        sweeper = asyncio.create_task(sweep_loop())
        try:
            yield
        finally:
            sweeper.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await sweeper

    app = FastAPI(title="webrtc-signaling-service", lifespan=lifespan)
    app.state.service = service

    @app.get("/healthz")
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.websocket("/ws/{room}")
    async def ws_endpoint(ws: WebSocket, room: str) -> None:
        await ws.accept()
        token = ws.query_params.get("token", "")
        conn: Connection = WebSocketConnection(ws)
        try:
            ack = await service.connect(room, token, conn)
        except ServiceError as exc:
            await conn.send_json({"type": "error", "code": exc.code.value,
                                  "detail": exc.detail})
            await conn.close(code=4401, reason=exc.detail)
            return

        client_id: str = ack["client_id"]

        async def pump() -> None:
            """Drain the member outbox to the socket; never blocks the room."""
            member = service.room(room).get(client_id)  # type: ignore[union-attr]
            while member is not None:
                data = await member.outbox.get()
                await conn.send_json(data)
                member = service.room(room).get(client_id)  # type: ignore[union-attr]

        pump_task = asyncio.create_task(pump())
        try:
            while True:
                raw = await ws.receive_text()
                try:
                    message = json.loads(raw)
                except json.JSONDecodeError:
                    await conn.send_json({"type": "error", "code": "bad_json",
                                          "detail": "message must be JSON"})
                    continue
                if isinstance(message, dict):
                    await service.handle(room, client_id, message, now=time.time())
        except WebSocketDisconnect:
            pass
        finally:
            pump_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await pump_task
            await service.disconnect(room, client_id, connection=conn)

    return app
