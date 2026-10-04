"""Transport-independent connection abstraction."""

from __future__ import annotations

from typing import Any, Protocol


class Connection(Protocol):
    """A client connection able to receive outbound messages."""

    async def send_json(self, data: dict[str, Any]) -> None: ...

    async def close(self, code: int = 1000, reason: str = "") -> None: ...
