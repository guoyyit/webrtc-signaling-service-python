"""Relay fallback / recovery coordination.

Tracks, per unordered member pair, whether traffic between them is
relayed by the server (direct connection unavailable) or direct.
State transitions are reported by the room layer to all members so
degradation and recovery are observable room-wide.  Switching relay on
or off never tears down member state, so established data channels
survive the transition.
"""

from __future__ import annotations

from .negotiation import pair_key


class RelayManager:
    """Tracks relay state between member pairs of a room."""

    def __init__(self) -> None:
        self._relaying: set[tuple[str, str]] = set()

    def start(self, x: str, y: str) -> bool:
        """Mark the pair as relayed.  Returns True if state changed."""
        key = pair_key(x, y)
        if key in self._relaying:
            return False
        self._relaying.add(key)
        return True

    def stop(self, x: str, y: str) -> bool:
        """Mark the pair as direct again.  Returns True if state changed."""
        key = pair_key(x, y)
        if key not in self._relaying:
            return False
        self._relaying.discard(key)
        return True

    def is_relaying(self, x: str, y: str) -> bool:
        return pair_key(x, y) in self._relaying

    def pairs(self) -> list[tuple[str, str]]:
        return sorted(self._relaying)

    def drop_member(self, client_id: str) -> None:
        for key in [k for k in self._relaying if client_id in k]:
            self._relaying.discard(key)
