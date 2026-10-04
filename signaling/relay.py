"""Relay fallback coordination.

When a peer pair cannot establish a direct connection it reports a relay
fallback; the server records the pair's link mode and notifies the room so
members see the degradation. Recovery back to direct is reported the same
way. Already established data channels of other pairs are never touched:
the coordinator only records mode and emits change notifications.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from .models import LinkMode


@dataclass
class LinkState:
    """Link mode for an unordered peer pair."""

    peer_a: str
    peer_b: str
    mode: LinkMode
    updated_at: float


def _pair_key(room_id: str, a: str, b: str) -> tuple[str, str, str]:
    return (room_id, min(a, b), max(a, b))


class RelayCoordinator:
    """Tracks direct/relay mode per peer pair."""

    def __init__(self) -> None:
        # key: (room_id, smaller_id, larger_id) -> LinkState
        self._links: dict[tuple[str, str, str], LinkState] = {}

    def report(
        self,
        room_id: str,
        sender_id: str,
        target_id: str,
        mode: LinkMode,
        now: float,
    ) -> Optional[LinkState]:
        """Record a link mode report.

        Returns the new LinkState when the mode changed, otherwise None
        (duplicate or repeated reports are ignored).
        """
        if sender_id == target_id:
            return None
        key = _pair_key(room_id, sender_id, target_id)
        existing = self._links.get(key)
        if existing is not None and existing.mode == mode:
            return None
        peer_a, peer_b = key[1], key[2]
        state = LinkState(peer_a=peer_a, peer_b=peer_b, mode=mode, updated_at=now)
        self._links[key] = state
        return state

    def get(self, room_id: str, peer_a: str, peer_b: str) -> Optional[LinkMode]:
        state = self._links.get(_pair_key(room_id, peer_a, peer_b))
        return None if state is None else state.mode

    def remove_peer(self, room_id: str, member_id: str) -> list[LinkState]:
        """Drop all pairs involving a leaving member; returns removed states."""
        removed: list[LinkState] = []
        for key in list(self._links.keys()):
            if key[0] != room_id:
                continue
            if member_id in (key[1], key[2]):
                removed.append(self._links.pop(key))
        return removed
