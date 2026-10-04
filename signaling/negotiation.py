"""Peer-pair negotiation state machine with deterministic glare resolution.

When both peers in a pair offer at the same time (glare), the server applies
a fixed rule: the member with the lexicographically smaller id is the
"impolite" peer and wins; the larger id is "polite" and must abandon its
offer. The abandoned side receives an explicit ``negotiation_abandoned``
frame so it can end cleanly and re-initiate -- no mutual deadlock.

The state machine is idempotent and monotonic: every frame carries a
sequence number, stale or duplicate frames are dropped without disturbing
the current phase.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

from .models import NegotiationPhase


@dataclass
class NegotiationState:
    """Negotiation state for an unordered peer pair."""

    peer_a: str  # deterministic: smaller member id
    peer_b: str  # larger member id
    phase: NegotiationPhase = NegotiationPhase.IDLE
    offer_from: str = ""
    offer_seq: int = 0
    answer_seq: int = 0
    last_ice_seq: int = 0
    retries: int = 0
    started_at: float = 0.0
    updated_at: float = 0.0

    @property
    def polite_id(self) -> str:
        """The peer that yields on glare (the larger id)."""
        return self.peer_b

    @property
    def impolite_id(self) -> str:
        """The peer that wins glare (the smaller id)."""
        return self.peer_a

    def has_pending_offer_from(self, member_id: str) -> bool:
        return self.phase == NegotiationPhase.OFFER_PENDING and self.offer_from == member_id


@dataclass
class NegotiationDecision:
    """Outcome of processing one negotiation frame."""

    forward: bool = True
    abandon_target: Optional[str] = None  # peer that must end its negotiation
    new_phase: Optional[NegotiationPhase] = None
    failed: bool = False
    reason: str = ""


def _pair_key(room_id: str, a: str, b: str) -> tuple[str, str, str]:
    return (room_id, min(a, b), max(a, b))


class NegotiationTracker:
    """Tracks negotiation state for every peer pair in every room."""

    def __init__(
        self,
        negotiation_timeout: float,
        max_retries: int,
    ) -> None:
        self._timeout = negotiation_timeout
        self._max_retries = max_retries
        self._states: dict[tuple[str, str, str], NegotiationState] = {}

    # -- state access -----------------------------------------------------

    def get(self, room_id: str, peer_a: str, peer_b: str) -> Optional[NegotiationState]:
        return self._states.get(_pair_key(room_id, peer_a, peer_b))

    def _pair(self, room_id: str, a: str, b: str, now: float) -> NegotiationState:
        key = _pair_key(room_id, a, b)
        state = self._states.get(key)
        if state is None:
            state = NegotiationState(
                peer_a=key[1], peer_b=key[2], started_at=now, updated_at=now
            )
            self._states[key] = state
        return state

    # -- frame processing --------------------------------------------------

    def on_offer(
        self,
        room_id: str,
        sender_id: str,
        target_id: str,
        seq: int,
        now: float,
    ) -> NegotiationDecision:
        state = self._pair(room_id, sender_id, target_id, now)

        # Duplicate offer from the same side: drop without state change.
        if (
            state.phase == NegotiationPhase.OFFER_PENDING
            and state.offer_from == sender_id
            and seq <= state.offer_seq
        ):
            return NegotiationDecision(forward=False, reason="duplicate_offer")

        # Glare: both sides have an offer pending. Deterministic winner is
        # the impolite (smaller id) peer; the polite side must abandon.
        if state.phase == NegotiationPhase.OFFER_PENDING and state.offer_from != sender_id:
            if sender_id == state.impolite_id:
                # Impolite wins: keep this offer, abandon the polite side.
                state.offer_from = sender_id
                state.offer_seq = seq
                state.updated_at = now
                return NegotiationDecision(
                    forward=True,
                    abandon_target=state.polite_id,
                    new_phase=NegotiationPhase.OFFER_PENDING,
                    reason="glare_impolite_wins",
                )
            # Polite side loses: drop its offer and tell it to end.
            return NegotiationDecision(
                forward=False,
                abandon_target=sender_id,
                new_phase=NegotiationPhase.OFFER_PENDING,
                reason="glare_polite_yields",
            )

        # Fresh / re-negotiation offer.
        state.phase = NegotiationPhase.OFFER_PENDING
        state.offer_from = sender_id
        state.offer_seq = seq
        state.answer_seq = 0
        state.updated_at = now
        return NegotiationDecision(
            forward=True,
            new_phase=NegotiationPhase.OFFER_PENDING,
            reason="offer",
        )

    def on_answer(
        self,
        room_id: str,
        sender_id: str,
        target_id: str,
        seq: int,
        now: float,
    ) -> NegotiationDecision:
        state = self._pair(room_id, sender_id, target_id, now)

        # Answer is only valid against a pending offer from the peer.
        if state.phase != NegotiationPhase.OFFER_PENDING:
            return NegotiationDecision(forward=False, reason="answer_without_offer")
        if state.offer_from != target_id:
            return NegotiationDecision(forward=False, reason="answer_wrong_target")
        if seq <= state.answer_seq:
            return NegotiationDecision(forward=False, reason="duplicate_answer")

        state.phase = NegotiationPhase.CONNECTED
        state.answer_seq = seq
        state.updated_at = now
        return NegotiationDecision(
            forward=True,
            new_phase=NegotiationPhase.CONNECTED,
            reason="answer",
        )

    def on_ice_candidate(
        self,
        room_id: str,
        sender_id: str,
        target_id: str,
        seq: int,
        now: float,
    ) -> NegotiationDecision:
        state = self._pair(room_id, sender_id, target_id, now)
        if state.phase not in (NegotiationPhase.OFFER_PENDING, NegotiationPhase.CONNECTED):
            return NegotiationDecision(forward=False, reason="ice_without_negotiation")
        # Duplicate candidates are harmless but must not be re-processed.
        if seq and seq <= state.last_ice_seq:
            return NegotiationDecision(forward=False, reason="duplicate_ice")
        state.last_ice_seq = seq
        state.updated_at = now
        return NegotiationDecision(forward=True, reason="ice")

    def on_renegotiate(
        self,
        room_id: str,
        sender_id: str,
        target_id: str,
        now: float,
    ) -> NegotiationDecision:
        state = self._pair(room_id, sender_id, target_id, now)
        state.phase = NegotiationPhase.IDLE
        state.offer_from = ""
        state.offer_seq = 0
        state.answer_seq = 0
        state.updated_at = now
        return NegotiationDecision(
            forward=True,
            new_phase=NegotiationPhase.IDLE,
            reason="renegotiate",
        )

    def tick(self, now: float) -> list[tuple[str, str, str]]:
        """Fail negotiations that exceeded the timeout.

        Returns a list of (room_id, peer_a, peer_b) pairs that failed.
        """
        failed: list[tuple[str, str, str]] = []
        for key, state in list(self._states.items()):
            if state.phase != NegotiationPhase.OFFER_PENDING:
                continue
            if (now - state.updated_at) <= self._timeout:
                continue
            state.retries += 1
            state.updated_at = now
            if state.retries > self._max_retries:
                state.phase = NegotiationPhase.FAILED
            else:
                state.phase = NegotiationPhase.IDLE
            failed.append(key)
        return failed
