"""Negotiation state machine with deterministic glare resolution.

Glare rule (deterministic, both sides compute the same result):
for a pair of peers, the lexicographically *larger* client id is the
"polite" peer.  When offers collide, the polite peer's offer wins; the
impolite peer rolls its own offer back and may re-offer afterwards.

The tracker is transport-independent: it consumes events and returns
verdicts; the room/service layer turns verdicts into forwarded messages.
"""

from __future__ import annotations

import enum
from dataclasses import dataclass, field
from typing import Any


class OfferVerdict(enum.Enum):
    FORWARD = "forward"                # forward the offer to the peer
    IGNORE_STALE = "ignore_stale"      # duplicate or out-of-order offer
    ROLLBACK_SELF = "rollback_self"    # impolite side collided: drop own offer
    FORWARD_AND_ROLLBACK_PEER = "forward_and_rollback_peer"  # polite side wins


class AnswerVerdict(enum.Enum):
    FORWARD = "forward"
    IGNORE_STALE = "ignore_stale"      # no matching pending offer / duplicate


@dataclass
class PairNegotiation:
    """Negotiation state for one unordered pair of peers."""

    a: str
    b: str
    polite_id: str
    pending_offer_from: str | None = None
    pending_offer_seq: int = 0
    pending_offer_at: float = 0.0
    pending_offer_payload: dict[str, Any] = field(default_factory=dict)
    retries: int = 0
    last_offer_seq: dict[str, int] = field(default_factory=dict)
    last_answer_seq: dict[str, int] = field(default_factory=dict)

    def peer_of(self, client_id: str) -> str:
        return self.b if client_id == self.a else self.a


def pair_key(x: str, y: str) -> tuple[str, str]:
    return (x, y) if x < y else (y, x)


class NegotiationTracker:
    """Tracks per-pair negotiation state and resolves glare deterministically."""

    def __init__(self, timeout_s: float, max_retries: int) -> None:
        self._timeout_s = timeout_s
        self._max_retries = max_retries
        self._pairs: dict[tuple[str, str], PairNegotiation] = {}

    def _pair(self, x: str, y: str) -> PairNegotiation:
        key = pair_key(x, y)
        pair = self._pairs.get(key)
        if pair is None:
            pair = PairNegotiation(a=key[0], b=key[1], polite_id=key[1])
            self._pairs[key] = pair
        return pair

    def polite_peer(self, x: str, y: str) -> str:
        return max(x, y)

    def on_offer(
        self, sender: str, peer: str, seq: int, now: float,
        payload: dict[str, Any] | None = None,
    ) -> OfferVerdict:
        pair = self._pair(sender, peer)
        if seq <= pair.last_offer_seq.get(sender, 0):
            return OfferVerdict.IGNORE_STALE
        pair.last_offer_seq[sender] = seq
        if pair.pending_offer_from is None:
            pair.pending_offer_from = sender
            pair.pending_offer_seq = seq
            pair.pending_offer_at = now
            pair.pending_offer_payload = dict(payload or {})
            pair.retries = 0
            return OfferVerdict.FORWARD
        if pair.pending_offer_from == sender:
            # re-offer while own offer pending (retry): refresh, still forward
            pair.pending_offer_seq = seq
            pair.pending_offer_at = now
            pair.pending_offer_payload = dict(payload or {})
            return OfferVerdict.FORWARD
        # glare: both sides have offered
        if sender == pair.polite_id:
            pair.pending_offer_from = sender
            pair.pending_offer_seq = seq
            pair.pending_offer_at = now
            pair.pending_offer_payload = dict(payload or {})
            pair.retries = 0
            return OfferVerdict.FORWARD_AND_ROLLBACK_PEER
        return OfferVerdict.ROLLBACK_SELF

    def on_rollback(self, sender: str, peer: str) -> None:
        """The sender abandoned its own offer; clear it if it was pending."""
        pair = self._pair(sender, peer)
        if pair.pending_offer_from == sender:
            pair.pending_offer_from = None
            pair.pending_offer_seq = 0
            pair.pending_offer_payload = {}
            pair.retries = 0

    def on_answer(self, sender: str, peer: str, seq: int) -> AnswerVerdict:
        pair = self._pair(sender, peer)
        if pair.pending_offer_from != peer:
            return AnswerVerdict.IGNORE_STALE
        if seq <= pair.last_answer_seq.get(sender, 0):
            return AnswerVerdict.IGNORE_STALE
        pair.last_answer_seq[sender] = seq
        pair.pending_offer_from = None
        pair.pending_offer_seq = 0
        pair.pending_offer_payload = {}
        pair.retries = 0
        return AnswerVerdict.FORWARD

    def check_timeouts(
        self, now: float
    ) -> tuple[list[tuple[str, str, int, dict[str, Any]]], list[tuple[str, str]]]:
        """Advance timeout state.

        Returns ``(retries, failures)``: ``retries`` are pending offers
        that timed out and should be re-forwarded once more,
        ``failures`` are pairs whose retries are exhausted; their
        pending state is cleared so both sides can start over.
        """
        retries: list[tuple[str, str, int, dict[str, Any]]] = []
        failures: list[tuple[str, str]] = []
        for pair in self._pairs.values():
            if pair.pending_offer_from is None:
                continue
            if now - pair.pending_offer_at < self._timeout_s:
                continue
            sender = pair.pending_offer_from
            if pair.retries >= self._max_retries:
                peer = pair.peer_of(sender)
                pair.pending_offer_from = None
                pair.pending_offer_seq = 0
                pair.pending_offer_payload = {}
                pair.retries = 0
                failures.append((sender, peer))
                continue
            pair.retries += 1
            pair.pending_offer_at = now
            retries.append((sender, pair.peer_of(sender), pair.pending_offer_seq,
                            dict(pair.pending_offer_payload)))
        return retries, failures

    def drop_member(self, client_id: str) -> None:
        """Forget all negotiation state involving a departed member."""
        for key in [k for k in self._pairs if client_id in k]:
            del self._pairs[key]
