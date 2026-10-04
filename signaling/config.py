"""Configuration for the signaling service."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SignalingConfig:
    """Tunable parameters of the signaling service.

    All durations are in seconds.
    """

    max_room_capacity: int = 16
    negotiation_timeout_s: float = 10.0
    max_negotiation_retries: int = 3
    heartbeat_interval_s: float = 5.0
    heartbeat_timeout_s: float = 15.0
    max_pending_messages: int = 256
    token_ttl_s: float = 300.0
    token_secret: str = "dev-secret"
