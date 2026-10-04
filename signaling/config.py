"""Configuration for the signaling service.

All limits and timings are centralized here so they can be tuned per
deployment and overridden in tests.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, fields
from typing import Any, Mapping, get_type_hints


@dataclass(frozen=True)
class SignalingConfig:
    """Tunable service parameters.

    Attributes:
        room_capacity: Maximum number of members in a single room.
        token_ttl: Credential time-to-live in seconds.
        negotiation_timeout: Seconds to wait for an answer before failing.
        max_retries: Maximum negotiation attempts per peer pair.
        heartbeat_interval: Expected client ping interval in seconds.
        heartbeat_timeout: Seconds without a heartbeat before disconnecting.
        send_queue_size: Per-session outbound queue bound (slow member guard).
        send_timeout: Seconds a send may block before the member is isolated.
    """

    room_capacity: int = 8
    token_ttl: float = 3600.0
    negotiation_timeout: float = 15.0
    max_retries: int = 3
    heartbeat_interval: float = 5.0
    heartbeat_timeout: float = 15.0
    send_queue_size: int = 64
    send_timeout: float = 2.0

    def __post_init__(self) -> None:
        if self.room_capacity < 1:
            raise ValueError("room_capacity must be >= 1")
        if self.token_ttl <= 0:
            raise ValueError("token_ttl must be > 0")
        if self.negotiation_timeout <= 0:
            raise ValueError("negotiation_timeout must be > 0")
        if self.max_retries < 0:
            raise ValueError("max_retries must be >= 0")
        if self.heartbeat_interval <= 0:
            raise ValueError("heartbeat_interval must be > 0")
        if self.heartbeat_timeout <= 0:
            raise ValueError("heartbeat_timeout must be > 0")
        if self.send_queue_size < 1:
            raise ValueError("send_queue_size must be >= 1")
        if self.send_timeout <= 0:
            raise ValueError("send_timeout must be > 0")


# Environment variable names mirror the dataclass field names.
_ENV_PREFIX = "SIGNALING_"


def load_config(overrides: Mapping[str, Any] | None = None) -> SignalingConfig:
    """Build a config from defaults, environment variables and overrides.

    Precedence (highest first): ``overrides`` mapping, environment
    variables (``SIGNALING_<FIELD_NAME>``), dataclass defaults.
    """
    values: dict[str, Any] = {}
    for f in fields(SignalingConfig):
        env_key = _ENV_PREFIX + f.name.upper()
        if env_key in os.environ:
            values[f.name] = os.environ[env_key]
    if overrides:
        for key, value in overrides.items():
            if key not in {f.name for f in fields(SignalingConfig)}:
                raise ValueError(f"Unknown config key: {key}")
            values[key] = value

    # Coerce types per field (annotations are strings due to
    # `from __future__ import annotations`).
    type_hints = get_type_hints(SignalingConfig)
    coerced: dict[str, Any] = {}
    for f in fields(SignalingConfig):
        if f.name not in values:
            continue
        raw = values[f.name]
        target_type = type_hints[f.name]
        if target_type in (int, float):
            try:
                coerced[f.name] = target_type(raw)
            except (TypeError, ValueError) as exc:
                raise ValueError(f"Invalid value for {f.name}: {raw!r}") from exc
        else:
            coerced[f.name] = raw
    return SignalingConfig(**coerced)
