"""Multi-room real-time signaling & connection coordination service."""

from .config import SignalingConfig, load_config

__all__ = ["SignalingConfig", "load_config"]
