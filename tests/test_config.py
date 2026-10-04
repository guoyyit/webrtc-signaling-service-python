"""Configuration loading and validation tests."""

from __future__ import annotations

import pytest

from signaling.config import SignalingConfig, load_config


def test_defaults():
    cfg = SignalingConfig()
    assert cfg.room_capacity == 8
    assert cfg.negotiation_timeout == 15.0
    assert cfg.max_retries == 3
    assert cfg.heartbeat_interval == 5.0
    assert cfg.heartbeat_timeout == 15.0
    assert cfg.send_queue_size == 64


def test_override_mapping():
    cfg = load_config({"room_capacity": 4, "negotiation_timeout": 5.0, "max_retries": 1})
    assert cfg.room_capacity == 4
    assert cfg.negotiation_timeout == 5.0
    assert cfg.max_retries == 1


def test_invalid_values_rejected():
    with pytest.raises(ValueError):
        load_config({"room_capacity": 0})
    with pytest.raises(ValueError):
        load_config({"negotiation_timeout": -1})
    with pytest.raises(ValueError):
        load_config({"max_retries": -1})
    with pytest.raises(ValueError):
        load_config({"send_queue_size": 0})


def test_unknown_key_rejected():
    with pytest.raises(ValueError):
        load_config({"does_not_exist": 1})


def test_env_override(monkeypatch):
    monkeypatch.setenv("SIGNALING_ROOM_CAPACITY", "32")
    monkeypatch.setenv("SIGNALING_MAX_RETRIES", "5")
    cfg = load_config()
    assert cfg.room_capacity == 32
    assert cfg.max_retries == 5


def test_override_takes_precedence_over_env(monkeypatch):
    monkeypatch.setenv("SIGNALING_ROOM_CAPACITY", "32")
    cfg = load_config({"room_capacity": 16})
    assert cfg.room_capacity == 16
