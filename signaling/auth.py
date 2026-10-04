"""Credential issuance and per-room access control.

Tokens are signed with an HMAC and carry room/member/role claims plus an
expiry. Verification is constant-time and never raises for bad input; it
returns None so callers can reject the join deterministically.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import time
from typing import Optional

from .models import Role, TokenClaims


class AuthManager:
    """Issues and verifies signed room credentials."""

    def __init__(self, secret: str, token_ttl: float) -> None:
        self._secret = secret.encode("utf-8")
        self._token_ttl = token_ttl

    def issue_token(
        self,
        room_id: str,
        member_id: str,
        role: Role,
        now: Optional[float] = None,
    ) -> str:
        """Issue a signed JWT-like token for a room member."""
        now = time.time() if now is None else now
        header = {"alg": "HS256", "typ": "JWT"}
        payload = {
            "room_id": room_id,
            "member_id": member_id,
            "role": role.value,
            "iat": now,
            "exp": now + self._token_ttl,
        }
        header_b = self._b64url_encode(json.dumps(header, separators=(",", ":")).encode("utf-8"))
        payload_b = self._b64url_encode(json.dumps(payload, separators=(",", ":")).encode("utf-8"))
        signing_input = f"{header_b}.{payload_b}".encode("utf-8")
        signature = self._sign(signing_input)
        return f"{header_b}.{payload_b}.{signature}"

    def verify_token(
        self,
        token: str,
        room_id: str,
        now: Optional[float] = None,
    ) -> Optional[TokenClaims]:
        """Verify a token for the given room.

        Returns the claims when valid and unexpired, otherwise None.
        """
        now = time.time() if now is None else now
        try:
            header_b, payload_b, signature = token.split(".")
        except (ValueError, AttributeError):
            return None

        signing_input = f"{header_b}.{payload_b}".encode("utf-8")
        expected = self._sign(signing_input)
        if not hmac.compare_digest(signature, expected):
            return None

        try:
            payload = json.loads(self._b64url_decode(payload_b))
            header = json.loads(self._b64url_decode(header_b))
        except (ValueError, json.JSONDecodeError):
            return None

        if header.get("alg") != "HS256" or header.get("typ") != "JWT":
            return None
        if payload.get("room_id") != room_id:
            return None

        role_raw = payload.get("role")
        try:
            role = Role(role_raw)
        except ValueError:
            return None

        try:
            issued_at = float(payload["iat"])
            expires_at = float(payload["exp"])
        except (KeyError, TypeError, ValueError):
            return None

        if now >= expires_at:
            return None
        if expires_at <= issued_at:
            return None

        return TokenClaims(
            room_id=room_id,
            member_id=str(payload["member_id"]),
            role=role,
            issued_at=issued_at,
            expires_at=expires_at,
        )

    @staticmethod
    def _b64url_encode(data: bytes) -> str:
        return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")

    @staticmethod
    def _b64url_decode(data: str) -> bytes:
        padding = "=" * (-len(data) % 4)
        return base64.urlsafe_b64decode(data + padding)

    def _sign(self, payload: bytes) -> str:
        digest = hmac.new(self._secret, payload, hashlib.sha256).digest()
        return self._b64url_encode(digest)
