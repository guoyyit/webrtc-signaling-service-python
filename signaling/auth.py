"""Token issuing and validation for room access.

Tokens are HMAC-SHA256 signed JSON blobs: ``b64url(payload).b64url(sig)``.
The payload carries room, client id, role and an absolute expiry timestamp,
so validation is deterministic for a given ``now``.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
from dataclasses import dataclass

from .models import Role


@dataclass(frozen=True)
class TokenClaims:
    """Decoded and validated token claims."""

    room: str
    client_id: str
    role: Role
    expires_at: float


class AuthError(Exception):
    """Raised when a token is invalid or expired."""


def _b64e(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def _b64d(text: str) -> bytes:
    pad = "=" * (-len(text) % 4)
    return base64.urlsafe_b64decode(text + pad)


class TokenIssuer:
    """Issues and validates HMAC-signed room access tokens."""

    def __init__(self, secret: str, ttl_s: float) -> None:
        self._secret = secret.encode("utf-8")
        self._ttl_s = ttl_s

    def _sign(self, payload_b64: str) -> str:
        sig = hmac.new(self._secret, payload_b64.encode("ascii"), hashlib.sha256)
        return _b64e(sig.digest())

    def issue(self, room: str, client_id: str, role: Role, now: float) -> str:
        payload = {
            "room": room,
            "client_id": client_id,
            "role": role.value,
            "exp": now + self._ttl_s,
        }
        payload_b64 = _b64e(json.dumps(payload, separators=(",", ":")).encode())
        return f"{payload_b64}.{self._sign(payload_b64)}"

    def validate(self, token: str, room: str, now: float) -> TokenClaims:
        try:
            payload_b64, sig = token.split(".", 1)
        except ValueError as exc:
            raise AuthError("malformed token") from exc
        if not hmac.compare_digest(sig, self._sign(payload_b64)):
            raise AuthError("bad signature")
        try:
            payload = json.loads(_b64d(payload_b64))
            claims = TokenClaims(
                room=str(payload["room"]),
                client_id=str(payload["client_id"]),
                role=Role(payload["role"]),
                expires_at=float(payload["exp"]),
            )
        except (ValueError, KeyError, TypeError) as exc:
            raise AuthError("malformed payload") from exc
        if claims.room != room:
            raise AuthError("token issued for a different room")
        if now >= claims.expires_at:
            raise AuthError("token expired")
        return claims
