"""Verify Freshchat's X-Freshchat-Signature header.

Freshchat signs the raw request body with RSA and SHA-256 and sends the
signature base64-encoded. You verify it with the public key shown on the
Freshchat webhook configuration page -- there is no shared secret.

Verification is skipped (with a warning) when no public key is configured, so
the service is usable before the key is pasted in, but a configured key is
always enforced.
"""
from __future__ import annotations

import base64
import logging

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

from . import config

log = logging.getLogger(__name__)

_key = None


def _public_key():
    global _key
    if _key is None:
        pem = config.FRESHCHAT_PUBLIC_KEY.strip()
        if not pem:
            return None
        # Allow the key to be supplied as a single line with literal \n, which
        # is how most dashboards let you paste a PEM into an env var.
        pem = pem.replace("\\n", "\n")
        if "BEGIN" not in pem:
            pem = f"-----BEGIN PUBLIC KEY-----\n{pem}\n-----END PUBLIC KEY-----"
        _key = serialization.load_pem_public_key(pem.encode())
    return _key


def verify(raw_body: bytes, signature_b64: str | None) -> bool:
    """True if the body matches the signature, or if no key is configured."""
    key = _public_key()
    if key is None:
        return True  # not configured; caller logs the warning once at startup
    if not signature_b64:
        log.warning("webhook rejected: missing X-Freshchat-Signature")
        return False
    try:
        key.verify(
            base64.b64decode(signature_b64),
            raw_body,
            padding.PKCS1v15(),
            hashes.SHA256(),
        )
        return True
    except (InvalidSignature, ValueError, TypeError) as e:
        log.warning("webhook rejected: bad signature (%s)", type(e).__name__)
        return False
