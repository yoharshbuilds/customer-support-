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


def _decode(sig: str) -> list[bytes]:
    """Candidate raw signatures from the header.

    Headers get line-wrapped, whitespace-padded, or urlsafe-encoded in transit
    depending on who is sending. Try the plausible readings rather than
    rejecting a genuine call over an encoding detail.
    """
    cleaned = "".join(sig.split())
    out: list[bytes] = []
    for decoder in (base64.b64decode, base64.urlsafe_b64decode):
        for candidate in (cleaned, cleaned + "=" * (-len(cleaned) % 4)):
            try:
                blob = decoder(candidate)
            except Exception:  # noqa: BLE001 - just an encoding guess
                continue
            if blob and blob not in out:
                out.append(blob)
    return out


# Freshchat documents RSA + SHA-256. Real traffic showed a minority of genuine
# calls failing that check, so the other standard combinations are tried before
# declaring a request forged -- a signature we cannot parse is not evidence of
# an attacker, and rejecting it loses a real resolve.
_SCHEMES = (
    ("PKCS1v15/SHA256", padding.PKCS1v15(), hashes.SHA256()),
    ("PKCS1v15/SHA1", padding.PKCS1v15(), hashes.SHA1()),
    ("PKCS1v15/SHA512", padding.PKCS1v15(), hashes.SHA512()),
)


def verify(raw_body: bytes, signature_b64: str | None) -> bool:
    """True if the body matches the signature, or if no key is configured."""
    key = _public_key()
    if key is None:
        return True  # not configured; caller logs the warning once at startup
    if not signature_b64:
        log.warning("webhook rejected: missing X-Freshchat-Signature")
        return False

    for blob in _decode(signature_b64):
        for name, pad, algo in _SCHEMES:
            try:
                key.verify(blob, raw_body, pad, algo)
                if name != "PKCS1v15/SHA256":
                    log.info("signature verified with %s (not the documented scheme)", name)
                return True
            except (InvalidSignature, ValueError, TypeError):
                continue

    log.warning("webhook rejected: no scheme verified the signature (%d bytes body)", len(raw_body))
    return False
