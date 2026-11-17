"""Canonical JSON digests used for rule summaries, payload fingerprints and checkpoints."""
import hashlib
import json
from typing import Any


def canonical(obj: Any) -> str:
    """Serialize *obj* deterministically (sorted keys, tight separators, UTF-8)."""
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def sha256_hex(obj: Any) -> str:
    """Hex sha256 of the canonical serialization of *obj*."""
    return hashlib.sha256(canonical(obj).encode("utf-8")).hexdigest()


def tagged(obj: Any) -> str:
    """Digest of *obj* with an explicit algorithm tag, e.g. ``sha256:ab12...``."""
    return "sha256:" + sha256_hex(obj)
