from __future__ import annotations

from hashlib import sha256
from pathlib import Path


def sha256_bytes(content: bytes) -> str:
    return sha256(content).hexdigest()


def sha256_file(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(1 << 20)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()
