from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any


class FileWebCache:
    def __init__(self, root: Path):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)

    def _path_for_url(self, url: str) -> Path:
        digest = hashlib.sha256(url.strip().encode('utf-8', errors='ignore')).hexdigest()
        return self.root / f'{digest}.json'

    def get(self, url: str) -> dict[str, Any] | None:
        if not url:
            return None
        path = self._path_for_url(url)
        if not path.exists():
            return None
        try:
            return json.loads(path.read_text(encoding='utf-8'))
        except Exception:
            return None

    def set(self, url: str, payload: dict[str, Any]) -> None:
        if not url:
            return
        path = self._path_for_url(url)
        try:
            path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
        except Exception:
            return
