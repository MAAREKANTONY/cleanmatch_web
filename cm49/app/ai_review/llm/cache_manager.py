from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any


class LLMCacheManager:
    def __init__(self, cache_dir: Path):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def _cache_key(self, capability: str, context: dict[str, Any], prompt_version: str, *, provider: str = '', model: str = '', prompt_text: str = '') -> str:
        payload = {
            'capability': capability,
            'prompt_version': prompt_version,
            'provider': provider,
            'model': model,
            'prompt_hash': hashlib.sha256(str(prompt_text or '').encode('utf-8')).hexdigest() if prompt_text else '',
            'website_url': context.get('website_url', ''),
            'menu_urls': context.get('menu_urls', []),
            'photo_urls': context.get('photo_urls', []),
            'outlet_name': context.get('outlet_name', ''),
            'country': context.get('country', ''),
            'website_title': context.get('website_title', ''),
            'website_meta_description': context.get('website_meta_description', ''),
            'main_type': context.get('main_type', ''),
            'all_types': context.get('all_types', ''),
            'web_text': context.get('web_text', ''),
            'menu_excerpt': context.get('menu_excerpt', ''),
        }
        raw = json.dumps(payload, ensure_ascii=False, sort_keys=True)
        return hashlib.sha256(raw.encode('utf-8')).hexdigest()

    def get(self, capability: str, context: dict[str, Any], prompt_version: str, *, provider: str = '', model: str = '', prompt_text: str = '') -> dict[str, Any] | None:
        key = self._cache_key(capability, context, prompt_version, provider=provider, model=model, prompt_text=prompt_text)
        path = self.cache_dir / f'{key}.json'
        if not path.exists():
            return None
        try:
            return json.loads(path.read_text(encoding='utf-8'))
        except Exception:
            return None

    def set(self, capability: str, context: dict[str, Any], prompt_version: str, value: dict[str, Any], *, provider: str = '', model: str = '', prompt_text: str = '') -> None:
        key = self._cache_key(capability, context, prompt_version, provider=provider, model=model, prompt_text=prompt_text)
        path = self.cache_dir / f'{key}.json'
        path.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding='utf-8')
