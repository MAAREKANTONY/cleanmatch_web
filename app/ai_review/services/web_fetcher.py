from __future__ import annotations

import html
import re
from dataclasses import dataclass
from typing import Iterable
from urllib.parse import urlparse
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

SCRIPT_STYLE_RE = re.compile(r'<(script|style)[^>]*>.*?</\\1>', re.IGNORECASE | re.DOTALL)
TAG_RE = re.compile(r'<[^>]+>')
TITLE_RE = re.compile(r'<title[^>]*>(.*?)</title>', re.IGNORECASE | re.DOTALL)
META_DESC_RE = re.compile(r'<meta[^>]+(?:name=["\']description["\']|property=["\']og:description["\'])[^>]+content=["\'](.*?)["\']', re.IGNORECASE | re.DOTALL)
WHITESPACE_RE = re.compile(r'\s+')


@dataclass
class FetchResult:
    url: str
    ok: bool
    status: str
    source_type: str
    title: str = ''
    meta_description: str = ''
    text_excerpt: str = ''
    from_cache: bool = False


def normalize_url(url: str) -> str:
    value = (url or '').strip()
    if not value:
        return ''
    if value.startswith('//'):
        return 'https:' + value
    if '://' not in value:
        return 'https://' + value
    return value


def host_label(url: str) -> str:
    parsed = urlparse(normalize_url(url))
    return parsed.netloc or parsed.path


def strip_html_to_text(raw_html: str, max_chars: int = 4000) -> str:
    cleaned = SCRIPT_STYLE_RE.sub(' ', raw_html or '')
    cleaned = TAG_RE.sub(' ', cleaned)
    cleaned = html.unescape(cleaned)
    cleaned = WHITESPACE_RE.sub(' ', cleaned).strip()
    return cleaned[:max_chars]


def extract_title(raw_html: str) -> str:
    match = TITLE_RE.search(raw_html or '')
    if not match:
        return ''
    return WHITESPACE_RE.sub(' ', html.unescape(match.group(1))).strip()[:300]


def extract_meta_description(raw_html: str) -> str:
    match = META_DESC_RE.search(raw_html or '')
    if not match:
        return ''
    return WHITESPACE_RE.sub(' ', html.unescape(match.group(1))).strip()[:600]


def fetch_html(url: str, *, timeout_seconds: int = 5, user_agent: str = 'cleanmatch-web', max_bytes: int = 300000) -> str:
    req = Request(normalize_url(url), headers={'User-Agent': user_agent, 'Accept-Language': 'en,fr;q=0.8,es;q=0.7,it;q=0.6,de;q=0.6'})
    try:
        with urlopen(req, timeout=timeout_seconds) as resp:
            raw = resp.read(max_bytes + 1)
            if len(raw) > max_bytes:
                raw = raw[:max_bytes]
            encoding = resp.headers.get_content_charset() or 'utf-8'
        return raw.decode(encoding, errors='ignore')
    except (HTTPError, URLError, OSError, ValueError):
        return ''


def candidate_urls(website_root_url: str = '', website: str = '', menu_url: str = '') -> Iterable[tuple[str, str]]:
    seen = set()
    for source_type, url in [('website_root', website_root_url), ('website', website), ('menu', menu_url)]:
        normalized = normalize_url(url)
        if normalized and normalized not in seen:
            seen.add(normalized)
            yield source_type, normalized
