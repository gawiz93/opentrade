"""
Shared HTTP client with automatic retries, caching, and rate limiting.
"""

import time
import hashlib
import json
import os
import logging
from pathlib import Path
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

CACHE_DIR = Path(os.getenv("OPENTRADE_CACHE_DIR", "/tmp/opentrade_cache"))


class HttpClient:
    def __init__(
        self,
        rate_limit_rps: float = 1.0,    # requests per second
        cache_ttl_sec: int = 3600 * 6,  # 6 hour cache
        headers: dict | None = None,
    ):
        self.rate_limit_rps = rate_limit_rps
        self.cache_ttl_sec = cache_ttl_sec
        self._last_request = 0.0

        self.session = requests.Session()
        retry = Retry(
            total=5,
            backoff_factor=2,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        self.session.mount("https://", HTTPAdapter(max_retries=retry))
        self.session.mount("http://", HTTPAdapter(max_retries=retry))
        if headers:
            self.session.headers.update(headers)

        CACHE_DIR.mkdir(parents=True, exist_ok=True)

    def get(self, url: str, params: dict | None = None, cache: bool = True, **kwargs) -> requests.Response:
        self._rate_limit()
        cache_key = self._cache_key(url, params)

        if cache and self._cache_valid(cache_key):
            logger.debug(f"Cache hit: {url}")
            return self._load_cache(cache_key)

        logger.debug(f"Fetch: {url} {params or ''}")
        r = self.session.get(url, params=params, timeout=30, **kwargs)
        r.raise_for_status()

        if cache:
            self._save_cache(cache_key, r)
        return r

    def post(self, url: str, json: dict | None = None, **kwargs) -> requests.Response:
        self._rate_limit()
        r = self.session.post(url, json=json, timeout=30, **kwargs)
        r.raise_for_status()
        return r

    def _rate_limit(self):
        if self.rate_limit_rps <= 0:
            return
        min_interval = 1.0 / self.rate_limit_rps
        elapsed = time.time() - self._last_request
        if elapsed < min_interval:
            time.sleep(min_interval - elapsed)
        self._last_request = time.time()

    def _cache_key(self, url: str, params: dict | None) -> str:
        key = url + json.dumps(params or {}, sort_keys=True)
        return hashlib.md5(key.encode()).hexdigest()

    def _cache_path(self, key: str) -> Path:
        return CACHE_DIR / f"{key}.json"

    def _cache_valid(self, key: str) -> bool:
        p = self._cache_path(key)
        if not p.exists():
            return False
        return (time.time() - p.stat().st_mtime) < self.cache_ttl_sec

    def _save_cache(self, key: str, r: requests.Response):
        try:
            self._cache_path(key).write_text(
                json.dumps({"status": r.status_code, "text": r.text})
            )
        except Exception:
            pass

    def _load_cache(self, key: str):
        data = json.loads(self._cache_path(key).read_text())
        mock = requests.Response()
        mock.status_code = data["status"]
        mock._content = data["text"].encode()
        return mock
