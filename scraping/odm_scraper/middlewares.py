"""
Scrapy middlewares.

Responsibilities:
- Optionally modify requests and responses
- Handle headers, retries, or custom crawling behavior
- This file may remain unused if default Scrapy behavior is sufficient
"""

from __future__ import annotations

import os


def _build_proxy_url() -> str | None:
    explicit = os.getenv("BRIGHTDATA_PROXY")
    if explicit:
        return explicit

    username = os.getenv("BRIGHTDATA_USERNAME")
    password = os.getenv("BRIGHTDATA_PASSWORD")
    host = os.getenv("BRIGHTDATA_HOST", "brd.superproxy.io")
    port = os.getenv("BRIGHTDATA_PORT", "22225")

    if username and password:
        return f"http://{username}:{password}@{host}:{port}"

    return None


class BrightDataProxyMiddleware:
    def __init__(self, proxy_url: str | None):
        self.proxy_url = proxy_url

    @classmethod
    def from_crawler(cls, crawler):
        proxy_url = _build_proxy_url()
        return cls(proxy_url)

    def process_request(self, request, spider):
        if not self.proxy_url:
            return None
        request.meta["proxy"] = self.proxy_url
        return None
