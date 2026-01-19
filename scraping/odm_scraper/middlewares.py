"""
Scrapy middlewares.

Responsibilities:
- Optionally modify requests and responses
- Handle headers, retries, or custom crawling behavior
- This file may remain unused if default Scrapy behavior is sufficient
"""

from __future__ import annotations

import os
from typing import Optional

import requests
from scrapy.http import HtmlResponse


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


class BrightDataUnlockerAPIMiddleware:
    def __init__(self, token: Optional[str], zone: Optional[str]):
        self.token = token
        self.zone = zone

    @classmethod
    def from_crawler(cls, crawler):
        token = os.getenv("BRIGHTDATA_TOKEN")
        zone = os.getenv("BRIGHTDATA_ZONE")
        return cls(token, zone)

    def process_request(self, request, spider):
        if not self.token or not self.zone:
            return None

        payload = {
            "zone": self.zone,
            "url": request.url,
            "format": "raw",
            "method": request.method,
        }

        try:
            resp = requests.post(
                "https://api.brightdata.com/request",
                json=payload,
                headers={
                    "Authorization": f"Bearer {self.token}",
                    "Content-Type": "application/json",
                },
                timeout=60,
            )
        except Exception as exc:
            spider.logger.warning("brightdata unlocker error url=%s err=%s", request.url, exc)
            return None

        return HtmlResponse(
            url=request.url,
            status=resp.status_code,
            body=resp.content,
            encoding=resp.encoding or "utf-8",
            request=request,
        )
