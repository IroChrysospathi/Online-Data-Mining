"""
Scrapy middlewares.

Responsibilities:
- Optionally modify requests and responses
- Handle headers, retries, or custom crawling behavior

Bright Data integration:
- Proxy middleware: sets request.meta["proxy"] using BRIGHTDATA_* env vars
- Unlocker API middleware: fetches page via Bright Data Web Unlocker API

IMPORTANT FIX:
- Web Unlocker expects payload like: {"zone": "...", "url": "...", "format": "raw"}
- Do NOT send "method" (can cause 400 Bad Request)
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
        # If not configured, let Scrapy download normally (or proxy middleware handle it)
        if not self.token or not self.zone:
            return None

        # Match Bright Data's working example payload
        payload = {
            "zone": self.zone,
            "url": request.url,
            "format": "raw",  # recommended for Scrapy; returns the HTML bytes
            # DO NOT include "method" here (can trigger 400 on Web Unlocker)
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

        # If Bright Data returns an error, log the message body (super helpful for debugging)
        if resp.status_code >= 400:
            spider.logger.error(
                "BrightData API error status=%s url=%s body=%s",
                resp.status_code,
                request.url,
                (resp.text or "")[:800],
            )

        return HtmlResponse(
            url=request.url,
            status=resp.status_code,
            body=resp.content,
            encoding=resp.encoding or "utf-8",
            request=request,
        )
