"""
Scrapy middlewares.

Bright Data integration:
- Proxy middleware: sets request.meta["proxy"] using BRIGHTDATA_* env vars
- Unlocker API middleware: fetches page via Bright Data Web Unlocker API

Notes:
- Web Unlocker expects payload like: {"zone": "...", "url": "...", "format": "raw"}
- Do NOT send "method" (can cause 400 Bad Request)
"""

from __future__ import annotations

import os
from typing import Optional, Dict, Any

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
        return cls(_build_proxy_url())

    def process_request(self, request, spider):
        if self.proxy_url:
            request.meta.setdefault("proxy", self.proxy_url)
        return None


class BrightDataUnlockerAPIMiddleware:
    """
    Fetches the page via Bright Data Web Unlocker API and returns a Scrapy HtmlResponse.

    Improvements vs your version:
      - forwards request headers (User-Agent, Accept-Language, etc.)
      - forces Accept-Encoding: identity (avoid weird encodings / variants)
      - retries once on empty/blocked-ish html
      - returns None on hard API errors so Scrapy retry/download can take over (if configured)
    """

    API_URL = "https://api.brightdata.com/request"

    def __init__(self, token: Optional[str], zone: Optional[str], timeout: int = 60):
        self.token = token
        self.zone = zone
        self.timeout = timeout
        self.session = requests.Session()

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            os.getenv("BRIGHTDATA_TOKEN"),
            os.getenv("BRIGHTDATA_ZONE"),
            int(os.getenv("BRIGHTDATA_TIMEOUT", "60")),
        )

    def _request_headers_to_dict(self, scrapy_headers) -> Dict[str, str]:
        out: Dict[str, str] = {}
        try:
            for k, v in scrapy_headers.items():
                # scrapy headers are bytes
                kk = k.decode("utf-8", errors="ignore") if isinstance(k, (bytes, bytearray)) else str(k)
                if isinstance(v, (list, tuple)):
                    vv = b",".join(v)
                else:
                    vv = v
                vv = vv.decode("utf-8", errors="ignore") if isinstance(vv, (bytes, bytearray)) else str(vv)
                out[kk] = vv
        except Exception:
            pass
        return out

    def _looks_bad_html(self, body: bytes | None) -> bool:
        if not body:
            return True
        # very short html is often a block / placeholder
        if len(body) < 800:
            return True
        low = body[:5000].lower()
        needles = [
            b"access denied",
            b"captcha",
            b"cloudflare",
            b"datadome",
            b"unusual traffic",
            b"verify you are human",
            b"attention required",
            b"request blocked",
        ]
        return any(n in low for n in needles)

    def process_request(self, request, spider):
        # Only activate if token+zone exist
        if not self.token or not self.zone:
            return None

        # Allow opting out per-request if you ever need it
        if request.meta.get("skip_brightdata_unlocker"):
            return None

        # Forward request headers; force identity encoding
        fwd_headers = self._request_headers_to_dict(request.headers)
        fwd_headers["Accept-Encoding"] = "identity"

        payload: Dict[str, Any] = {
            "zone": self.zone,
            "url": request.url,
            "format": "raw",
            "headers": fwd_headers,
        }

        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        }

        def do_call() -> requests.Response | None:
            try:
                return self.session.post(
                    self.API_URL,
                    json=payload,
                    headers=headers,
                    timeout=self.timeout,
                )
            except Exception as exc:
                spider.logger.warning(
                    "BrightData unlocker error url=%s err=%s", request.url, exc
                )
                return None

        resp = do_call()
        if resp is None:
            return None

        # If BrightData itself errors, let Scrapy handle retries/fallback
        if resp.status_code >= 400:
            spider.logger.error(
                "BrightData API error status=%s url=%s body=%s",
                resp.status_code,
                request.url,
                (resp.text or "")[:800],
            )
            return None

        body = resp.content or b""

        # One retry if the HTML looks like a soft block / placeholder
        if self._looks_bad_html(body) and not request.meta.get("_brightdata_unlocker_retried"):
            request.meta["_brightdata_unlocker_retried"] = True
            resp2 = do_call()
            if resp2 is not None and resp2.status_code < 400 and resp2.content:
                body = resp2.content

        # Build Scrapy response
        request.meta["brightdata_via_unlocker"] = True

        return HtmlResponse(
            url=request.url,
            status=resp.status_code,
            body=body,
            encoding=resp.encoding or "utf-8",
            request=request,
        )
