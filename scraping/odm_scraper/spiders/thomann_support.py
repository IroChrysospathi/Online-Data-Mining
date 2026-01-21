"""
Support spider.

Responsibilities:
- Crawl non-product pages (e.g., FAQ, warranty, support information)
- Extract relevant informational content
- Yield structured support data if required by the project
"""

"""
thomann_support.py

Goal:
- Produce competitor-level Customer Service and Expert Support data for Thomann (competitor_id=4)

Sources:
1) Microphones category page (contact channels):
   https://www.thomann.nl/alle-producten-in-de-categorie-microfoons.html
2) Product pages (money-back + warranty text often appears):
   loaded via -a input_file=... (JSONL/JSON/CSV), typically the output of thomann_products spider.

Exports:
- type="customer_service" (competitor-level)
- type="expert_support"   (competitor-level)

All items include competitor_id=4 consistently.
"""

import csv
import json
import os
import re
import uuid
import subprocess
import scrapy
from datetime import datetime, timezone
from urllib.parse import urlparse, urlunparse


COMPETITOR_ID = 4
COMPETITOR_NAME = "Thomann"

# Helpers

def iso_utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def clean(text):
    if text is None:
        return None
    return re.sub(r"\s+", " ", str(text)).strip() or None


def canonicalize_url(url: str) -> str:
    try:
        u = urlparse(url)
        return urlunparse((u.scheme, u.netloc, u.path.rstrip("/"), "", "", ""))
    except Exception:
        return url


def get_git_commit_hash():
    try:
        out = subprocess.check_output(["git", "rev-parse", "HEAD"], stderr=subprocess.DEVNULL)
        return out.decode("utf-8", errors="ignore").strip() or None
    except Exception:
        return None


def is_thomann_domain(url: str) -> bool:
    if not url:
        return False
    try:
        return urlparse(url).netloc.endswith("thomann.nl")
    except Exception:
        return False


def extract_thomann_listing_id_from_html(html_text: str) -> str | None:
    if not html_text:
        return None
    m = re.search(r"artikelnummer\s*[:#]?\s*(\d{5,})", html_text, flags=re.IGNORECASE)
    if m:
        return m.group(1)
    m = re.search(r"/prod/(\d{5,})\.(?:jpg|jpeg|png)", html_text, flags=re.IGNORECASE)
    if m:
        return m.group(1)
    return None


def parse_money_back_days(text: str) -> int | None:
    if not text:
        return None
    m = re.search(r"(\d{1,3})\s+dagen\s+money-?\s*back", text, flags=re.IGNORECASE)
    if m:
        try:
            return int(m.group(1))
        except Exception:
            return None
    return None


def parse_warranty_years(text: str) -> int | None:
    if not text:
        return None
    # e.g., "Drie jaar Thomann garantie" OR "3 jaar Thomann garantie"
    m = re.search(r"(\d{1,2})\s+jaar\s+thomann\s+garantie", text, flags=re.IGNORECASE)
    if m:
        try:
            return int(m.group(1))
        except Exception:
            return None
    # Dutch word numbers (very small set; best-effort)
    m = re.search(r"\b(een|twee|drie|vier|vijf)\s+jaar\s+thomann\s+garantie", text, flags=re.IGNORECASE)
    if m:
        mapping = {"een": 1, "twee": 2, "drie": 3, "vier": 4, "vijf": 5}
        return mapping.get(m.group(1).lower())
    return None


def detect_channels(text: str):
    """
    Returns dict of booleans for chat/phone/email/whatsapp based on keyword presence.
    """
    t = (text or "").lower()
    return {
        "chat_available": ("chat" in t),
        "phone_available": ("tel" in t) or ("telefoon" in t) or ("bel" in t),
        "email_available": ("e-mail" in t) or ("email" in t) or ("@" in t),
        "whatsapp_available": ("whatsapp" in t),
    }

# Spider

class ThomannSupportSpider(scrapy.Spider):
    name = "thomann_support"
    allowed_domains = ["thomann.nl"]

    custom_settings = {
        "ROBOTSTXT_OBEY": True,
        "DOWNLOAD_DELAY": 1.5,
        "AUTOTHROTTLE_ENABLED": True,
        "CONCURRENT_REQUESTS": 2,
        "USER_AGENT": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0 Safari/537.36"
        ),
        "CLOSESPIDER_PAGECOUNT": 200,
    }

    crawler_version = "thomann_support/RAW-1.0"

    def __init__(self, input_file=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.input_file = input_file

        self.scrape_run_id = str(uuid.uuid4())
        self.started_at = iso_utc_now()
        self.git_commit_hash = get_git_commit_hash()

        self.microphones_category_url = "https://www.thomann.nl/alle-producten-in-de-categorie-microfoons.html"

        # loaded product URLs (optional but recommended)
        self.product_rows = self._load_products(self.input_file)

        # aggregated support signals
        self.money_back_days = None
        self.warranty_years = None
        self.channel_flags = {
            "chat_available": None,
            "phone_available": None,
            "email_available": None,
            "whatsapp_available": None,
        }

        self.source_urls = set()

 
    # Input loading 

    def _load_products(self, path):
        """
        Accepts JSONL/JSON/CSV produced by products spider.
        Expected fields per row: source_url or url.
        """
        rows = []
        if not path:
            return rows
        if not os.path.exists(path):
            self.logger.error(f"input_file not found: {path}")
            return rows

        ext = os.path.splitext(path)[1].lower()

        def add_row(obj):
            if not isinstance(obj, dict):
                return
            # if type exists, accept product rows only
            if "type" in obj and obj.get("type") != "product":
                return
            url = obj.get("source_url") or obj.get("url") or obj.get("product_url")
            if not url or not is_thomann_domain(url):
                return
            rows.append({"url": canonicalize_url(url)})

        if ext in [".jsonl", ".json"]:
            with open(path, "r", encoding="utf-8") as f:
                raw = f.read().strip()
            if not raw:
                return rows

            # JSON array
            if raw[0] == "[":
                try:
                    data = json.loads(raw)
                except Exception as e:
                    self.logger.error(f"Failed to parse JSON array: {e}")
                    return rows
                if isinstance(data, list):
                    for obj in data:
                        add_row(obj)
                return rows

            # JSONL
            for line in raw.splitlines():
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except Exception:
                    continue
                add_row(obj)
            return rows

        if ext == ".csv":
            with open(path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for obj in reader:
                    add_row(obj)
            return rows

        self.logger.error(f"Unsupported input_file extension: {ext}")
        return rows

   
    # Crawl

    def start_requests(self):
        # Emit run metadata once
        yield {
            "type": "run",
            "scrape_run_id": self.scrape_run_id,
            "started_at": self.started_at,
            "git_commit_hash": self.git_commit_hash,
            "crawler_version": self.crawler_version,
            "competitor_id": COMPETITOR_ID,
            "competitor_name": COMPETITOR_NAME,
            "notes": "Thomann support crawl",
        }

        # 1) Always scrape category page first for expert contact channels
        yield scrapy.Request(self.microphones_category_url, callback=self.parse_category_support, dont_filter=True)

        # 2) Then scrape product pages to confirm money-back + warranty wording
        for row in self.product_rows:
            yield scrapy.Request(row["url"], callback=self.parse_product_support, dont_filter=False)

    def parse_category_support(self, response):
        self.source_urls.add(canonicalize_url(response.url))
        full_text = clean(" ".join(response.css("body *::text").getall())) or ""

        # Channel detection
        flags = detect_channels(full_text)
        for k, v in flags.items():
            # aggregate with OR logic; if None set to v
            if self.channel_flags.get(k) is None:
                self.channel_flags[k] = v
            else:
                self.channel_flags[k] = bool(self.channel_flags[k] or v)

        # Yield expert_support competitor-level (category page is a strong source)
        yield {
            "type": "expert_support",
            "competitor_id": COMPETITOR_ID,
            "competitor_name": COMPETITOR_NAME,
            "scrape_run_id": self.scrape_run_id,
            "scraped_at": iso_utc_now(),
            "chat_available": self.channel_flags["chat_available"],
            "phone_available": self.channel_flags["phone_available"],
            "email_available": self.channel_flags["email_available"],
            "whatsapp_available": self.channel_flags["whatsapp_available"],
            "source_url": canonicalize_url(response.url),
        }

        # Also emit customer_service if we already have values (maybe from previous product parses)
        if self.money_back_days is not None or self.warranty_years is not None:
            yield self._customer_service_item()

    def parse_product_support(self, response):
        self.source_urls.add(canonicalize_url(response.url))
        full_text = clean(" ".join(response.css("body *::text").getall())) or ""

        # Extract money-back / warranty from product pages (best-effort)
        mb = parse_money_back_days(full_text)
        wy = parse_warranty_years(full_text)

        # Keep first non-null values (or you can keep max/most common)
        if self.money_back_days is None and mb is not None:
            self.money_back_days = mb
        if self.warranty_years is None and wy is not None:
            self.warranty_years = wy

        # Once we have something, yield customer_service competitor-level
        if self.money_back_days is not None or self.warranty_years is not None:
            yield self._customer_service_item()

    def _customer_service_item(self):
        """
        Competitor-level customer service item.
        """
        # choose one representative source URL if available
        src = None
        if self.source_urls:
            src = sorted(self.source_urls)[0]

        return {
            "type": "customer_service",
            "competitor_id": COMPETITOR_ID,
            "competitor_name": COMPETITOR_NAME,
            "scrape_run_id": self.scrape_run_id,
            "scraped_at": iso_utc_now(),
            "money_back_days": self.money_back_days,
            "warranty_years": self.warranty_years,
            "source_url": src,
        }
