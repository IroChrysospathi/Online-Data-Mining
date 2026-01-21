"""
Output JSONL items:
- type = "CUSTOMER_SERVICE"
- type = "EXPERT_SUPPORT"
"""

from __future__ import annotations

import csv
import json
import os
import re
import hashlib
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

import scrapy

# Helpers
def iso_utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def clean(text):
    if text is None:
        return None
    return re.sub(r"\s+", " ", str(text)).strip() or None


def detect_int(x):
    try:
        return int(x)
    except Exception:
        return None


def is_bol_domain(url: str) -> bool:
    if not url:
        return False
    try:
        return urlparse(url).netloc.endswith("bol.com")
    except Exception:
        return False


def stable_int_key(s: str, *, mod: int = 2_000_000_000) -> int:
    if s is None:
        s = ""
    h = hashlib.sha1(s.encode("utf-8")).hexdigest()
    return int(h[:12], 16) % mod


def to_decimal_eur(s):
    if s is None:
        return None
    s = str(s).strip().replace("€", "").strip()
    s = s.replace(".", "").replace(",", ".")
    s = re.sub(r"[^\d.]", "", s)
    if not s:
        return None
    try:
        return float(s)
    except Exception:
        return None


def text_has_any(text, words):
    t = (text or "").lower()
    return any(w.lower() in t for w in words)

# Bright Data proxy handling
def brightdata_proxy_url() -> str | None:
    """
    Supports either:
      - BRIGHTDATA_PROXY="http://user:pass@host:port"
    or:
      - BRIGHTDATA_USERNAME / BRIGHTDATA_PASSWORD / BRIGHTDATA_HOST / BRIGHTDATA_PORT
    """
    p = os.getenv("BRIGHTDATA_PROXY")
    if p:
        return p.strip()

    user = os.getenv("BRIGHTDATA_USERNAME")
    pwd = os.getenv("BRIGHTDATA_PASSWORD")
    host = os.getenv("BRIGHTDATA_HOST")
    port = os.getenv("BRIGHTDATA_PORT")

    if user and pwd and host and port:
        return f"http://{user}:{pwd}@{host}:{port}"

    return None

# Selenium fallback
BLOCKED_MARKERS = [
    "this is a modal window",
    "beginning of dialog window",
    "cookie",
    "toestemming",
    "consent",
    "accepteer",
    "accept all",
    "captcha",
    "access denied",
    "forbidden",
    "robot",
]


def looks_like_shell_or_blocked_html(html: str | None) -> bool:
    if not html:
        return True
    low = html.lower()
    if any(m in low for m in BLOCKED_MARKERS):
        return True
    # very small HTML often indicates consent shell
    if len(low) < 20_000:
        return True
    return False


def selenium_enabled() -> bool:
    return str(os.getenv("USE_SELENIUM", "")).strip().lower() in {"1", "true", "yes", "y", "on"}


def render_with_selenium(url: str, wait_seconds: int = 6) -> str:
    """
    Render URL with Selenium and return page_source.
    Tries to accept cookies.
    """
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.chrome.service import Service
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    import time

    chromedriver_path = os.getenv("CHROMEDRIVER")
    service = Service(chromedriver_path) if chromedriver_path else Service()

    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--window-size=1365,900")
    options.add_argument(
        "--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
    )

    driver = webdriver.Chrome(service=service, options=options)
    try:
        driver.get(url)
        time.sleep(1.2)

        for xpath in [
            "//button[contains(translate(., 'AKKOORDACCEPT', 'akkoordaccept'), 'akkoord')]",
            "//button[contains(translate(., 'AKKOORDACCEPT', 'akkoordaccept'), 'accept')]",
            "//button[contains(translate(., 'ALLES', 'alles'), 'alles')]",
        ]:
            try:
                btn = WebDriverWait(driver, 1.5).until(EC.element_to_be_clickable((By.XPATH, xpath)))
                btn.click()
                time.sleep(0.6)
                break
            except Exception:
                pass

        wait = WebDriverWait(driver, max(2, int(wait_seconds)))
        try:
            wait.until(
                lambda d: (
                    len(d.find_elements(By.CSS_SELECTOR, "body")) > 0
                    and (len(d.page_source) > 30_000)
                )
            )
        except Exception:
            pass

        time.sleep(0.8)
        return driver.page_source
    finally:
        driver.quit()

# Spider
class BolSupportSpider(scrapy.Spider):
    name = "bol_support"
    allowed_domains = ["bol.com"]
    handle_httpstatus_list = [404]

    custom_settings = {
        "ROBOTSTXT_OBEY": False,
        "DOWNLOAD_TIMEOUT": 45,
        "RETRY_TIMES": 2,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": 1.0,
        "AUTOTHROTTLE_MAX_DELAY": 10.0,
        "CONCURRENT_REQUESTS": 2,
        "USER_AGENT": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0 Safari/537.36"
        ),
        "HTTPERROR_ALLOWED_CODES": [404],
    }

    def __init__(self, input_file=None, competitor_id=2, selenium_wait=6, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.input_file = input_file
        self.competitor_id = detect_int(competitor_id) if competitor_id is not None else 2

        try:
            self.selenium_wait = int(selenium_wait)
        except Exception:
            self.selenium_wait = 6

        self.proxy_url = brightdata_proxy_url()

        # Defaults 
        self.global_free_shipping_threshold_amt = None
        self.global_cooling_off_days = 30

        self.support_seed_urls = [
            "https://www.bol.com/nl/nl/klantenservice/",
            "https://www.bol.com/nl/nl/klantenservice/contact/",
            "https://www.bol.com/nl/nl/klantenservice/retourneren/",
            "https://www.bol.com/nl/nl/klantenservice/bezorgen/",
        ]

        self.product_rows = self._load_products(self.input_file)
        self.logger.info("Loaded %s product URLs from input_file=%s", len(self.product_rows), self.input_file)

    # input loading

    def _load_products(self, path):
        rows = []
        if not path:
            self.logger.error("Missing -a input_file=... (required)")
            return rows

        if not os.path.exists(path):
            self.logger.error("input_file not found: %s", path)
            return rows

        ext = os.path.splitext(path)[1].lower()

        def add_row(obj):
            if not isinstance(obj, dict):
                return

            # accept productlisting items from bol_products.jsonl
            t = obj.get("type")
            if t and t not in {"PRODUCTLISTING"}:
                return

            url = obj.get("product_url")
            if not url or not is_bol_domain(url):
                return

            url = url.strip()
            rows.append({"url": url, "listing_key": stable_int_key(url)})

        if ext in [".jsonl", ".json"]:
            raw = Path(path).read_text(encoding="utf-8").strip()
            if not raw:
                return rows

            if raw.startswith("["):
                try:
                    data = json.loads(raw)
                except Exception as e:
                    self.logger.error("Failed to parse JSON array: %s", e)
                    return rows
                if isinstance(data, list):
                    for obj in data:
                        add_row(obj)
                return rows

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

        self.logger.error("Unsupported input_file extension: %s", ext)
        return rows

    # helpers 

    def _base_meta(self):
        m = {}
        if self.proxy_url:
            m["proxy"] = self.proxy_url
        return m

    def maybe_render(self, response):
        if not selenium_enabled():
            return response

        html = response.text or ""
        if not looks_like_shell_or_blocked_html(html):
            return response

        self.logger.warning("Selenium fallback: %s", response.url)
        try:
            html2 = render_with_selenium(response.url, wait_seconds=self.selenium_wait)
            from scrapy.http import HtmlResponse
            return HtmlResponse(url=response.url, body=html2, encoding="utf-8", request=response.request)
        except Exception as exc:
            self.logger.warning("Selenium render failed url=%s err=%s", response.url, exc)
            return response

    # crawl 

    def start_requests(self):
        if not self.product_rows:
            self.logger.error("No product URLs loaded. Use -a input_file=... with PRODUCTLISTING lines.")
            return

        meta = self._base_meta()

        # first parse global defaults from support pages, then schedule products
        yield scrapy.Request(
            self.support_seed_urls[0],
            callback=self.parse_support_then_schedule,
            meta={**meta, "support_index": 0},
            dont_filter=True,
        )

    def parse_support_then_schedule(self, response):
        response = self.maybe_render(response)
        idx = response.meta.get("support_index", 0)

        if response.status == 404:
            self.logger.warning("Support URL 404: %s", response.url)
            next_idx = idx + 1
            if next_idx < len(self.support_seed_urls):
                yield scrapy.Request(
                    self.support_seed_urls[next_idx],
                    callback=self.parse_support_then_schedule,
                    meta={**self._base_meta(), "support_index": next_idx},
                    dont_filter=True,
                )
                return
            self.logger.warning("All support URLs failed. Proceeding with product scraping only.")
        else:
            full_text = clean(" ".join(response.css("body *::text").getall())) or ""

            m = re.search(
                r"gratis\s+verzending.{0,80}?vanaf\s*€\s*([0-9]+(?:[.,][0-9]{1,2})?)",
                full_text,
                re.IGNORECASE,
            )
            if m:
                self.global_free_shipping_threshold_amt = to_decimal_eur(m.group(1))

            m = re.search(r"(\d+)\s*dagen\s*bedenktijd", full_text, re.IGNORECASE)
            if m:
                v = detect_int(m.group(1))
                if v:
                    self.global_cooling_off_days = v

        meta = self._base_meta()
        for row in self.product_rows:
            yield scrapy.Request(
                row["url"],
                callback=self.parse_product,
                meta={**meta, "listing_key": row["listing_key"], "product_url": row["url"]},
                dont_filter=True,
            )

    def parse_product(self, response):
        response = self.maybe_render(response)

        scraped_at = iso_utc_now()
        product_url = response.meta.get("product_url") or response.url
        listing_key = response.meta.get("listing_key") or stable_int_key(product_url)

        full_text = clean(" ".join(response.css("body *::text").getall())) or ""

        # CUSTOMER_SERVICE (DB columns)
        shipping_included = None
        if text_has_any(full_text, ["gratis verzending", "gratis bezorging", "gratis geleverd"]):
            shipping_included = True
        elif text_has_any(full_text, ["verzendkosten", "bezorgkosten"]):
            shipping_included = False

        free_shipping_threshold_amt = self.global_free_shipping_threshold_amt

        pickup_point_available = True if text_has_any(full_text, ["afhaalpunt", "ophaalpunt", "afhalen", "pickup point", "pick-up point"]) else None
        delivery_shipping_available = True if text_has_any(full_text, ["bezorgen", "bezorgd", "geleverd", "levertijd", "thuisbezorgd", "morgen in huis"]) else None
        delivery_courier_available = True if text_has_any(full_text, ["postnl", "dhl", "dpd", "ups", "gls", "bezorger", "koerier"]) else None

        cooling_off_days = None
        m = re.search(r"(\d+)\s*dagen\s*bedenktijd", full_text, re.IGNORECASE)
        if m:
            cooling_off_days = detect_int(m.group(1))
        if cooling_off_days is None:
            cooling_off_days = self.global_cooling_off_days

        free_returns = True if text_has_any(full_text, ["gratis retourneren", "gratis retour", "kosteloos retourneren", "gratis terugsturen"]) else None

        warranty_provider = None
        m = re.search(r"verkoop\s+door\s+([^\|\n\r]+)", full_text, re.IGNORECASE)
        seller_text = clean(m.group(1)) if m else None
        if seller_text:
            if "bol.com" in seller_text.lower() or seller_text.lower().strip() in {"bol", "bolcom"}:
                warranty_provider = "bol.com"
            else:
                warranty_provider = seller_text

        warranty_duration_months = None
        m = re.search(r"(\d+)\s*(jaar|jaren)\s*garantie", full_text, re.IGNORECASE)
        if m:
            y = detect_int(m.group(1))
            if y is not None:
                warranty_duration_months = y * 12
        if warranty_duration_months is None:
            m = re.search(r"(\d+)\s*(maand|maanden)\s*garantie", full_text, re.IGNORECASE)
            if m:
                warranty_duration_months = detect_int(m.group(1))

        customer_service_url = None
        for h in response.css("a::attr(href)").getall():
            if not h:
                continue
            u = response.urljoin(h)
            if not is_bol_domain(u):
                continue
            ul = u.lower()
            if "/klantenservice" in ul and any(x in ul for x in ["/retour", "/retourneren", "/garantie", "/bezorgen", "/contact", "/verzenden"]):
                customer_service_url = u
                break
        if customer_service_url is None:
            customer_service_url = "https://www.bol.com/nl/nl/klantenservice/"

        yield {
            "type": "CUSTOMER_SERVICE",
            "competitor_id": self.competitor_id,
            "listing_id": None,            # resolved during DB import using listing_key
            "scraped_at": scraped_at,
            "shipping_included": shipping_included,
            "free_shipping_threshold_amt": free_shipping_threshold_amt,
            "pickup_point_available": pickup_point_available,
            "delivery_shipping_available": delivery_shipping_available,
            "delivery_courier_available": delivery_courier_available,
            "cooling_off_days": cooling_off_days,
            "free_returns": free_returns,
            "warranty_provider": warranty_provider,
            "warranty_duration_months": warranty_duration_months,
            "customer_service_url": customer_service_url,
            "listing_key": listing_key,     # helper for joining to productlisting
            "product_url": product_url,     
        }

        # EXPERT_SUPPORT (DB columns)
        sold_by_bol = False
        if seller_text:
            sold_by_bol = ("bol.com" in seller_text.lower()) or (seller_text.lower().strip() in {"bol", "bolcom"})

        mentions_chat = text_has_any(full_text, ["chat", "livechat", "chatten", "chat met"])
        expert_chat_available = None
        if sold_by_bol and (mentions_chat or text_has_any(full_text, ["klantenservice", "hulp", "contact"])):
            expert_chat_available = True
        elif (not sold_by_bol) and mentions_chat:
            expert_chat_available = True

        phone_support_available = True if (
            re.search(r"\b(\+31|0)\s?\d{1,3}[\s\-]?\d{3,4}[\s\-]?\d{3,4}\b", full_text)
            or text_has_any(full_text, ["bel ons", "telefonisch", "telefoon"])
        ) else None

        email_support_available = None
        if text_has_any(full_text, ["stuur een bericht", "stuur ons een bericht", "contactformulier", "e-mail", "email", "mail ons"]):
            email_support_available = True
        elif sold_by_bol and text_has_any(full_text, ["klantenservice", "contact"]):
            email_support_available = True

        in_store_support = False  # bol has no stores

        # a compact support snippet
        expert_support_text = clean(full_text[:4000])

        yield {
            "type": "EXPERT_SUPPORT",
            "competitor_id": self.competitor_id,
            "scraped_at": scraped_at,
            "source_url": product_url,
            "expert_chat_available": expert_chat_available,
            "phone_support_available": phone_support_available,
            "email_support_available": email_support_available,
            "in_store_support": in_store_support,
            "expert_support_text": expert_support_text,
            "listing_key": listing_key,   # helper for joining to productlisting
        }
