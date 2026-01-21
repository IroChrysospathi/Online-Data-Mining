# bol_products.py
# Scrapy spider that ONLY crawls https://www.bol.com/nl/nl/l/microfoons/7119/
# and emits ERD-aligned rows for microphone products.
#
# Improvements in this version:
# - Much stronger “blocked/modal/consent shell” detection
# - Selenium fallback triggers when HTML is clearly not a real product/listing page
# - Selenium waits for real selectors (title/price/buy block), tries to accept cookies
# - Extracts title/description/price/stock also from JSON-LD (offers.*) when available
# - Better price/stock selectors + better description cleanup
#
# NEW IN THIS UPDATE (your request):
# - Stronger price extraction (handles split price, meta/itemprop, embedded JSON, aria-labels)
# - Stronger stock extraction + explicit YES/NO label + scraping "In stock" badge/text
#
# Output items (JSON lines):
# - SCRAPERUN
# - CATEGORY (ONLY seed)
# - PRODUCT
# - PRODUCTLISTING
# - PRICESNAPSHOT
# - REVIEW (aggregate placeholder)
# - PRODUCTMATCH

from __future__ import annotations

import json
import os
import re
import uuid
import subprocess
import hashlib
from datetime import datetime, timezone
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

import scrapy


# -------------------------
# Microphone category constraints (breadcrumbs only)
# -------------------------

ALLOWED_CATEGORY_KEYWORDS = {
    "microfoons",
    "studiomicrofoon",
    "live-microfoon",
    "draadloze-microfoon",
    "usb-microfoon",
    "multimedia-av-microfoon",
    "zang-microfoon",
    "microfoon-opnameset",
}

EXCLUDED_CATEGORY_KEYWORDS = {
    "accessoire", "accessoires", "toebehoren",
    "onderdeel", "onderdelen",
    "statieven", "statief",
    "kabel", "kabels",
    "clip", "clips",
    "klem", "klemmen",
    "windkap", "windkappen",
    "popfilter", "popfilters",
    "capsule", "capsules",
    "shockmount", "shockmounts",
    "pistoolgreep", "pistoolgrepen",
    "opbergtassen", "hoezen",
    "flightcase", "flightcases",
    "accu", "lader", "laders",
    "booster", "boosters",
    "reflectiefilter", "reflectiefilters",
    "voorversterker", "voorversterkers",
    "vocal-effect", "vocal-effecten",
}


# -------------------------
# Product-level microphone filter
# -------------------------

MIC_INCLUDE_WORDS = {
    "microfoon", "microfoons", "microphone", "mic",
    "lavalier", "dasspeld",
    "usb microfoon", "usb-microfoon",
    "studio microfoon", "studiomicrofoon",
    "draadloze microfoon", "draadloze-microfoon",
    "zangmicrofoon", "zang microfoon",
    "karaoke microfoon", "condensator microfoon", "dynamische microfoon",
    "shotgun microfoon", "richtmicrofoon",
}

MIC_EXCLUDE_WORDS = {
    "koptelefoon", "koptelefoons", "headphone", "headset", "hoofdtelefoon",
    "oortjes", "earbuds",
    "speaker", "luidspreker", "partybox", "soundbar",
    "camera", "gimbal", "drone",
    "accessoire", "accessoires", "toebehoren",
    "kabel", "adapter",
    "statief", "statieven",
    "houder", "mount", "shockmount",
    "popfilter", "windkap",
}


# -------------------------
# helpers
# -------------------------

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def clean(text):
    if text is None:
        return None
    s = re.sub(r"\s+", " ", str(text)).strip()
    return s or None


def get_git_commit_hash():
    try:
        out = subprocess.check_output(["git", "rev-parse", "HEAD"], stderr=subprocess.DEVNULL)
        return out.decode("utf-8").strip()
    except Exception:
        return None


def brightdata_mode() -> str:
    # Unlocker has priority if token+zone are set
    if os.getenv("BRIGHTDATA_TOKEN") and os.getenv("BRIGHTDATA_ZONE"):
        return "unlocker_api"
    # Proxy mode if full proxy or username+password exist
    if os.getenv("BRIGHTDATA_PROXY") or (os.getenv("BRIGHTDATA_USERNAME") and os.getenv("BRIGHTDATA_PASSWORD")):
        return "proxy"
    return "disabled"


def strip_tracking(url: str) -> str:
    if not url:
        return url
    try:
        u = urlparse(url)
        q = parse_qs(u.query)
        drop = {
            "bltgh", "bltghc", "blt", "Referrer", "referrer", "promo", "promoCode",
            "gclid", "fbclid", "utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content",
        }
        q2 = {k: v for k, v in q.items() if k not in drop}
        query = urlencode(q2, doseq=True) if q2 else ""
        return urlunparse((u.scheme, u.netloc, u.path, u.params, query, ""))
    except Exception:
        return url


# -------------------------
# PRICE parsing (ENHANCED)
# -------------------------

_PRICE_RX = re.compile(r"(\d{1,3}(?:[.\s]\d{3})*|\d+)(?:[.,](\d{1,2}))?")

def price_to_float(text: str | None):
    """
    Robust EUR text -> float.
    Handles:
      - "€ 49,99"
      - "49,99"
      - "49.-" / "49,-"
      - "1.234,56"
      - "1 234,56"
      - "€\xa049,99"
    """
    if not text:
        return None
    s = clean(text)
    if not s:
        return None

    s = (
        s.replace("\u20ac", "")
         .replace("€", "")
         .replace("\xa0", " ")
         .replace("\u202f", " ")
         .strip()
    )
    # common dash cents: 49,- or 49.-
    s = re.sub(r"(\d)\s*[.,]\s*[-–—]\b", r"\1", s)

    m = _PRICE_RX.search(s)
    if not m:
        return None

    whole = (m.group(1) or "").replace(" ", "").replace(".", "")
    frac = m.group(2)

    try:
        if frac is None or frac == "":
            return float(int(whole))
        if len(frac) == 1:
            frac = frac + "0"
        return float(f"{int(whole)}.{int(frac):02d}")
    except Exception:
        return None


def _first_text(response, selectors: list[str]) -> str | None:
    for sel in selectors:
        v = response.css(sel).get()
        if v:
            v = clean(v)
            if v:
                return v
    return None


def _first_all_text(response, selectors: list[str], limit: int = 80) -> str | None:
    """
    For cases where price is split into multiple spans.
    """
    for sel in selectors:
        parts = response.css(sel).getall()
        if not parts:
            continue
        joined = clean(" ".join([p for p in parts if clean(p)])[:limit])
        if joined:
            return joined
    return None


def extract_prices_from_ld(product_node: dict | None):
    """
    Extract current/base/discount from JSON-LD offers variants:
      - Offer with price
      - AggregateOffer with lowPrice/highPrice
      - priceSpecification
    """
    current_price = None
    base_price = None
    discount_amount = None
    discount_percent = None
    currency = None

    if not product_node or not isinstance(product_node, dict):
        return current_price, base_price, discount_amount, discount_percent, currency

    offers = product_node.get("offers")
    if isinstance(offers, list) and offers:
        offers = offers[0]

    if not isinstance(offers, dict):
        return current_price, base_price, discount_amount, discount_percent, currency

    currency = clean(offers.get("priceCurrency"))

    # price / lowPrice
    raw_price = offers.get("price")
    raw_low = offers.get("lowPrice")

    def _to_float(x):
        if x is None:
            return None
        try:
            return float(str(x).replace(",", "."))
        except Exception:
            return None

    current_price = _to_float(raw_price)
    if current_price is None:
        current_price = _to_float(raw_low)

    # sometimes base/was price is inside priceSpecification
    ps = offers.get("priceSpecification")
    if isinstance(ps, list) and ps:
        ps = ps[0]
    if isinstance(ps, dict):
        # try "price" and/or "valueAddedTaxIncluded" patterns
        p2 = _to_float(ps.get("price"))
        if current_price is None:
            current_price = p2

    # compute discount if we can infer "highPrice" as base
    raw_high = offers.get("highPrice")
    base_price = _to_float(raw_high)

    if current_price is not None and base_price is not None and base_price > current_price:
        discount_amount = round(base_price - current_price, 2)
        try:
            discount_percent = round((discount_amount / base_price) * 100.0, 2)
        except Exception:
            discount_percent = None

    return current_price, base_price, discount_amount, discount_percent, currency


def extract_price_fields(response, product_node: dict | None):
    """
    Returns:
      current_price, base_price, discount_amount, discount_percent, price_text
    """
    # 1) JSON-LD (best when present)
    ld_current, ld_base, ld_disc_amt, ld_disc_pct, _ld_cur = extract_prices_from_ld(product_node)

    # 2) HTML selectors (covers split prices + meta tags)
    # Try meta/itemprop first (often survives hydration)
    meta_price = response.css('meta[itemprop="price"]::attr(content)').get()
    if not meta_price:
        meta_price = response.css('meta[property="product:price:amount"]::attr(content)').get()
    meta_price = clean(meta_price)

    # Common bol price containers / split pieces
    # (don’t rely on exact classnames only; bol often splits whole & cents)
    price_text = (
        _first_text(response, [
            '[data-test="price"]::text',
            '[data-test="product-price"]::text',
            '[data-test="buy-block"] [data-test="price"]::text',
            '[data-test="buy-block"] [class*="price"]::text',
            '[class*="promo-price"]::text',
            '[class*="current-price"]::text',
            '[aria-label*="€"]::attr(aria-label)',
            '[data-test="price"]::attr(aria-label)',
        ])
        or _first_all_text(response, [
            # split price patterns
            '[data-test="price"] *::text',
            '[data-test="product-price"] *::text',
            '[data-test="buy-block"] [class*="price"] *::text',
            # sometimes price is in a button or offer block
            '[data-test="buy-block"] button *::text',
        ], limit=120)
    )

    # base/was price (if shown)
    base_txt = clean(
        _first_text(response, [
            '[data-test="was-price"]::text',
            '[class*="was-price"]::text',
            '[class*="strike"]::text',
            '[class*="strikethrough"]::text',
            '[aria-label*="Van €"]::attr(aria-label)',
            '[aria-label*="van €"]::attr(aria-label)',
        ])
        or _first_all_text(response, [
            '[data-test="was-price"] *::text',
            '[class*="was-price"] *::text',
            '[class*="strike"] *::text',
        ], limit=120)
    )

    # 3) embedded JSON fallback (last resort, cheap regex search)
    # Sometimes bol embeds price in script JSON even when DOM is minimal.
    embedded_current = None
    try:
        scripts = response.css("script::text").getall()
        if scripts:
            blob = " ".join(scripts[:12])  # keep it bounded
            # try a couple of likely keys
            m = re.search(r'"price"\s*:\s*"?(?P<p>\d+(?:[.,]\d{1,2})?)"?', blob)
            if m:
                embedded_current = price_to_float(m.group("p"))
    except Exception:
        embedded_current = None

    # Compute current_price with priority:
    # JSON-LD > meta > visible text > embedded
    current_price = ld_current
    if current_price is None and meta_price:
        current_price = price_to_float(meta_price)
    if current_price is None:
        current_price = price_to_float(price_text)
    if current_price is None:
        current_price = embedded_current

    base_price = ld_base
    if base_price is None:
        base_price = price_to_float(base_txt)

    discount_amount = ld_disc_amt
    discount_percent = ld_disc_pct
    if discount_amount is None and current_price is not None and base_price is not None and base_price > current_price:
        discount_amount = round(base_price - current_price, 2)
        try:
            discount_percent = round((discount_amount / base_price) * 100.0, 2)
        except Exception:
            discount_percent = None

    # Ensure price_text exists if we have numeric price
    if not price_text and current_price is not None:
        price_text = f"€ {current_price:.2f}"

    return current_price, base_price, discount_amount, discount_percent, price_text


def normalize_bad_model(model):
    m = clean(model)
    if not m:
        return None
    if m.lower() in {"nvt", "n/a", "unknown", "onbekend", "nee"}:
        return None
    # reject common garbage fragments
    if len(m) <= 2:
        return None
    if m.lower() in {"ning", "eau", "ijk"}:
        return None
    return m


def canonicalize(brand, title, model):
    brand = clean(brand)
    title = clean(title)
    model = clean(model)
    parts = [p for p in [brand, title, model] if p]
    if not parts:
        return None
    s = " ".join(parts)
    s = re.sub(r"\s+", " ", s).strip()
    return s or None


def stable_int_key(s: str, *, mod: int = 2_000_000_000) -> int:
    if s is None:
        s = ""
    h = hashlib.sha1(s.encode("utf-8")).hexdigest()
    return int(h[:12], 16) % mod


def parse_bol_category_code(cat_url: str) -> str | None:
    if not cat_url:
        return None
    try:
        u = urlparse(cat_url)
        m = re.search(r"/l/([^/?#]+/[^/?#]+)/?", u.path)
        if m:
            return m.group(1)
        m = re.search(r"/l/([^/?#]+)/?", u.path)
        if m:
            return m.group(1)
        return None
    except Exception:
        return None


def url_slug_keyword(url: str) -> str | None:
    code = parse_bol_category_code(url)
    if not code:
        return None
    slug = code.split("/")[0].strip().lower()
    return slug or None


def is_microphone_category_url(url: str) -> bool:
    if not url:
        return False
    slug = url_slug_keyword(url)
    if not slug:
        return False
    if slug in EXCLUDED_CATEGORY_KEYWORDS:
        return False
    return slug in ALLOWED_CATEGORY_KEYWORDS


# -------------------------
# STOCK parsing (ENHANCED)
# -------------------------

def infer_in_stock(stock_text: str | None) -> bool | None:
    """
    More robust:
    - supports EN/NL badges ("In stock", "Op voorraad")
    - handles common out-of-stock/delivery exceptions
    """
    s = (stock_text or "").lower()
    s = s.replace("\xa0", " ").strip()
    if not s:
        return None

    # positive
    if "in stock" in s:
        return True
    if "op voorraad" in s:
        return True
    if "direct leverbaar" in s:
        return True
    if "vandaag besteld" in s or "morgen in huis" in s:
        # not perfect, but usually indicates available
        return True

    # negative
    if "uitverkocht" in s:
        return False
    if "tijdelijk uitverkocht" in s:
        return False
    if "niet leverbaar" in s:
        return False
    if "niet beschikbaar" in s:
        return False
    if "out of stock" in s:
        return False

    return None


def stock_yes_no_label(in_stock: bool | None) -> str | None:
    """
    Explicit YES/NO label requested.
    """
    if in_stock is True:
        return "YES"
    if in_stock is False:
        return "NO"
    return None


def stock_status_short(stock_text: str | None) -> str | None:
    s = clean(stock_text)
    if not s:
        return None
    return s[:80]


def extract_stock_fields(response, ld_availability: str | None):
    """
    Returns:
      in_stock_on_page, stock_status_text, stock_label
    """
    in_stock_on_page = None

    # 1) JSON-LD availability
    if ld_availability:
        low = ld_availability.lower()
        # schema.org/InStock, OutOfStock, PreOrder etc
        if "instock" in low:
            in_stock_on_page = True
        elif "outofstock" in low:
            in_stock_on_page = False

    # 2) Badge / visible stock status (like your screenshot "In stock")
    # Try common badge-ish nodes first
    badge_text = clean(
        _first_text(response, [
            '[data-test*="stock"]::text',
            '[data-test*="availability"]::text',
            '[class*="stock"]::text',
            '[class*="availability"]::text',
            '[aria-label*="stock"]::attr(aria-label)',
        ])
        or _first_all_text(response, [
            # include nested spans inside badge
            '[data-test*="stock"] *::text',
            '[data-test*="availability"] *::text',
            '[class*="stock"] *::text',
            '[class*="availability"] *::text',
        ], limit=120)
    )

    # 3) Delivery/buy-block text (your previous logic)
    stock_bits = response.css('[data-test="delivery-info"] *::text').getall()
    if not stock_bits:
        stock_bits = response.css('[data-test="buy-block"] *::text').getall()
    block_text = clean(" ".join([s for s in stock_bits if clean(s)])[:600])

    # decide stock_text for status (badge preferred, else block text)
    stock_text = badge_text or block_text

    if in_stock_on_page is None:
        in_stock_on_page = infer_in_stock(stock_text)

    stock_status_text = stock_status_short(stock_text)
    stock_label = stock_yes_no_label(in_stock_on_page)

    # If you want to always store the simple badge (YES/NO) visibly in stock_status_text too:
    # (keeping your schema: stock_status_text is free text)
    # stock_status_text = stock_status_text or stock_label

    return in_stock_on_page, stock_status_text, stock_label


# -------------------------
# Product-level microphone filter helpers
# -------------------------

def is_actual_microphone(response, title_on_page: str | None) -> bool:
    """
    Conservative:
    - hard reject known non-mic products
    - accept if obvious mic terms exist
    - accept common mic brands/lines
    - accept if url includes mic keywords
    """
    title = (clean(title_on_page) or "").lower()

    for w in MIC_EXCLUDE_WORDS:
        if w in title:
            return False

    for w in MIC_INCLUDE_WORDS:
        if w in title:
            return True

    KNOWN_MIC_PATTERNS = [
        "dji mic",
        "rode",
        "røde",
        "shure",
        "maono",
        "fifine",
        "saramonic",
        "boya",
        "audio-technica",
        "samson",
        "behringer",
        "sennheiser",
    ]
    if any(p in title for p in KNOWN_MIC_PATTERNS):
        return True

    url = (response.url or "").lower()
    if "microfoon" in url or "microphone" in url or "mic-" in url:
        return True

    return False


# -------------------------
# "blocked/shell" detection
# -------------------------

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
    # very tiny html is usually not real content
    if len(low) < 20_000:
        return True
    return False


# -------------------------
# OPTIONAL selenium fallback renderer
# -------------------------

def selenium_enabled() -> bool:
    return str(os.getenv("USE_SELENIUM", "")).strip().lower() in {"1", "true", "yes", "y", "on"}


def render_with_selenium(url: str, wait_seconds: int = 6) -> str:
    """
    Renders a URL with Selenium and returns page_source.
    Tries to accept cookie/consent if present.
    Waits for real product/listing markers.
    """
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.chrome.service import Service
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    import time

    # Prefer a local chromedriver if user provided it
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

        # small initial wait (gives consent overlays time to appear)
        time.sleep(1.2)

        # Try to accept cookies/consent if any obvious buttons exist
        for xpath in [
            "//button[contains(translate(., 'AKKOORDACCEPT', 'akkoordaccept'), 'akkoord')]",
            "//button[contains(translate(., 'AKKOORDACCEPT', 'akkoordaccept'), 'accept')]",
            "//button[contains(translate(., 'ALLES', 'alles'), 'alles')]",
            "//button[contains(translate(., 'TOESTEMMING', 'toestemming'), 'toestemming')]",
        ]:
            try:
                btn = WebDriverWait(driver, 1.5).until(EC.element_to_be_clickable((By.XPATH, xpath)))
                btn.click()
                time.sleep(0.6)
                break
            except Exception:
                pass

        # Wait for either listing markers or product markers
        wait = WebDriverWait(driver, max(2, int(wait_seconds)))
        try:
            wait.until(
                lambda d: (
                    len(d.find_elements(By.CSS_SELECTOR, 'a[data-test="product-title"]')) > 0
                    or len(d.find_elements(By.CSS_SELECTOR, 'a[href*="/nl/nl/p/"]')) > 6
                    or len(d.find_elements(By.CSS_SELECTOR, 'h1[data-test="title"]')) > 0
                    or len(d.find_elements(By.CSS_SELECTOR, '[data-test="buy-block"]')) > 0
                    or len(d.find_elements(By.CSS_SELECTOR, 'script[type="application/ld+json"]')) > 0
                )
            )
        except Exception:
            # Even if wait fails, return whatever we got
            pass

        # final small delay for late hydration
        time.sleep(0.8)
        return driver.page_source
    finally:
        driver.quit()


# -------------------------
# spider
# -------------------------

class BolProductsSpider(scrapy.Spider):
    name = "bol_products"
    allowed_domains = ["bol.com"]

    start_urls = ["https://www.bol.com/nl/nl/l/microfoons/7119/"]

    custom_settings = {
        "ROBOTSTXT_OBEY": False,
        "DOWNLOAD_TIMEOUT": 45,
        "RETRY_TIMES": 2,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": 1.0,
        "AUTOTHROTTLE_MAX_DELAY": 10.0,
        "CLOSESPIDER_PAGECOUNT": 400,
    }

    crawler_version = "bol_products/ERD-STRICT-1.0"

    def __init__(self, *args, selenium_wait=6, **kwargs):
        super().__init__(*args, **kwargs)

        self.scrape_run_uuid = str(uuid.uuid4())
        self.scrape_run_key = stable_int_key(self.scrape_run_uuid)
        self.started_at = utc_now_iso()
        self.git_commit_hash = get_git_commit_hash()

        self.bd_mode = brightdata_mode()

        try:
            self.selenium_wait = int(selenium_wait)
        except Exception:
            self.selenium_wait = 6

        self._seen_category_key = set()
        self._seen_listing_key = set()
        self._seen_product_key = set()

        self.seed_category_url = strip_tracking(self.start_urls[0])

        # proxy only when user actually configured proxy mode
        self.proxy_url = self._resolve_proxy_url() if self.bd_mode == "proxy" else None

    # -------- Bright Data proxy (only used if bd_mode == "proxy") --------

    def _resolve_proxy_url(self) -> str | None:
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

    def _base_meta(self) -> dict:
        m = {"seed_category_url": self.seed_category_url}
        if self.proxy_url:
            m["proxy"] = self.proxy_url
        return m

    # -------- selenium fallback wrapper --------

    def _listing_has_real_content(self, response) -> bool:
        # listing should have multiple product links
        n = len(response.css('a[data-test="product-title"]::attr(href)').getall())
        if n >= 6:
            return True
        n2 = len(response.css('a[href*="/nl/nl/p/"]::attr(href)').getall())
        return n2 >= 10

    def _product_has_real_content(self, response) -> bool:
        # require a title plus at least one "commerce signal" (price or buy block or offers in JSON-LD)
        title = response.css('h1[data-test="title"]::text').get() or response.css("h1::text").get()
        if not clean(title):
            return False

        has_buy_block = bool(response.css('[data-test="buy-block"]').get())

        # expanded price checks (more resilient)
        has_price = bool(
            response.css('meta[itemprop="price"]::attr(content)').get()
            or response.css('[data-test="price"]::text').get()
            or response.css('[data-test="product-price"]::text').get()
            or response.css('[class*="promo-price"]::text').get()
            or response.css('[aria-label*="€"]::attr(aria-label)').get()
        )

        has_ld = bool(response.css('script[type="application/ld+json"]::text').getall())
        return has_buy_block or has_price or has_ld

    def maybe_render(self, response, reason: str):
        if not selenium_enabled():
            return response

        html = response.text or ""
        if not looks_like_shell_or_blocked_html(html):
            # HTML looks “real enough”, now check page-specific markers
            if reason == "listing" and self._listing_has_real_content(response):
                return response
            if reason == "product" and self._product_has_real_content(response):
                return response

        self.logger.warning("Selenium fallback (%s): %s", reason, response.url)
        try:
            html2 = render_with_selenium(response.url, wait_seconds=self.selenium_wait)
            from scrapy.http import HtmlResponse
            return HtmlResponse(url=response.url, body=html2, encoding="utf-8", request=response.request)
        except Exception as exc:
            self.logger.warning("Selenium render failed url=%s err=%s", response.url, exc)
            return response

    # -------- ERD emitters --------

    def emit_scraperun(self):
        yield {
            "type": "SCRAPERUN",
            "scrape_run_id": None,
            "started_at": self.started_at,
            "git_commit_hash": self.git_commit_hash,
            "crawler_version": self.crawler_version,
            "notes": "bol microphones only (microfoons/7119) + product-level mic filter + brightdata unlocker/proxy",
            "scrape_run_key": self.scrape_run_key,
        }

    def emit_category(self, *, url: str, name: str | None):
        # HARD LOCK: only emit the seed category row
        url = strip_tracking(url)
        if url != self.seed_category_url:
            return

        category_code = parse_bol_category_code(url)
        if not category_code:
            return

        category_key = stable_int_key(url)
        if category_key in self._seen_category_key:
            return
        self._seen_category_key.add(category_key)

        yield {
            "type": "CATEGORY",
            "category": category_code,
            "competitor_id": None,
            "name": clean(name) or "Microfoons",
            "url": url,
            "parent_category_id": None,
            "category_key": category_key,
            "scrape_run_key": self.scrape_run_key,
            "scrape_run_id": None,
        }

    def emit_product(self, *, canonical_name: str, brand: str | None, model: str | None) -> int:
        canonical_name = clean(canonical_name)
        if not canonical_name:
            return None

        product_key = stable_int_key(canonical_name)
        if product_key in self._seen_product_key:
            return product_key

        self._seen_product_key.add(product_key)
        yield {
            "type": "PRODUCT",
            "product_id": None,
            "canonical_name": canonical_name,
            "brand": clean(brand),
            "model": clean(model),
            "product_key": product_key,
            "scrape_run_key": self.scrape_run_key,
        }
        return product_key

    def emit_productlisting(
        self,
        *,
        product_url: str,
        title_on_page: str | None,
        image_url_on_page: str | None,
        in_stock_on_page: bool | None,
        gtin_on_page: str | None,
        description_clean: str | None,
        category_id=None,
    ) -> int:
        product_url = strip_tracking(product_url)
        listing_key = stable_int_key(product_url)
        if listing_key in self._seen_listing_key:
            return listing_key

        self._seen_listing_key.add(listing_key)
        yield {
            "type": "PRODUCTLISTING",
            "listing_id": None,
            "competitor_id": None,
            "category_id": category_id,
            "product_url": product_url,
            "title_on_page": clean(title_on_page),
            "image_url_on_page": clean(image_url_on_page),
            "in_stock_on_page": in_stock_on_page,
            "gtin_on_page": clean(gtin_on_page),
            "description_clean": clean(description_clean),
            "listing_key": listing_key,
            "scrape_run_key": self.scrape_run_key,
        }
        return listing_key

    def emit_pricesnapshot(
        self,
        *,
        listing_key: int,
        scraped_at: str,
        current_price,
        base_price,
        discount_amount,
        discount_percent,
        price_text: str | None,
        in_stock: bool | None,
        stock_status_text: str | None,
        stock_label: str | None,  # NEW (YES/NO)
    ):
        yield {
            "type": "PRICESNAPSHOT",
            "price_snapshot_id": None,
            "listing_id": None,
            "scrape_run_id": None,
            "scraped_at": scraped_at,
            "currency": "EUR",
            "current_price": current_price,
            "base_price": base_price,
            "discount_amount": discount_amount,
            "discount_percent": discount_percent,
            "price_text": clean(price_text),
            "in_stock": in_stock,
            "stock_status_text": clean(stock_status_text),
            "stock_label": clean(stock_label),  # NEW FIELD (if your ERD allows it)
            "listing_key": listing_key,
            "scrape_run_key": self.scrape_run_key,
        }

    def emit_review_aggregate(
        self,
        *,
        listing_key: int,
        created_at: str,
        rating_value: float | None,
        rating_scale: int | None,
        review_count: int | None,
        review_url: str | None,
    ):
        if rating_value is None and review_count is None:
            return
        yield {
            "type": "REVIEW",
            "review_id": None,
            "listing_id": None,
            "created_at": created_at,
            "rating_value": rating_value,
            "rating_scale": rating_scale,
            "review_count": review_count,
            "review_text": None,
            "reviewer_name": None,
            "verified": None,
            "verified_purchase": None,
            "review_url": clean(review_url),
            "listing_key": listing_key,
            "scrape_run_key": self.scrape_run_key,
        }

    def emit_productmatch(
        self,
        *,
        product_key: int,
        listing_key: int,
        match_method: str,
        match_score: float,
        matched_at: str,
    ):
        yield {
            "type": "PRODUCTMATCH",
            "match_id": None,
            "product_id": None,
            "listing_id": None,
            "match_method": match_method,
            "match_score": match_score,
            "matched_at": matched_at,
            "product_key": product_key,
            "listing_key": listing_key,
            "scrape_run_key": self.scrape_run_key,
        }

    # -------- crawl entry --------

    def start_requests(self):
        if self.bd_mode == "disabled":
            raise RuntimeError(
                "Bright Data is required. Set either:\n"
                "  - BRIGHTDATA_TOKEN + BRIGHTDATA_ZONE (Unlocker API)\n"
                "or:\n"
                "  - BRIGHTDATA_PROXY / BRIGHTDATA_USERNAME+PASSWORD (+HOST+PORT) (proxy)"
            )

        if self.bd_mode == "proxy" and not self.proxy_url:
            raise RuntimeError(
                "Bright Data proxy mode detected but proxy URL not configured.\n"
                "Set BRIGHTDATA_PROXY='http://user:pass@host:port' OR "
                "BRIGHTDATA_USERNAME/BRIGHTDATA_PASSWORD/BRIGHTDATA_HOST/BRIGHTDATA_PORT"
            )

        yield from self.emit_scraperun()
        yield from self.emit_category(url=self.seed_category_url, name="Microfoons")

        yield scrapy.Request(
            self.seed_category_url,
            callback=self.parse_listing,
            meta=self._base_meta(),
            dont_filter=True,
        )

    # -------- listing parsing --------

    def parse_listing(self, response):
        response = self.maybe_render(response, reason="listing")

        if "bol.com" not in response.url:
            return

        links = response.css('a[data-test="product-title"]::attr(href)').getall()
        if not links:
            links = response.css('a[href*="/nl/nl/p/"]::attr(href)').getall()

        links = [strip_tracking(response.urljoin(h)) for h in links if h]
        links = list(dict.fromkeys(links))

        meta = dict(response.meta)

        for url in links:
            yield response.follow(url, callback=self.parse_product, meta=meta)

        next_url = (
            response.css('a[rel="next"]::attr(href)').get()
            or response.css('a[data-test="pagination-next"]::attr(href)').get()
            or response.css('li.pagination__item--next a::attr(href)').get()
            or response.css('a[aria-label*="Volgende"]::attr(href)').get()
            or response.css('a[aria-label*="Next"]::attr(href)').get()
        )

        if next_url:
            yield response.follow(next_url, callback=self.parse_listing, meta=meta)
        else:
            # Fallback: try incrementing ?page=
            u = urlparse(response.url)
            q = parse_qs(u.query)
            page = int(q.get("page", ["1"])[0])
            q["page"] = [str(page + 1)]
            guessed = urlunparse((u.scheme, u.netloc, u.path, u.params, urlencode(q, doseq=True), ""))
            if page < 120:  # bump this if you want more pages
                yield scrapy.Request(guessed, callback=self.parse_listing, meta=meta)

    # -------- product parsing (STRICT mic-only) --------

    def parse_product(self, response):
        response = self.maybe_render(response, reason="product")
        scraped_at = utc_now_iso()
        product_url = strip_tracking(response.url)
        seed_category_url = response.meta.get("seed_category_url") or self.seed_category_url

        # breadcrumb category check (optional guard)
        crumb_hrefs = response.css(
            'nav a[href]::attr(href), ol a[href]::attr(href), a[data-test*="breadcrumb"]::attr(href)'
        ).getall()
        crumb_urls = [strip_tracking(response.urljoin(h)) for h in crumb_hrefs if h]
        crumb_urls = [u for u in crumb_urls if "/nl/nl/l/" in u]
        if crumb_urls and not any(is_microphone_category_url(u) for u in crumb_urls):
            return

        # ---- JSON-LD extraction (often best signal) ----
        ld_title = None
        ld_desc = None
        ld_brand = None
        ld_model = None
        ld_gtin = None
        ld_availability = None
        ld_image = None
        rating_value_ld = None
        review_count_ld = None

        ld_nodes = []
        for ld in response.css('script[type="application/ld+json"]::text').getall():
            ld = clean(ld)
            if not ld:
                continue
            try:
                data = json.loads(ld)
            except Exception:
                continue
            if isinstance(data, list):
                ld_nodes.extend([x for x in data if isinstance(x, dict)])
            elif isinstance(data, dict):
                ld_nodes.append(data)

        # flatten possible @graph
        flat_nodes = []
        for n in ld_nodes:
            if "@graph" in n and isinstance(n["@graph"], list):
                flat_nodes.extend([x for x in n["@graph"] if isinstance(x, dict)])
            else:
                flat_nodes.append(n)

        # pick first "Product"-like node
        product_node = None
        for n in flat_nodes:
            t = n.get("@type")
            if t in {"Product", "IndividualProduct"} or "gtin" in n or "offers" in n:
                product_node = n
                break

        if product_node:
            ld_title = clean(product_node.get("name"))
            ld_desc = clean(product_node.get("description"))
            ld_gtin = clean(product_node.get("gtin13") or product_node.get("gtin14") or product_node.get("gtin"))

            # image could be str, list[str], OR ImageObject dict
            ld_image = product_node.get("image")
            if isinstance(ld_image, dict):
                ld_image = ld_image.get("url") or ld_image.get("contentUrl")
            elif isinstance(ld_image, list) and ld_image:
                first = ld_image[0]
                if isinstance(first, dict):
                    ld_image = first.get("url") or first.get("contentUrl")
                else:
                    ld_image = first
            ld_image = clean(ld_image)

            b = product_node.get("brand")
            if isinstance(b, dict):
                ld_brand = clean(b.get("name"))
            elif isinstance(b, str):
                ld_brand = clean(b)

            mval = product_node.get("model")
            if isinstance(mval, dict):
                ld_model = clean(mval.get("name") or mval.get("@id") or mval.get("url"))
            elif isinstance(mval, str):
                ld_model = clean(mval)

            # offers: availability only (price handled by extract_price_fields)
            offers = product_node.get("offers")
            if isinstance(offers, list) and offers:
                offers = offers[0]
            if isinstance(offers, dict):
                ld_availability = clean(offers.get("availability"))

            agg = product_node.get("aggregateRating")
            if isinstance(agg, dict):
                rv = agg.get("ratingValue")
                rc = agg.get("reviewCount")
                try:
                    rating_value_ld = float(str(rv).replace(",", ".")) if rv is not None else None
                except Exception:
                    rating_value_ld = None
                try:
                    review_count_ld = int(str(rc).replace(".", "")) if rc is not None else None
                except Exception:
                    review_count_ld = None

        # ---- HTML extraction ----
        title_on_page = clean(
            response.css('h1[data-test="title"]::text').get()
            or response.css("h1::text").get()
            or ld_title
        )

        # HARD FILTER: only keep actual microphones
        if not is_actual_microphone(response, title_on_page):
            return

        # Emit ONLY the seed category
        yield from self.emit_category(url=seed_category_url, name="Microfoons")

        brand = clean(
            response.css('a[data-test="brand-link"]::text').get()
            or response.css('[data-test="brand"]::text').get()
            or response.css('a[href*="/nl/nl/b/"]::text').get()
            or ld_brand
        )

        # description: prefer JSON-LD, else HTML
        description_clean = clean(ld_desc)
        if not description_clean:
            desc_parts = response.css('[data-test="description"] *::text').getall()
            if not desc_parts:
                desc_parts = response.css('section *[class*="description"] *::text').getall()
            description_clean = clean(" ".join([d for d in desc_parts if clean(d)])[:4000])
        # remove the consent/modal garbage if it slipped in
        if description_clean and "modal window" in description_clean.lower():
            description_clean = None

        # image: prefer JSON-LD, else HTML
        image_url_on_page = clean(ld_image) or clean(
            response.css('img[data-test="product-image"]::attr(src)').get()
            or response.css('img[srcset]::attr(src)').get()
            or response.css('img::attr(src)').get()
        )
        if image_url_on_page and image_url_on_page.startswith("//"):
            image_url_on_page = "https:" + image_url_on_page

        # -------- PRICE (ENHANCED) --------
        current_price, base_price, discount_amount, discount_percent, price_text = extract_price_fields(
            response, product_node
        )

        # -------- STOCK (ENHANCED + YES/NO label) --------
        in_stock_on_page, stock_status_text, stock_label = extract_stock_fields(response, ld_availability)

        # reviews aggregate
        rating_scale = 5
        rating_value = rating_value_ld
        review_count = review_count_ld

        if rating_value is None:
            rating_txt = clean(
                response.css('[data-test="rating"]::text').get()
                or response.css('[class*="rating"]::text').get()
            )
            if rating_txt:
                m = re.search(r"(\d+(?:[.,]\d+)?)", rating_txt)
                if m:
                    try:
                        rating_value = float(m.group(1).replace(",", "."))
                    except Exception:
                        rating_value = None

        if review_count is None:
            review_count_txt = clean(
                response.css('[data-test="rating-count"]::text').get()
                or response.css('a[href*="#ratings"]::text').get()
            )
            if review_count_txt:
                m = re.search(r"(\d+)", review_count_txt.replace(".", ""))
                if m:
                    review_count = int(m.group(1))

        # identifiers
        gtin_on_page = clean(ld_gtin)
        model = clean(ld_model)

        # fallbacks from body (ONLY if page looks real; avoid modal garbage)
        body_text = clean(" ".join(response.css("body *::text").getall())) or ""
        if body_text and any(m in body_text.lower() for m in ["modal window", "dialog window"]):
            body_text = ""

        if not gtin_on_page and body_text:
            m = re.search(r"\b(EAN|GTIN)\b\D{0,30}(\d{8,14})\b", body_text, re.IGNORECASE)
            if m:
                gtin_on_page = m.group(2)

        if not model and body_text:
            m = re.search(
                r"\b(Model|Modelnummer|Typenummer)\b\D{0,30}([A-Z0-9][A-Z0-9\-_\/\.]{2,})",
                body_text,
                re.IGNORECASE,
            )
            if m:
                model = m.group(2)

        model = normalize_bad_model(model)

        # canonical name
        canonical_name = (
            canonicalize(brand, title_on_page, model)
            or canonicalize(None, title_on_page, None)
        )

        # -------- emit PRODUCT --------
        product_key = None
        if canonical_name:
            product_key = stable_int_key(canonical_name)
            if product_key not in self._seen_product_key:
                yield from self.emit_product(canonical_name=canonical_name, brand=brand, model=model)

        # -------- emit PRODUCTLISTING --------
        listing_key = stable_int_key(product_url)
        if listing_key not in self._seen_listing_key:
            yield from self.emit_productlisting(
                product_url=product_url,
                title_on_page=title_on_page,
                image_url_on_page=image_url_on_page,
                in_stock_on_page=in_stock_on_page,
                gtin_on_page=gtin_on_page,
                description_clean=description_clean,
                category_id=None,
            )

        # -------- emit PRICESNAPSHOT --------
        yield from self.emit_pricesnapshot(
            listing_key=listing_key,
            scraped_at=scraped_at,
            current_price=current_price,
            base_price=base_price,
            discount_amount=discount_amount,
            discount_percent=discount_percent,
            price_text=price_text,
            in_stock=in_stock_on_page,
            stock_status_text=stock_status_text,
            stock_label=stock_label,  # YES/NO
        )

        # -------- emit REVIEW aggregate placeholder --------
        yield from self.emit_review_aggregate(
            listing_key=listing_key,
            created_at=scraped_at,
            rating_value=rating_value,
            rating_scale=rating_scale,
            review_count=review_count,
            review_url=product_url + "#ratings",
        )

        # -------- emit PRODUCTMATCH --------
        if product_key is not None:
            if gtin_on_page:
                match_method = "gtin"
                match_score = 1.00
            else:
                match_method = "canonical_name"
                match_score = 0.70
            yield from self.emit_productmatch(
                product_key=product_key,
                listing_key=listing_key,
                match_method=match_method,
                match_score=match_score,
                matched_at=scraped_at,
            )
