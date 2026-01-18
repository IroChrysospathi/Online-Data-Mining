"""
bol_support.py

Aligned to your bol_products schema:
- JSONL/JSON file with lines like {"type":"product", ..., "source_url":"https://www.bol.com/.../<id>/"}

Exports:
1) CUSTOMER_SERVICE (as before)  -> type="customer_service"
2) EXPERT_SUPPORT (per listing) -> type="expert_support"

IMPORTANT:
- CUSTOMER_SERVICE item contains ONLY the CUSTOMER_SERVICE columns you specified.
- EXPERT_SUPPORT item contains ONLY the EXPERT_SUPPORT columns required (plus listing_id for alignment).

You will build the DB later; for now we just export JSON items.
"""

import csv
import json
import os
import re
import scrapy
from datetime import datetime, timezone
from urllib.parse import urlparse


# ----------------------------
# Helpers
# ----------------------------

def iso_utc_now():
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

def text_has_any(text, words):
    t = (text or "").lower()
    return any(w.lower() in t for w in words)

def is_bol_domain(url: str) -> bool:
    if not url:
        return False
    try:
        return urlparse(url).netloc.endswith("bol.com")
    except Exception:
        return False

def extract_listing_id_from_url(url: str):
    # .../9300000184016836/  -> "9300000184016836"
    if not url:
        return None
    m = re.search(r"/(\d{8,})/?(?:\?|#|$)", url)
    return m.group(1) if m else None

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

def extract_supportish_text(full_text: str, max_len: int = 4000) -> str | None:
    """
    Keep a compact snippet focused on support-related words.
    This stays product-aligned (comes from the PDP itself), and is defensible for the assignment.
    """
    if not full_text:
        return None

    # Grab sentences/chunks around support keywords (NL + EN)
    keywords = [
        "klantenservice", "contact", "help", "hulp",
        "chat", "bel", "telefonisch", "telefoon",
        "mail", "e-mail", "email", "bericht",
        "retour", "retourneren", "bedenktijd",
        "garantie", "verkoop door", "partner",
    ]
    t = full_text
    tl = t.lower()

    hits = []
    for kw in keywords:
        idx = tl.find(kw)
        if idx != -1:
            start = max(0, idx - 220)
            end = min(len(t), idx + 420)
            hits.append(t[start:end])

    if not hits:
        return t[:max_len]

    snippet = " ... ".join(clean(h) for h in hits if h)
    snippet = clean(snippet) or None
    if snippet and len(snippet) > max_len:
        snippet = snippet[:max_len]
    return snippet


class BolSupportSpider(scrapy.Spider):
    name = "bol_support"
    allowed_domains = ["bol.com"]

    # Let callbacks receive 404s so we can fall back instead of spider ending with 0 items
    handle_httpstatus_list = [404]

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
        "HTTPERROR_ALLOWED_CODES": [404],
    }

    def __init__(self, input_file=None, competitor_id=None, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.input_file = input_file
        self.competitor_id = detect_int(competitor_id) if competitor_id is not None else 1

        # Defaults (bol typically 30 days)
        self.global_free_shipping_threshold_amt = None
        self.global_cooling_off_days = 30

        # Support URLs change often; we try a few and if all fail, we still proceed.
        self.support_seed_urls = [
            "https://www.bol.com/nl/nl/klantenservice/",
            "https://www.bol.com/nl/nl/klantenservice/contact/",
            "https://www.bol.com/nl/nl/klantenservice/retourneren/",
            "https://www.bol.com/nl/nl/klantenservice/bezorgen/",
        ]

        self.product_rows = self._load_products(self.input_file)
        self.logger.info(f"Loaded {len(self.product_rows)} product URLs from input_file={self.input_file}")

    # ----------------------------
    # Input loading (your schema)
    # ----------------------------

    def _load_products(self, path):
        rows = []

        if not path:
            self.logger.error("Missing -a input_file=... (required)")
            return rows

        if not os.path.exists(path):
            self.logger.error(f"input_file not found: {path}")
            return rows

        ext = os.path.splitext(path)[1].lower()

        def add_row(obj):
            if not isinstance(obj, dict):
                return

            # If type exists, keep only products
            if "type" in obj and obj.get("type") != "product":
                return

            url = obj.get("source_url") or obj.get("url") or obj.get("product_url")
            if not url or not is_bol_domain(url):
                return

            listing_id = obj.get("listing_id") or extract_listing_id_from_url(url)
            if not listing_id:
                return

            rows.append({"listing_id": str(listing_id), "url": url})

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

    # ----------------------------
    # Crawl
    # ----------------------------

    def start_requests(self):
        if not self.product_rows:
            self.logger.error("No products loaded. Check input_file (type=product and source_url).")
            return

        yield scrapy.Request(
            self.support_seed_urls[0],
            callback=self.parse_support_then_schedule,
            meta={"support_index": 0},
            dont_filter=True,
        )

    def parse_support_then_schedule(self, response):
        """
        Try to parse global defaults from support pages.
        If this page is 404, try next. If all fail, schedule products anyway.
        """
        idx = response.meta.get("support_index", 0)

        if response.status == 404:
            self.logger.warning(f"Support URL 404: {response.url}")
            next_idx = idx + 1
            if next_idx < len(self.support_seed_urls):
                yield scrapy.Request(
                    self.support_seed_urls[next_idx],
                    callback=self.parse_support_then_schedule,
                    meta={"support_index": next_idx},
                    dont_filter=True,
                )
                return
            self.logger.warning("All support URLs failed. Proceeding with product scraping only.")
        else:
            full_text = clean(" ".join(response.css("body *::text").getall())) or ""

            # free shipping threshold (best-effort)
            m = re.search(
                r"gratis\s+verzending.{0,80}?vanaf\s*€\s*([0-9]+(?:[.,][0-9]{1,2})?)",
                full_text,
                re.IGNORECASE,
            )
            if m:
                self.global_free_shipping_threshold_amt = to_decimal_eur(m.group(1))

            # cooling off days (best-effort)
            m = re.search(r"(\d+)\s*dagen\s*bedenktijd", full_text, re.IGNORECASE)
            if m:
                v = detect_int(m.group(1))
                if v:
                    self.global_cooling_off_days = v

        # IMPORTANT: schedule products no matter what
        for row in self.product_rows:
            yield scrapy.Request(
                row["url"],
                callback=self.parse_product,
                meta={"listing_id": row["listing_id"]},
            )

    # ----------------------------
    # Product page parsing -> CUSTOMER_SERVICE + EXPERT_SUPPORT
    # ----------------------------

    def parse_product(self, response):
        listing_id = response.meta.get("listing_id")
        scraped_at = iso_utc_now()

        full_text = clean(" ".join(response.css("body *::text").getall())) or ""

        # -------- CUSTOMER_SERVICE fields --------

        shipping_included = None
        if text_has_any(full_text, ["gratis verzending", "gratis bezorging", "gratis geleverd"]):
            shipping_included = True
        elif text_has_any(full_text, ["verzendkosten", "bezorgkosten"]):
            shipping_included = False

        free_shipping_threshold_amt = self.global_free_shipping_threshold_amt

        pickup_point_available = None
        if text_has_any(full_text, ["afhaalpunt", "ophaalpunt", "afhalen", "pickup point", "pick-up point"]):
            pickup_point_available = True

        delivery_shipping_available = None
        if text_has_any(full_text, ["bezorgen", "bezorgd", "geleverd", "levertijd", "thuisbezorgd", "morgen in huis"]):
            delivery_shipping_available = True

        delivery_courier_available = None
        if text_has_any(full_text, ["postnl", "dhl", "dpd", "ups", "gls", "bezorger", "koerier"]):
            delivery_courier_available = True

        cooling_off_days = None
        m = re.search(r"(\d+)\s*dagen\s*bedenktijd", full_text, re.IGNORECASE)
        if m:
            cooling_off_days = detect_int(m.group(1))
        if cooling_off_days is None:
            cooling_off_days = self.global_cooling_off_days

        free_returns = None
        if text_has_any(full_text, ["gratis retourneren", "gratis retour", "kosteloos retourneren", "gratis terugsturen"]):
            free_returns = True

        warranty_provider = None
        m = re.search(r"verkoop\s+door\s+([^\|\n\r]+)", full_text, re.IGNORECASE)
        seller_text = None
        if m:
            seller_text = clean(m.group(1))
            if seller_text:
                if "bol.com" in seller_text.lower() or seller_text.lower().strip() in ["bol", "bolcom"]:
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
        hrefs = response.css("a::attr(href)").getall()
        for h in hrefs:
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

        # Emit CUSTOMER_SERVICE item (ONLY your columns)
        yield {
            "type": "customer_service",
            "competitor_id": self.competitor_id,
            "listing_id": listing_id,
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
        }

        # -------- EXPERT_SUPPORT fields (product-aligned heuristic) --------
        # We derive from the PDP itself (seller responsibility + contact/help wording).

        # in_store_support_available: bol has no stores
        in_store_support_available = False

        # If sold by bol, users typically can chat with bol CS; partner listings are often redirected.
        sold_by_bol = False
        if seller_text:
            sold_by_bol = ("bol.com" in seller_text.lower()) or (seller_text.lower().strip() in ["bol", "bolcom"])

        # chat_available:
        # - TRUE if explicitly mentions chat/help AND sold by bol
        # - FALSE if clearly partner and no chat mention
        # - else None
        mentions_chat = text_has_any(full_text, ["chat", "livechat", "chatten", "chat met"])
        chat_available = None
        if sold_by_bol and (mentions_chat or text_has_any(full_text, ["klantenservice", "hulp", "contact"])):
            chat_available = True
        elif (not sold_by_bol) and mentions_chat:
            # sometimes bol still offers chat entry; keep True if we see it
            chat_available = True

        # phone_support_available:
        # bol rarely shows phone numbers publicly; only set True if explicit.
        phone_support_available = None
        if re.search(r"\b(\+31|0)\s?\d{1,3}[\s\-]?\d{3,4}[\s\-]?\d{3,4}\b", full_text) or text_has_any(full_text, ["bel ons", "telefonisch", "telefoon"]):
            phone_support_available = True

        # email_support_available:
        # bol uses forms; treat as available if page mentions contact/message.
        email_support_available = None
        if text_has_any(full_text, ["stuur een bericht", "stuur ons een bericht", "contactformulier", "e-mail", "email", "mail ons"]):
            email_support_available = True
        elif sold_by_bol and text_has_any(full_text, ["klantenservice", "contact"]):
            # If sold by bol and there is a general contact/help section, assume message-based support.
            email_support_available = True

        # expert_support_text: PDP snippet around support keywords
        expert_support_text = extract_supportish_text(full_text, max_len=4000)

        # Emit EXPERT_SUPPORT item (ONLY required fields + listing_id for alignment)
        yield {
            "type": "expert_support",
            "listing_id": listing_id,
            "scraped_at": scraped_at,
            "chat_available": chat_available,
            "phone_support_available": phone_support_available,
            "email_support_available": email_support_available,
            "in_store_support_available": in_store_support_available,
            "expert_support_text": expert_support_text,
        }


