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
# thomann_support.py

import json
import os
import re
import hashlib
from datetime import datetime, timezone
from urllib.parse import urlparse

import scrapy


# Competitor metadata (fixed IDs for ERD consistency)
COMPETITOR_ID = 4
COMPETITOR_NAME = "Thomann"


# Helpers

def iso_utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def clean(text):
    if text is None:
        return None
    return re.sub(r"\s+", " ", str(text)).strip() or None


def visible_body_text(response) -> str:
    """
    Returns visible text from <body>, excluding script/style/noscript.
    Prevents GTM/dataLayer JS from polluting extracted text.
    """
    parts = response.xpath(
        "//body//*[not(self::script) and not(self::style) and not(self::noscript)]/text()"
    ).getall()
    return clean(" ".join(parts)) or ""


def has_delivery_courier(response) -> bool:
    """
    Detects couriers specifically for delivery_courier_available.
    Checks:
      1) visible body text (e.g. 'DHL:' next to logo)
      2) alt/title/aria-label attributes (e.g. courier logos)
    Returns True/False (never None).
    """
    couriers = ["dhl", "ups", "ups express", "dpd", "gls", "fedex"]

    # 1) Visible text
    text = (visible_body_text(response) or "").lower()
    if any(c in text for c in couriers):
        return True

    # 2) Attributes (logo alt/title/aria-label)
    attrs = " ".join(
        response.xpath("//@alt | //@title | //@aria-label").getall()
    ).lower()
    if any(c in attrs for c in couriers):
        return True

    return False


def is_cookie_consent_text(text: str) -> bool:
    """
    Heuristic filter: if the extracted text is primarily cookie/consent content,
    do not store it as expert_support_text (set it to None).
    """
    if not text:
        return False
    t = text.lower()
    return any(
        phrase in t
        for phrase in [
            "met onze cookies",
            "cookies",
            "cookiebeleid",
            "cookie policy",
            "cookie-instellingen",
            "cookie instellingen",
            "gebruik van cookies",
            "privacy-instellingen",
            "privacy instellingen",
            "toestemming",
            "consent",
            "akkoord",
            "accept all",
            "alles accepteren",
        ]
    )


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


def is_thomann_domain(url: str) -> bool:
    if not url:
        return False
    try:
        return urlparse(url).netloc.endswith("thomann.nl")
    except Exception:
        return False


# Bright Data proxy handling

def brightdata_proxy_url():
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


def looks_like_shell_or_blocked_html(html):
    if not html:
        return True
    low = html.lower()
    if any(m in low for m in BLOCKED_MARKERS):
        return True
    if len(low) < 20_000:
        return True
    return False


def selenium_enabled():
    return str(os.getenv("USE_SELENIUM", "")).strip().lower() in {"1", "true", "yes", "y", "on"}


def render_with_selenium(url: str, wait_seconds: int = 6) -> str:
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
            wait.until(lambda d: (len(d.find_elements(By.CSS_SELECTOR, "body")) > 0 and len(d.page_source) > 30_000))
        except Exception:
            pass

        time.sleep(0.8)
        return driver.page_source
    finally:
        driver.quit()


# Spider

class ThomannSupportSpider(scrapy.Spider):
    name = "thomann_support"
    allowed_domains = ["thomann.nl"]
    handle_httpstatus_list = [404]

    HELPdesk_SHIPPING_URL = "https://www.thomann.nl/helpdesk_shipping.html"
    CONTACT_URL = "https://www.thomann.nl/compinfo_contact.html"

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

    def __init__(self, input_file=None, selenium_wait=6, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.input_file = input_file

        try:
            self.selenium_wait = int(selenium_wait)
        except Exception:
            self.selenium_wait = 6

        # Bright Data is mandatory
        self.proxy_url = brightdata_proxy_url()
        if not self.proxy_url:
            raise RuntimeError(
                "Bright Data proxy is mandatory but not configured. "
                "Set BRIGHTDATA_PROXY or BRIGHTDATA_USERNAME/BRIGHTDATA_PASSWORD/BRIGHTDATA_HOST/BRIGHTDATA_PORT."
            )

        # Global values scraped once
        self.global_customer_service = {
            "free_shipping_threshold_amt": None,
            "delivery_courier_available": None,  # will be set True/False on helpdesk page
        }
        self.global_expert_support = {
            "expert_chat_available": None,
            "phone_support_available": None,
            "email_support_available": None,
            "expert_support_text": None,
            "customer_service_url": self.CONTACT_URL,
        }

        self.product_rows = self._load_products(self.input_file)
        self.logger.info("Loaded %s product rows from input_file=%s", len(self.product_rows), self.input_file)

    def _base_meta(self):
        return {"proxy": self.proxy_url}

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

    def _load_products(self, path):
        """
        Accepts JSONL/JSON produced by products spider.
        Expected fields per row: type='product', source_url, listing_id.
        """
        rows = []
        if not path:
            self.logger.error("Missing -a input_file=... (required)")
            return rows

        if not os.path.exists(path):
            self.logger.error("input_file not found: %s", path)
            return rows

        raw = open(path, "r", encoding="utf-8").read().strip()
        if not raw:
            return rows

        def add_obj(obj):
            if not isinstance(obj, dict):
                return
            if obj.get("type") != "product":
                return

            url = obj.get("source_url")
            if not url or not is_thomann_domain(url):
                return

            listing_id = obj.get("listing_id")
            rows.append(
                {
                    "product_url": url.strip(),
                    "listing_id": listing_id,
                    "listing_key": stable_int_key(url.strip()),
                }
            )

        # JSON array
        if raw.startswith("["):
            try:
                data = json.loads(raw)
            except Exception as e:
                self.logger.error("Failed to parse JSON array: %s", e)
                return rows
            if isinstance(data, list):
                for obj in data:
                    add_obj(obj)
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
            add_obj(obj)

        return rows

    # Crawl

    def start_requests(self):
        if not self.product_rows:
            self.logger.error("No product URLs loaded. Use -a input_file=... with type=product lines.")
            return

        yield scrapy.Request(
            self.HELPdesk_SHIPPING_URL,
            callback=self.parse_helpdesk_shipping,
            meta=self._base_meta(),
            dont_filter=True,
        )

    def parse_helpdesk_shipping(self, response):
        response = self.maybe_render(response)

        if response.status == 404:
            self.logger.warning("helpdesk_shipping returned 404: %s", response.url)
        else:
            full_text = visible_body_text(response)

            m = re.search(
                r"(?:geen\s+verzendkosten|gratis\s+verzending).{0,160}?\bvanaf\b.{0,40}?€\s*([0-9]+(?:[.,][0-9]{1,2})?)",
                full_text,
                flags=re.IGNORECASE,
            )
            if m:
                self.global_customer_service["free_shipping_threshold_amt"] = to_decimal_eur(m.group(1))

            # ONLY change: make delivery_courier_available robust (text + attributes)
            self.global_customer_service["delivery_courier_available"] = has_delivery_courier(response)

        yield scrapy.Request(
            self.CONTACT_URL,
            callback=self.parse_contact,
            meta=self._base_meta(),
            dont_filter=True,
        )

    def parse_contact(self, response):
        response = self.maybe_render(response)

        if response.status == 404:
            self.logger.warning("contact page returned 404: %s", response.url)
        else:
            full_text = visible_body_text(response)

            if text_has_any(full_text, ["chat", "chat nu", "chatten", "chat met"]):
                self.global_expert_support["expert_chat_available"] = True

            mailtos = response.css("a[href^='mailto:']::attr(href)").getall()
            if mailtos or re.search(r"\b[a-z0-9._%+\-]+@thomann\.[a-z]{2,}\b", full_text, re.IGNORECASE):
                self.global_expert_support["email_support_available"] = True

            if re.search(r"\+\d{1,3}[\s\-]?\d{1,4}[\s\-]?\d{2,4}[\s\-]?\d{2,6}", full_text):
                self.global_expert_support["phone_support_available"] = True

            # store expert_support_text, but null it if it's cookie/consent text
            expert_text = clean(full_text[:4000])
            if expert_text and "datalayer" in expert_text.lower():
                expert_text = None
            if is_cookie_consent_text(expert_text):
                expert_text = None
            self.global_expert_support["expert_support_text"] = expert_text

        for row in self.product_rows:
            yield scrapy.Request(
                row["product_url"],
                callback=self.parse_product,
                meta={**self._base_meta(), **row},
                dont_filter=True,
            )

    def parse_product(self, response):
        response = self.maybe_render(response)

        scraped_at = iso_utc_now()
        product_url = response.meta.get("product_url") or response.url
        listing_key = response.meta.get("listing_key") or stable_int_key(product_url)
        listing_id = response.meta.get("listing_id")

        full_text = visible_body_text(response)

        shipping_included = None
        if text_has_any(full_text, ["standaard levering"]) and text_has_any(full_text, ["gratis"]):
            shipping_included = True
        elif text_has_any(full_text, ["verzendkosten", "bezorgkosten"]):
            shipping_included = False

        delivery_shipping_available = True if text_has_any(
            full_text, ["levering binnen", "levertijd", "werkdagen", "direct leverbaar"]
        ) else None

        cooling_off_days = None
        m = re.search(r"(\d+)\s*dagen\s*(?:money-?back|moneyback|bedenktijd)", full_text, re.IGNORECASE)
        if m:
            try:
                cooling_off_days = int(m.group(1))
            except Exception:
                cooling_off_days = None

        warranty_provider = None
        warranty_duration_months = None

        m = re.search(r"(\d+)\s*(?:jaar|jaren)\s+thomann\s+garantie", full_text, re.IGNORECASE)
        if m:
            try:
                years = int(m.group(1))
                warranty_provider = COMPETITOR_NAME
                warranty_duration_months = years * 12
            except Exception:
                pass
        else:
            m = re.search(r"\bdrie\s+jaar\s+thomann\s+garantie\b", full_text, re.IGNORECASE)
            if m:
                warranty_provider = COMPETITOR_NAME
                warranty_duration_months = 36

        customer_service_url = None
        for h in response.css("a::attr(href)").getall():
            if not h:
                continue
            u = response.urljoin(h)
            if not is_thomann_domain(u):
                continue
            ul = u.lower()
            if "helpdesk_shipping" in ul or "helpdesk" in ul or "compinfo_contact" in ul:
                customer_service_url = u
                break
        if customer_service_url is None:
            customer_service_url = self.global_expert_support.get("customer_service_url") or self.CONTACT_URL

        yield {
            "type": "CUSTOMER_SERVICE",
            "competitor_id": COMPETITOR_ID,
            "competitor_name": COMPETITOR_NAME,
            "listing_id": listing_id,
            "scraped_at": scraped_at,
            "shipping_included": shipping_included,
            "free_shipping_threshold_amt": self.global_customer_service.get("free_shipping_threshold_amt"),
            "pickup_point_available": None,
            "delivery_shipping_available": delivery_shipping_available,
            "delivery_courier_available": self.global_customer_service.get("delivery_courier_available"),
            "cooling_off_days": cooling_off_days,
            "free_returns": None,
            "warranty_provider": warranty_provider,
            "warranty_duration_months": warranty_duration_months,
            "customer_service_url": customer_service_url,
            "listing_key": listing_key,
            "product_url": product_url,
        }

        yield {
            "type": "EXPERT_SUPPORT",
            "competitor_id": COMPETITOR_ID,
            "competitor_name": COMPETITOR_NAME,
            "listing_id": listing_id,
            "scraped_at": scraped_at,
            "source_url": product_url,
            "expert_chat_available": self.global_expert_support.get("expert_chat_available"),
            "phone_support_available": self.global_expert_support.get("phone_support_available"),
            "email_support_available": self.global_expert_support.get("email_support_available"),
            "in_store_support": False,
            "expert_support_text": self.global_expert_support.get("expert_support_text"),
            "listing_key": listing_key,
        }
