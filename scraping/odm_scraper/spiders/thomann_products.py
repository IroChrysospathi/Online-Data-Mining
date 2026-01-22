"""
Product spider.

Responsibilities:
- Crawl product listing and product detail pages for a specific webshop
- Extract relevant product information (e.g., name, price, URL)
- Yield structured product data for further processing
"""
# thomann_products.py
# NOTE: This version uses Selenium ONLY for listing pagination ("Toon meer").
# Product pages remain pure Scrapy (fast + stable).
#
# What I removed as dead code (no longer needed):
# - self._pages_seen_per_listing
# - self.max_pages_per_listing
# - pg= pagination logic
# - set_query_param(...) (because we no longer force ls=100 and no longer do pg=)

import json
import os
import re
import time
import uuid
import subprocess
from datetime import datetime, timezone
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode

import scrapy
from scrapy.selector import Selector

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException
from webdriver_manager.chrome import ChromeDriverManager


# Competitor metadata (fixed IDs for ERD consistency)
# I keep competitor IDs fixed across all spiders (Bax, Bol, Thomann),
# so the database can join and compare competitors reliably.
COMPETITOR_ID = 4
COMPETITOR_NAME = "Thomann"


# Seed-page subcategories (captured from the Microphones page UI)
# This list comes from the subcategory list shown under:
# https://www.thomann.nl/microfoons.html
ALLOWED_SUBCATEGORY_NAMES_SEED = {
    "zangmicrofoons",
    "instrumentenmicrofoons",
    "grootmembraan microfoons",
    "kleinmembraan condensatormicrofoons",
    "ribbon zangmicrofoons",
    "draadloosmicrofoons",
    "headset-microfoons",
    "lavalier-microfoons",
    "stereo-microfoons",
    "zendermicrofoons",
    "video- en cameramicrofoons",
    "reporter microfoons",
    "usb/podcast microfoons",
    "grensvlakmicrofoons",
    "microfoons voor installatie",
    "meetmicrofoons",
    "broadcast koptelefoons",
    "microfoonsets",
    "speciaal microfoon",
    "ovid serie",
}

# I explicitly exclude microphone accessories based on the Thomann UI structure
EXCLUDED_CATEGORY_KEYWORDS = {
    "accessoire", "accessoires", "microfoonstandaard", "microfoonstandaards",
    "statief", "statieven", "tafelstatief", "tafelstatieven",
    "houder", "houders", "beugel", "beugels",
    "zwanenhals", "zwanenhalzen", "microfoonhengel", "microfoonhengels",
    "kabel", "kabels", "aansluitkabel", "aansluitkabels",
    "shockmount", "shockmounts", "windbescherming",
    "popfilter", "popfilters", "phantom", "voeding",
    "adapter", "adapters", "koffer", "koffers",
    "tas", "tassen", "vervangingsonderdelen", "onderdelen",
}


# -------------------------
# Helper functions
# -------------------------

def iso_utc_now() -> str:
    # I store timestamps in UTC for consistent comparisons across runs.
    return datetime.now(timezone.utc).isoformat()


def clean(s):
    # Small helper to normalize whitespace and convert empty strings to None.
    if s is None:
        return None
    s = re.sub(r"\s+", " ", str(s)).strip()
    return s or None


def normalize_category_label(s: str) -> str:
    # I normalize category labels so small differences (hyphens/spaces/case/counts) do not break matching.
    if not s:
        return ""
    s = (clean(s) or "").lower()
    # Remove trailing counts like "(123)"
    s = re.sub(r"\s*\(\s*\d+\s*\)\s*$", "", s)
    # Normalize common separators
    s = s.replace("-", " ").replace("/", " ")
    # Collapse whitespace
    s = re.sub(r"\s+", " ", s).strip()
    return s


# I keep a normalized allowlist for robust seed-page matching.
ALLOWED_SUBCATEGORY_NAMES_SEED_NORM = {normalize_category_label(x) for x in ALLOWED_SUBCATEGORY_NAMES_SEED}


def canonicalize_url_keep_meaning(url: str) -> str:
    """
    I remove typical tracking parameters (utm, ref, etc.) but keep functional
    query parameters. This is safer than removing the entire query string.
    """
    if not url:
        return url
    try:
        u = urlparse(url)
        q = parse_qsl(u.query, keep_blank_values=True)

        drop_prefixes = ("utm_",)
        drop_keys = {
            "ref", "referrer", "source", "spm", "gclid",
            "fbclid", "yclid", "mc_eid", "mc_cid", "cmp", "cmpid",
        }

        q2 = []
        for k, v in q:
            lk = (k or "").lower()
            if any(lk.startswith(p) for p in drop_prefixes):
                continue
            if lk in drop_keys:
                continue
            q2.append((k, v))

        query = urlencode(q2, doseq=True)
        path = (u.path or "").rstrip("/")

        return urlunparse((u.scheme, u.netloc, path, "", query, ""))
    except Exception:
        return url


def strip_tracking(url: str) -> str:
    # Wrapper so the same canonicalization is used everywhere.
    return canonicalize_url_keep_meaning(url)


def price_to_float(text):
    # Convert EU price notation to float (e.g., "€ 1.299,00" -> 1299.00).
    if text is None:
        return None
    t = re.sub(r"[^\d,\.]", "", str(text))
    if not t:
        return None
    if "," in t:
        t = t.replace(".", "").replace(",", ".")
    try:
        return float(t)
    except Exception:
        return None


def sane_price(p):
    # I filter out obviously wrong values (e.g., IDs or script numbers).
    if p is None:
        return None
    try:
        p = float(p)
    except Exception:
        return None
    if p < 1 or p > 50000:
        return None
    return p


def get_git_commit_hash():
    # I store the git commit hash so the dataset is reproducible.
    try:
        out = subprocess.check_output(["git", "rev-parse", "HEAD"], stderr=subprocess.DEVNULL)
        return out.decode("utf-8", errors="ignore").strip() or None
    except Exception:
        return None


def iter_json_ld(obj):
    # Recursively iterate JSON-LD nodes, including @graph structures.
    if isinstance(obj, dict):
        yield obj
        g = obj.get("@graph")
        if isinstance(g, list):
            for x in g:
                yield from iter_json_ld(x)
    elif isinstance(obj, list):
        for x in obj:
            yield from iter_json_ld(x)


def looks_like_category_url(url: str) -> bool:
    # On Thomann, categories typically end with .html (products with .htm).
    if not url:
        return False
    u = urlparse(url)
    path = (u.path or "").lower()
    return path.endswith(".html") and not path.endswith(".htm")


def is_product_url(url: str) -> bool:
    # Product pages usually end with .htm. I also exclude known info pages.
    if not url:
        return False
    u = urlparse(url)
    path = (u.path or "").lower()

    if not path.endswith(".htm"):
        return False

    bad_parts = [
        "/compinfo", "compinfo_", "accessibility", "whistleblower",
        "privacy", "impressum", "terms", "agb", "datenschutz", "kontakt", "about",
    ]
    if any(bp in path for bp in bad_parts):
        return False

    return True


def should_follow_url(url: str) -> bool:
    # Central allow/deny filter for URLs I will crawl.
    if not url:
        return False
    u = urlparse(url)
    if not u.netloc.endswith("thomann.nl"):
        return False

    path = (u.path or "").lower()

    # Skip assets
    if any(path.endswith(ext) for ext in (".pdf", ".jpg", ".jpeg", ".png", ".webp", ".svg", ".zip")):
        return False

    # Skip clearly irrelevant sections
    bad = [
        "/cart", "/checkout", "/login", "/account", "/wishlist", "/compare",
        "/compinfo", "compinfo_", "accessibility", "whistleblower",
    ]
    if any(x in path for x in bad):
        return False

    # Accept only category (.html) and product (.htm)
    return path.endswith(".html") or path.endswith(".htm")


def extract_listing_id_from_html(html: str):
    """
    On Thomann, the "artikelnummer" is very stable.
    I use it as listing_id to keep IDs consistent across competitors.
    """
    if not html:
        return None

    m = re.search(r"artikelnummer\s*[:#]?\s*(\d{5,})", html, flags=re.IGNORECASE)
    if m:
        return m.group(1)

    # Fallback: sometimes the product ID appears in image URLs
    m = re.search(r"/prod/(\d{5,})\.(?:jpg|jpeg|png)", html, flags=re.IGNORECASE)
    if m:
        return m.group(1)

    return None


def normalize_bad_model(model):
    # Light model normalization (avoid obviously wrong "models").
    if not model:
        return None
    m = clean(model)
    if not m or len(m) > 80:
        return None
    return m


def canonicalize_name(brand, title, model):
    # Canonical name helps matching the same product across competitors.
    parts = [clean(brand), clean(title), clean(model)]
    parts = [p for p in parts if p]
    if not parts:
        return None
    s = " ".join(parts).lower()
    s = re.sub(r"[^a-z0-9]+", " ", s).strip()
    s = re.sub(r"\s+", " ", s)
    return s or None


def page_looks_like_product(response) -> bool:
    """
    Heuristic: treat as product if we see a stable artikelnummer
    or JSON-LD Product.
    """
    if extract_listing_id_from_html(response.text):
        return True

    blocks = response.css('script[type="application/ld+json"]::text').getall()
    for b in blocks:
        b = (b or "").strip()
        if not b:
            continue
        try:
            data = json.loads(b)
        except Exception:
            continue
        for n in iter_json_ld(data):
            if not isinstance(n, dict):
                continue
            t = n.get("@type")
            if t == "Product" or (isinstance(t, list) and "Product" in t):
                return True

    return False


def extract_itemlist_product_urls_from_selector(sel: Selector, base_url: str):
    """
    Extract product URLs from JSON-LD ItemList. Works on a Scrapy Selector built from Selenium HTML.
    """
    urls = []
    blocks = sel.css('script[type="application/ld+json"]::text').getall()
    for b in blocks:
        b = (b or "").strip()
        if not b:
            continue
        try:
            data = json.loads(b)
        except Exception:
            continue

        for n in iter_json_ld(data):
            if not isinstance(n, dict):
                continue
            t = n.get("@type")
            if not (t == "ItemList" or (isinstance(t, list) and "ItemList" in t)):
                continue

            elems = n.get("itemListElement")
            if not isinstance(elems, list):
                continue

            for el in elems:
                if not isinstance(el, dict):
                    continue
                item = el.get("item")
                candidate = None
                if isinstance(item, dict):
                    candidate = item.get("@id") or item.get("url")
                elif isinstance(item, str):
                    candidate = item
                candidate = candidate or el.get("url")

                if candidate:
                    full = strip_tracking(scrapy.utils.url.urljoin_rfc(base_url, candidate))
                    if should_follow_url(full) and is_product_url(full):
                        urls.append(full)

    return list(dict.fromkeys(urls))


def extract_price_from_meta(response):
    # First fallback after JSON-LD: meta tags / itemprop price.
    candidates = response.css(
        'meta[itemprop="price"]::attr(content), '
        'meta[property="product:price:amount"]::attr(content), '
        'meta[property="og:price:amount"]::attr(content), '
        '[itemprop="price"]::attr(content), [itemprop="price"]::text'
    ).getall()

    for c in candidates:
        p = sane_price(price_to_float(c))
        if p is not None:
            return p, clean(c)

    return None, None


def extract_price_from_buybox(response):
    """
    Parse prices from a limited DOM block (NOT full body text),
    to avoid picking up random numbers from scripts or unrelated content.
    Note: This should be used only to recover current_price, not to infer base_price.
    """
    block = response.css(
        'div[class*="price"], div[id*="price"], div[class*="buy"], '
        'section[class*="price"], section[class*="buy"]'
    )
    text = clean(" ".join(block.css("*::text").getall())) if block else None
    if not text:
        return None

    euro_vals = re.findall(r"€\s*\d[\d\.\s]*[,\.\d]{0,3}\d", text)
    floats = [sane_price(price_to_float(x)) for x in euro_vals]
    floats = [x for x in floats if x is not None]
    if not floats:
        return None

    floats_unique = sorted(set(floats))
    # Use the lowest value as the most likely current price.
    return floats_unique[0]


def extract_reference_price_30day(response):
    """
    Thomann shows discounts as:
      - badge: "-11%"
      - reference label: "30-Dagen-Beste-Prijs: € 449"

    This extracts that reference price (so we can compute discount_amount/percent).
    """
    block = response.css(
        'div[class*="price"], div[id*="price"], div[class*="buy"], '
        'section[class*="price"], section[class*="buy"], '
        'div[class*="price-and-availability"], div[class*="availability"]'
    )
    text = clean(" ".join(block.css("*::text").getall())) if block else None
    if not text:
        return None

    m = re.search(
        r"30\s*-\s*Dagen\s*-\s*Beste\s*-\s*Prijs\s*:\s*€\s*([\d\.\s]+(?:,\d{1,2})?)",
        text,
        re.IGNORECASE,
    )
    if not m:
        m = re.search(
            r"30\s*Dagen\s*Beste\s*Prijs\s*:\s*€\s*([\d\.\s]+(?:,\d{1,2})?)",
            text,
            re.IGNORECASE,
        )

    if not m:
        return None

    return sane_price(price_to_float(m.group(1)))


def extract_breadcrumb_from_html(response):
    """
    Fallback: extract breadcrumb from visible HTML navigation.

    Returns:
        (category_name, category_url, parent_name)
    """
    links = response.css('nav[aria-label*="breadcrumb"] a::attr(href)').getall()
    names = response.css('nav[aria-label*="breadcrumb"] a::text').getall()

    if not links or not names:
        links = response.css('nav.breadcrumb a::attr(href), .breadcrumb a::attr(href)').getall()
        names = response.css('nav.breadcrumb a::text, .breadcrumb a::text').getall()

    if not links or not names:
        links = response.xpath(
            '//nav[contains(translate(@aria-label, "BREADCRUMB", "breadcrumb"), "breadcrumb")]//a/@href'
        ).getall()
        names = response.xpath(
            '//nav[contains(translate(@aria-label, "BREADCRUMB", "breadcrumb"), "breadcrumb")]//a//text()'
        ).getall()

    links = [clean(response.urljoin(x)) for x in links if clean(x)]
    names = [clean(x) for x in names if clean(x)]

    pairs = [(n, u) for n, u in zip(names, links) if u and looks_like_category_url(u)]
    if not pairs:
        return None, None, None

    cat_name, cat_url = pairs[-1]
    parent_name = pairs[-2][0] if len(pairs) >= 2 else None
    return cat_name, cat_url, parent_name


def extract_breadcrumb_from_microdata(response):
    """
    Fallback: extract breadcrumb from schema.org microdata (HTML).

    Returns:
        (category_name, category_url, parent_name)
    """
    items = response.css(
        'ol[itemtype*="schema.org/BreadcrumbList"] '
        'li[itemtype*="schema.org/ListItem"]'
    )

    crumbs = []
    for li in items:
        name = clean(" ".join(li.css('[itemprop="name"]::text').getall()))
        href = li.css('[itemprop="item"]::attr(href)').get()
        if name and href:
            crumbs.append((name, clean(response.urljoin(href))))

    if not crumbs:
        return None, None, None

    cat_name, cat_url = crumbs[-1]
    parent_name = crumbs[-2][0] if len(crumbs) >= 2 else None
    return cat_name, cat_url, parent_name


def extract_stock_from_html(response):
    """
    Extract Thomann stock status from HTML (server-side, no Selenium needed).

    Returns:
      (stock_text, in_stock_bool_or_None)
    """
    txt = response.css("span.fx-availability::text").get()
    if txt:
        txt = clean(txt)
        cls = response.css("span.fx-availability::attr(class)").get("") or ""

        if "in-stock" in cls:
            return txt, True
        if "out-of-stock" in cls:
            return txt, False

        low = (txt or "").lower()
        if "direct leverbaar" in low or "op voorraad" in low or "in voorraad" in low:
            return txt, True
        if "niet leverbaar" in low or "niet op voorraad" in low or "uitverkocht" in low:
            return txt, False

        return txt, None

    href = response.css('link[itemprop="availability"]::attr(href)').get()
    if href:
        if "InStock" in href:
            return "InStock", True
        if "OutOfStock" in href:
            return "OutOfStock", False

    return None, None


# -------------------------
# Spider
# -------------------------

class ThomannProductsSpider(scrapy.Spider):
    name = "thomann_products"
    allowed_domains = ["thomann.nl"]

    # I explicitly start at the Microphones category root (hub page with tiles).
    start_urls = ["https://www.thomann.nl/microfoons.html"]

    # I crawl subcategories on depth 0,1,2 and stop at depth 3.
    max_category_depth = 3

    custom_settings = {
        "ROBOTSTXT_OBEY": False,
        "COOKIES_ENABLED": True,
        "DOWNLOAD_DELAY": 3.0,
        "RANDOMIZE_DOWNLOAD_DELAY": True,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": 0.5,
        "AUTOTHROTTLE_MAX_DELAY": 60.0,
        "CONCURRENT_REQUESTS": 1,
        "USER_AGENT": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0 Safari/537.36"
        ),
        "LOG_LEVEL": "INFO",
    }

    crawler_version = "thomann_products/json+selenium_listing"

    # Scrapy calls parse() by default for start_urls (when start_requests not overridden)
    # We keep it anyway (you wanted parse_any retained).
    def parse(self, response):
        yield from self.parse_any(response)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.scrape_run_id = str(uuid.uuid4())
        self.started_at = iso_utc_now()
        self.git_commit_hash = get_git_commit_hash()
        self._seed_subcats_emitted = False

        # Selenium driver setup (used only for listing expansion)
        self.driver = self._build_selenium_driver()

    def closed(self, reason):
        # Ensure Selenium driver is always closed when the spider finishes or crashes.
        try:
            if getattr(self, "driver", None):
                self.driver.quit()
        except Exception:
            pass

    def _build_selenium_driver(self):
        """
        Build a Chrome Selenium driver.
        """
        chrome_options = Options()

        # Headless is usually faster/stabler for long crawls.
        # Set HEADLESS=0 if you want to watch it.
        headless = os.environ.get("HEADLESS", "1").strip() != "0"
        if headless:
            chrome_options.add_argument("--headless=new")

        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1400,900")

        proxy = os.environ.get("BRIGHTDATA_PROXY")
        if proxy:
            chrome_options.add_argument(f"--proxy-server={proxy}")

        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=chrome_options)
        driver.set_page_load_timeout(60)
        return driver

    def selenium_expand_toon_meer(self, url: str, max_clicks: int = 500) -> str:
        """
        Open a Thomann listing page and repeatedly click the "Toon meer" button
        until no more products are loaded.

        Returns the fully rendered HTML after all products are visible.
        """
        self.logger.info("SELENIUM OPEN %s", url)
        self.driver.get(url)

        # Best effort: accept cookie / consent if it appears
        # This prevents Selenium from seeing an empty or blocked product listing
        try:
            btn = self.driver.find_elements(By.CSS_SELECTOR, "#onetrust-accept-btn-handler")
            if btn:
                btn[0].click()
                time.sleep(1.0)
        except Exception:
            pass

        wait = WebDriverWait(self.driver, 25)

        # Wait until either:
        # - product links are visible
        # - OR the "Toon meer" button exists
        try:
            wait.until(lambda d: (
                len(d.find_elements(By.CSS_SELECTOR, "a[href*='.htm']")) > 0
                or len(d.find_elements(By.CSS_SELECTOR, "button.search-pagination__show-more")) > 0
            ))
        except TimeoutException:
            title = self.driver.title
            current_url = self.driver.current_url
            html_head = self.driver.page_source[:1500].replace("\n", " ")

            self.logger.error(
                "SELENIUM TIMEOUT | title=%r | url=%s | html_head=%s",
                title,
                current_url,
                html_head,
            )
            return self.driver.page_source

        time.sleep(1.0)

        clicks = 0
        while clicks < max_clicks:
            buttons = self.driver.find_elements(By.CSS_SELECTOR, "button.search-pagination__show-more")
            if not buttons:
                break

            button = buttons[0]
            self.driver.execute_script("arguments[0].scrollIntoView({block:'center'});", button)
            time.sleep(0.4)

            before_count = len(self.driver.find_elements(By.CSS_SELECTOR, "a[href*='.htm']"))

            try:
                button.click()
            except Exception:
                self.driver.execute_script("arguments[0].click();", button)

            clicks += 1

            try:
                wait.until(lambda d: len(d.find_elements(By.CSS_SELECTOR, "a[href*='.htm']")) > before_count)
            except TimeoutException:
                break

            time.sleep(0.4)

        self.logger.info("SELENIUM EXPAND DONE | clicks=%s | url=%s", clicks, url)
        return self.driver.page_source

    def start_requests(self):
        # Emit one "run" record with metadata for reproducibility.
        yield {
            "type": "run",
            "scrape_run_id": self.scrape_run_id,
            "started_at": self.started_at,
            "git_commit_hash": self.git_commit_hash,
            "crawler_version": self.crawler_version,
            "competitor_id": COMPETITOR_ID,
            "competitor_name": COMPETITOR_NAME,
            "seed_url": self.start_urls[0],
        }

        yield scrapy.Request(
            strip_tracking(self.start_urls[0]),
            callback=self.parse_any,
            meta={"cat_depth": 0},
        )

    def parse_any(self, response):
        url = strip_tracking(response.url)
        if not should_follow_url(url):
            return

        if page_looks_like_product(response) and is_product_url(url):
            yield from self.parse_product(response)
            return

        yield from self.parse_listing(response)

    def parse_listing(self, response):
        """
        Listing crawl strategy:
        - Discover & crawl subcategories (Scrapy)
        - Extract product URLs from the listing:
          -> Use Selenium to click "Toon meer" until all items are loaded
          -> Then parse the final rendered HTML with Scrapy Selector
        """
        depth = int(response.meta.get("cat_depth", 0))
        self.logger.info("LISTING depth=%s %s", depth, response.url)

        # Discover subcategories from the current listing/hub page
        subs = list(self.find_subcategory_urls(response, depth=depth))
        self.logger.info("SUBCATS found=%s cat_depth=%s url=%s", len(subs), depth, response.url)
        if subs:
            self.logger.info("SUBCATS sample=%s", subs[:10])

        # Emit seed subcategory URLs once
        if depth == 0 and not self._seed_subcats_emitted:
            self._seed_subcats_emitted = True
            seed = strip_tracking(response.url)
            for sub in subs:
                yield {
                    "type": "subcategory",
                    "competitor_id": COMPETITOR_ID,
                    "competitor_name": COMPETITOR_NAME,
                    "scrape_run_id": self.scrape_run_id,
                    "seed_url": seed,
                    "subcategory_url": strip_tracking(sub),
                }

        # Crawl deeper subcategories (up to max depth)
        if depth < self.max_category_depth:
            for sub in subs:
                u = strip_tracking(sub)
                yield scrapy.Request(
                    u,
                    callback=self.parse_listing,
                    meta={"cat_depth": depth + 1},
                )

        # Selenium expansion for this listing page
        expanded_html = self.selenium_expand_toon_meer(strip_tracking(response.url))
        sel = Selector(text=expanded_html)

        # Prefer JSON-LD ItemList if present in the expanded DOM
        product_urls = extract_itemlist_product_urls_from_selector(sel, base_url=response.url)

        # Fallback: anchors in the expanded DOM
        if not product_urls:
            hrefs = sel.css("a[href*='.htm']::attr(href)").getall()
            tmp = []
            for h in hrefs:
                if not h:
                    continue
                u = strip_tracking(response.urljoin(h))
                if should_follow_url(u) and is_product_url(u):
                    tmp.append(u)
            product_urls = list(dict.fromkeys(tmp))

        self.logger.info(
            "LISTING EXPANDED | %s | products_found=%s",
            response.url,
            len(product_urls),
        )

        for u in product_urls:
            yield scrapy.Request(u, callback=self.parse_product)

    def find_subcategory_urls(self, response, depth: int):
        """
        Depth-aware category discovery (robust version).

        Strategy:
        - Iterate over <a> elements, not just href strings.
        - depth 0 (seed): strict allowlist based on normalized anchor label text.
        - depth 1/2: broader discovery using microphone-related keyword allowlist on the URL.
        - Hard exclude accessory/parts categories based on both URL path and label text.
        """
        out = []
        current = strip_tracking(response.url)

        BLOCKED_CATEGORY_PATH_KEYWORDS = {
            "video-podcast", "blowouts",
            "aktionen", "actie", "acties",
            "b_stock", "b-stock",
            "sale", "deals", "outlet",
            "topseller",
            "prodnews",
            "bf_", "blackfriday",
            "alle-producten-in-de-categorie",
            "brand", "merken",
        }

        for a in response.css("a"):
            href = a.attrib.get("href")
            if not href:
                continue

            u = strip_tracking(response.urljoin(href))
            if not should_follow_url(u):
                continue

            path = (urlparse(u).path or "").lower()
            if (not path.endswith(".html")) or path.endswith(".htm"):
                continue

            if u == current:
                continue

            if depth > 0 and path.endswith("microfoons.html"):
                continue

            if any(k in path for k in BLOCKED_CATEGORY_PATH_KEYWORDS):
                continue

            if depth > 0 and re.search(r"_[a-z0-9]+_microfoons?", path):
                continue

            text = clean(" ".join(a.css("::text").getall()))
            label_norm = normalize_category_label(text)

            if any(x in path for x in EXCLUDED_CATEGORY_KEYWORDS) or any(x in label_norm for x in EXCLUDED_CATEGORY_KEYWORDS):
                continue

            if depth == 0:
                if label_norm in ALLOWED_SUBCATEGORY_NAMES_SEED_NORM:
                    out.append(u)
                continue

            u_low = u.lower()
            if any(
                k in u_low
                for k in [
                    "microfoon", "microfoons", "mikro", "micro",
                    "zangmicro", "instrumentenmicro",
                    "condensator", "grootmembraan", "kleinmembraan",
                    "ribbon", "headset", "lavalier",
                    "usb", "podcast", "broadcast",
                    "zender", "video", "camera", "reporter",
                    "grensvlak", "installatie", "meetmicro", "ovid",
                ]
            ):
                out.append(u)

        return list(dict.fromkeys(out))

    def parse_product(self, response):
        self.logger.info("PRODUCT %s", response.url)

        item = {
            "type": "product",
            "competitor_id": COMPETITOR_ID,
            "competitor_name": COMPETITOR_NAME,
            "scrape_run_id": self.scrape_run_id,
            "scraped_at": iso_utc_now(),
            "source_url": strip_tracking(response.url),
            "seed_category": "microfoons.html",

            # Stable product identifier on Thomann
            "listing_id": None,

            # Core product attributes
            "title": None,
            "description": None,
            "brand": None,
            "gtin": None,
            "mpn": None,
            "sku": None,
            "model": None,
            "image_url": None,

            # Category/breadcrumb information
            "breadcrumb_category": None,
            "breadcrumb_url": None,
            "breadcrumb_parent": None,

            # Pricing + stock information
            "currency": "EUR",
            "price_text": None,
            "current_price": None,
            "base_price": None,
            "discount_amount": None,
            "discount_percent": None,
            "stock_status_text": None,
            "in_stock": None,

            # Reviews
            "rating_value": None,
            "rating_scale": 5,
            "review_count": None,

            # Matching helper
            "canonical_name": None,
        }

        # Stable listing_id from Thomann article number
        item["listing_id"] = extract_listing_id_from_html(response.text)

        # Parse JSON-LD blocks (Product + BreadcrumbList)
        blocks = response.css('script[type="application/ld+json"]::text').getall()
        nodes = []
        for b in blocks:
            b = (b or "").strip()
            if not b:
                continue
            try:
                data = json.loads(b)
                nodes.extend(iter_json_ld(data))
            except Exception:
                continue

        product_ld = None
        breadcrumb_ld = None
        for n in nodes:
            if not isinstance(n, dict):
                continue
            t = n.get("@type")
            if t == "Product" or (isinstance(t, list) and "Product" in t):
                product_ld = product_ld or n
            if t == "BreadcrumbList" or (isinstance(t, list) and "BreadcrumbList" in t):
                breadcrumb_ld = breadcrumb_ld or n

        # Extract product fields from Product JSON-LD
        if product_ld:
            item["title"] = clean(product_ld.get("name"))
            item["description"] = clean(product_ld.get("description"))

            brand = product_ld.get("brand")
            if isinstance(brand, dict):
                item["brand"] = clean(brand.get("name"))
            elif isinstance(brand, str):
                item["brand"] = clean(brand)

            for k in ("gtin13", "gtin14", "gtin12", "gtin8", "gtin"):
                v = product_ld.get(k)
                if v:
                    item["gtin"] = clean(v)
                    break

            if product_ld.get("mpn"):
                item["mpn"] = clean(product_ld.get("mpn"))
            if product_ld.get("sku"):
                item["sku"] = clean(product_ld.get("sku"))

            if product_ld.get("model"):
                m = product_ld.get("model")
                if isinstance(m, dict):
                    item["model"] = clean(m.get("name") or m.get("model"))
                else:
                    item["model"] = clean(m)

            img = product_ld.get("image")
            if isinstance(img, list) and img:
                item["image_url"] = clean(img[0])
            elif isinstance(img, str):
                item["image_url"] = clean(img)

            offers = product_ld.get("offers")
            if isinstance(offers, list) and offers:
                offers = offers[0]
            if isinstance(offers, dict):
                p = sane_price(price_to_float(offers.get("price")))
                if p is not None:
                    item["current_price"] = p
                    item["price_text"] = clean(offers.get("price"))

                av = offers.get("availability")
                if isinstance(av, str):
                    item["stock_status_text"] = av
                    item["in_stock"] = ("InStock" in av)

            agg = product_ld.get("aggregateRating")
            if isinstance(agg, dict):
                item["rating_value"] = clean(agg.get("ratingValue"))
                item["review_count"] = clean(agg.get("reviewCount") or agg.get("ratingCount"))

        # Final availability extraction (HTML-verified)
        stock_text, in_stock = extract_stock_from_html(response)
        if stock_text and not item["stock_status_text"]:
            item["stock_status_text"] = stock_text
        if item["in_stock"] is None and in_stock is not None:
            item["in_stock"] = in_stock

        # Breadcrumb/category info from BreadcrumbList JSON-LD
        if breadcrumb_ld and isinstance(breadcrumb_ld.get("itemListElement"), list):
            names = []
            urls = []
            for el in breadcrumb_ld["itemListElement"]:
                if isinstance(el, dict):
                    nm = el.get("name")
                    it = el.get("item")
                    names.append(clean(nm))
                    if isinstance(it, str):
                        urls.append(clean(it))
                    elif isinstance(it, dict):
                        urls.append(clean(it.get("@id")))
                    else:
                        urls.append(None)

            names = [n for n in names if n]
            urls = [u for u in urls if u]

            cat_candidates = [(n, u) for n, u in zip(names, urls) if u and looks_like_category_url(u)]
            if cat_candidates:
                item["breadcrumb_category"], item["breadcrumb_url"] = cat_candidates[-1]
                if len(cat_candidates) >= 2:
                    item["breadcrumb_parent"] = cat_candidates[-2][0]

        # Microdata + HTML breadcrumb fallbacks
        if not item["breadcrumb_category"] or not item["breadcrumb_url"]:
            cat, cat_url, parent = extract_breadcrumb_from_microdata(response)
            item["breadcrumb_category"] = item["breadcrumb_category"] or cat
            item["breadcrumb_url"] = item["breadcrumb_url"] or cat_url
            item["breadcrumb_parent"] = item["breadcrumb_parent"] or parent

        if not item["breadcrumb_category"] or not item["breadcrumb_url"]:
            cat, cat_url, parent = extract_breadcrumb_from_html(response)
            item["breadcrumb_category"] = item["breadcrumb_category"] or cat
            item["breadcrumb_url"] = item["breadcrumb_url"] or cat_url
            item["breadcrumb_parent"] = item["breadcrumb_parent"] or parent

        # HTML fallbacks for core fields (if JSON-LD is missing)
        if not item["title"]:
            item["title"] = (
                clean(response.css("h1::text").get())
                or clean(response.css('meta[property="og:title"]::attr(content)').get())
                or clean(response.css("title::text").get())
            )

        if not item["image_url"]:
            item["image_url"] = clean(response.css('meta[property="og:image"]::attr(content)').get())

        if not item["description"]:
            item["description"] = (
                clean(response.css('meta[name="description"]::attr(content)').get())
                or clean(response.css('meta[property="og:description"]::attr(content)').get())
            )

        # Price fallbacks: meta tags -> buybox current price (avoid full body scan)
        if item["current_price"] is None:
            p, ptxt = extract_price_from_meta(response)
            if p is not None:
                item["current_price"] = p
                item["price_text"] = item["price_text"] or ptxt

        buybox_cur = None
        if item["current_price"] is None:
            buybox_cur = extract_price_from_buybox(response)
            item["current_price"] = buybox_cur

        self.logger.info(
            "BUYBOX PRICE DEBUG | %s | buybox_cur=%r | final_current=%r final_base=%r",
            response.url,
            buybox_cur,
            item["current_price"],
            item["base_price"],
        )

        # Thomann-specific discount reference: 30-Day Best Price
        # Only use as base_price if it is actually higher than current_price.
        if item["base_price"] is None:
            ref30 = extract_reference_price_30day(response)
            if ref30 is not None and item["current_price"] is not None:
                if ref30 > item["current_price"]:
                    item["base_price"] = ref30
                else:
                    self.logger.info(
                        "REF30 IGNORED (<= current) | %s | ref30=%r current=%r",
                        response.url, ref30, item["current_price"]
                    )

        # Discount derived from base/current when possible
        if item["base_price"] is not None and item["current_price"] is not None:
            if item["base_price"] > item["current_price"]:
                item["discount_amount"] = round(item["base_price"] - item["current_price"], 2)
                if item["base_price"] > 0:
                    item["discount_percent"] = round((item["discount_amount"] / item["base_price"]) * 100, 2)

        # Final normalization + matching helper field
        item["model"] = normalize_bad_model(item["model"])
        item["canonical_name"] = (
            canonicalize_name(item["brand"], item["title"], item["model"])
            or canonicalize_name(None, item["title"], None)
        )

        yield item
