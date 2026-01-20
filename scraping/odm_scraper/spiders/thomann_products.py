"""
Product spider.

Responsibilities:
- Crawl product listing and product detail pages for a specific webshop
- Extract relevant product information (e.g., name, price, URL)
- Yield structured product data for further processing
"""
import json
import re
import uuid
import subprocess
from datetime import datetime, timezone
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode

import scrapy

# Competitor metadata (fixed IDs for ERD consistency)
# I keep competitor IDs fixed across all spiders (Bax, Bol, Thomann),so the database can join and compare competitors reliably.
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
    "accessoire", "accessoires",
    "microfoonstandaard", "microfoonstandaards",
    "statief", "statieven",
    "tafelstatief", "tafelstatieven",
    "houder", "houders",
    "beugel", "beugels",
    "zwanenhals", "zwanenhalzen",
    "microfoonhengel", "microfoonhengels",
    "kabel", "kabels",
    "aansluitkabel", "aansluitkabels",
    "shockmount", "shockmounts",
    "windbescherming",
    "popfilter", "popfilters",
    "phantom", "voeding",
    "adapter", "adapters",
    "koffer", "koffers",
    "tas", "tassen",
    "vervangingsonderdelen",
    "onderdelen",
}

# Helper functions

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
    s = clean(s).lower()
    # Remove trailing counts like "(123)"
    s = re.sub(r"\s*\(\s*\d+\s*\)\s*$", "", s)
    # Normalize common separators
    s = s.replace("-", " ").replace("/", " ")
    # Collapse whitespace
    s = re.sub(r"\s+", " ", s).strip()
    return s


# I keep a normalized allowlist for robust seed-page matching.
ALLOWED_SUBCATEGORY_NAMES_SEED_NORM = {
    normalize_category_label(x) for x in ALLOWED_SUBCATEGORY_NAMES_SEED
}


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
            "fbclid", "yclid", "mc_eid", "mc_cid", "cmp", "cmpid"
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
        "/compinfo", "compinfo_", "accessibility",
        "whistleblower", "privacy", "impressum",
        "terms", "agb", "datenschutz", "kontakt", "about"
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
        "/cart", "/checkout", "/login", "/account",
        "/wishlist", "/compare", "/compinfo", "compinfo_",
        "accessibility", "whistleblower",
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


def detect_availability_from_text(text: str):
    # Best-effort stock inference from visible (script-free) text.
    if not text:
        return None, None
    t = clean(text) or ""
    low = t.lower()

    in_stock = None
    if any(x in low for x in ["niet leverbaar", "niet op voorraad", "uitverkocht"]):
        in_stock = False
    elif any(x in low for x in ["direct leverbaar", "op voorraad", "in voorraad"]):
        in_stock = True

    snippet = t[:350] + "..." if len(t) > 350 else t
    return snippet, in_stock


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


def extract_itemlist_product_urls(response):
    # Prefer extracting product URLs from JSON-LD ItemList (more robust than raw anchors).
    urls = []
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
                    u = strip_tracking(response.urljoin(candidate))
                    if should_follow_url(u) and is_product_url(u):
                        urls.append(u)

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
    """
    block = response.css(
        'div[class*="price"], div[id*="price"], div[class*="buy"], '
        'section[class*="price"], section[class*="buy"]'
    )
    text = clean(" ".join(block.css("*::text").getall())) if block else None
    if not text:
        return None, None

    euro_vals = re.findall(r"€\s*\d[\d\.\s]*[,\.\d]{0,3}\d", text)
    floats = [sane_price(price_to_float(x)) for x in euro_vals]
    floats = [x for x in floats if x is not None]
    if not floats:
        return None, None

    floats_unique = sorted(set(floats))
    current = floats_unique[0]
    base = floats_unique[-1] if floats_unique[-1] > current else None
    return current, base


# Spider

class ThomannProductsSpider(scrapy.Spider):
    name = "thomann_products"
    allowed_domains = ["thomann.nl"]

    # I explicitly start at the Microphones category root.
    start_urls = ["https://www.thomann.nl/microfoons.html"]

    # I crawl subcategories also on depth 1 and 2, and stop at max depth 3.
    max_category_depth = 3

    custom_settings = {
        "ROBOTSTXT_OBEY": True,
        "DOWNLOAD_DELAY": 1.5,
        "RANDOMIZE_DOWNLOAD_DELAY": True,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": 1.0,
        "AUTOTHROTTLE_MAX_DELAY": 10.0,
        "CONCURRENT_REQUESTS": 4,
        "USER_AGENT": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0 Safari/537.36"
        ),
        # Safety cap: prevents the crawl from growing too large.
        "CLOSESPIDER_PAGECOUNT": 200,
    }

    crawler_version = "thomann_products/json"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.scrape_run_id = str(uuid.uuid4())
        self.started_at = iso_utc_now()
        self.git_commit_hash = get_git_commit_hash()
        self._seed_subcats_emitted = False

    def start_requests(self):
        # I first emit one "run" record with metadata for reproducibility.
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
            dont_filter=True,
            meta={"cat_depth": 0},
        )

    def parse_any(self, response):
        # I decide whether this page looks like a product or a listing/category page.
        url = strip_tracking(response.url)
        if not should_follow_url(url):
            return

        if page_looks_like_product(response) and is_product_url(url):
            yield from self.parse_product(response)
            return

        yield from self.parse_listing(response)

    def parse_listing(self, response):
        depth = int(response.meta.get("cat_depth", 0))
        self.logger.info("LISTING depth=%s %s", depth, response.url)

        subs = list(self.find_subcategory_urls(response, depth=depth))
        self.logger.info("SUBCATS found=%s cat_depth=%s url=%s", len(subs), depth, response.url)
        if subs:
            self.logger.info("SUBCATS sample=%s", subs[:10])

        # From the seed page (depth 0) I also store the subcategory URLs as items.
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
                    "subcategory_url": sub,
                }

        # Subcategory crawling: enabled on depth 0, 1, 2 (stop at depth 3).
        if depth < self.max_category_depth:
            for sub in subs:
                yield scrapy.Request(
                    sub,
                    callback=self.parse_listing,
                    dont_filter=True,
                    meta={"cat_depth": depth + 1},
                )

        # Product URL discovery: I prefer JSON-LD ItemList, and fall back to .htm anchors.
        urls = extract_itemlist_product_urls(response)

        if not urls:
            hrefs = response.css('a[href$=".htm"]::attr(href)').getall()
            tmp = []
            for h in hrefs:
                if not h:
                    continue
                u = strip_tracking(response.urljoin(h))
                if should_follow_url(u) and is_product_url(u):
                    tmp.append(u)
            urls = list(dict.fromkeys(tmp))

        for u in urls:
            yield scrapy.Request(u, callback=self.parse_product, dont_filter=True)

        # Pagination: keep the same depth for next/previous listing pages.
        next_url = response.css('a[rel="next"]::attr(href)').get()
        if not next_url:
            next_url = response.css('a:contains("Volgende")::attr(href), a:contains("Next")::attr(href)').get()

        if next_url:
            u = strip_tracking(response.urljoin(next_url))
            if should_follow_url(u):
                yield scrapy.Request(
                    u,
                    callback=self.parse_listing,
                    dont_filter=True,
                    meta={"cat_depth": depth},
                )

    def find_subcategory_urls(self, response, depth: int):
        """
        Depth-aware category discovery (robust version).

        Why this exists:
        - Thomann category links are not always literally ending in ".html" in the HTML
          (sometimes they have query params or anchors). If we use a strict CSS selector
          like a[href$=".html"], we miss many links.
        - On the seed page, subcategory anchor text often contains counts like "(123)"
          which breaks exact allowlist matching.

        Strategy:
        - Always collect anchors broadly (a::attr(href)), then decide based on the parsed URL path.
        - depth 0 (seed): strict allowlist based on cleaned anchor text
          (with trailing "(123)" removed).
        - depth 1/2: broader discovery using a microphone-related keyword allowlist on the URL.
        - Stop expanding once caller enforces max_category_depth.
        """
        out = []

        hrefs = response.css("a::attr(href)").getall()
        for href in hrefs:
            if not href:
                continue

            u = strip_tracking(response.urljoin(href))
            if not should_follow_url(u):
                continue

            # Category pages should be ".html" (products are ".htm")
            path = (urlparse(u).path or "").lower()
            if (not path.endswith(".html")) or path.endswith(".htm"):
                continue

            # Read anchor text (seed matching + exclusion)
            # I use "normalize-space" so I don't get empty strings due to formatting nodes.
            text = clean(response.xpath(f'normalize-space(//a[@href="{href}"])').get())
            label_norm = normalize_category_label(text)

            # Hard exclusion: never crawl accessory / parts categories
            # I check both URL path and label, because accessory pages often appear via label text.
            if any(x in path for x in EXCLUDED_CATEGORY_KEYWORDS) or any(x in label_norm for x in EXCLUDED_CATEGORY_KEYWORDS):
                continue

            if depth == 0:
                # Strict selection on seed page so we stay inside the Microphones tree,
                # but with normalization so small label differences don't break it.
                if label_norm in ALLOWED_SUBCATEGORY_NAMES_SEED_NORM:
                    out.append(u)
                continue

            # On deeper levels (depth 1 / 2), allow broader discovery,
            # but still keep it microphone-related and still excluding accessories.
            u_low = u.lower()
            if any(k in u_low for k in [
                "microfoon", "microfoons", "mikro", "micro",
                "zangmicro", "instrumentenmicro",
                "condensator", "grootmembraan", "kleinmembraan",
                "ribbon", "headset", "lavalier",
                "usb", "podcast", "broadcast",
                "zender", "video", "camera", "reporter",
                "grensvlak", "installatie", "meetmicro",
                "ovid",
            ]):
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

        # First: stable listing_id from Thomann article number
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

        # Extract breadcrumb/category info from BreadcrumbList JSON-LD
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

        # HTML fallbacks for core fields (in case JSON-LD is missing)
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

        # Price fallbacks: meta tags -> buybox block (avoid full body scan)
        if item["current_price"] is None:
            p, ptxt = extract_price_from_meta(response)
            if p is not None:
                item["current_price"] = p
                item["price_text"] = item["price_text"] or ptxt

        if item["current_price"] is None:
            cur, base = extract_price_from_buybox(response)
            item["current_price"] = cur
            item["base_price"] = base

        # Discount derived from base/current when possible
        if item["base_price"] is not None and item["current_price"] is not None:
            if item["base_price"] >= item["current_price"]:
                item["discount_amount"] = round(item["base_price"] - item["current_price"], 2)
                if item["base_price"] > 0:
                    item["discount_percent"] = round((item["discount_amount"] / item["base_price"]) * 100, 2)

        # Availability fallback using script-free text only
        if not item["stock_status_text"] or item["in_stock"] is None:
            safe_text = clean(" ".join(response.css("body *:not(script):not(style)::text").getall())) or ""
            snippet, instock = detect_availability_from_text(safe_text)
            if not item["stock_status_text"]:
                item["stock_status_text"] = snippet
            if item["in_stock"] is None:
                item["in_stock"] = instock

        # Rating fallback using script-free text only
        if not item["rating_value"] or not item["review_count"]:
            safe_text = clean(" ".join(response.css("body *:not(script):not(style)::text").getall())) or ""
            if not item["rating_value"]:
                m = re.search(r"(\d(?:[.,]\d)?)\s+van\s+de\s+5\s+sterren", safe_text, flags=re.IGNORECASE)
                if m:
                    item["rating_value"] = m.group(1).replace(",", ".")
            if not item["review_count"]:
                m = re.search(r"\((\d{1,7})\)", safe_text)
                if m:
                    item["review_count"] = m.group(1)

        # Identifier fallbacks using script-free text
        if not item["gtin"] or not item["mpn"] or not item["model"]:
            safe_text = clean(" ".join(response.css("body *:not(script):not(style)::text").getall())) or ""

            if not item["gtin"]:
                m = re.search(r"\b(EAN|GTIN)\b\D{0,30}(\d{8,14})\b", safe_text, re.IGNORECASE)
                if m:
                    item["gtin"] = m.group(2)

            if not item["mpn"]:
                m = re.search(
                    r"\b(MPN|Part number|Onderdeelnummer)\b\D{0,30}([A-Z0-9][A-Z0-9\-_\/\.]{2,})",
                    safe_text,
                    re.IGNORECASE,
                )
                if m:
                    item["mpn"] = m.group(2)

            if not item["model"]:
                m = re.search(
                    r"\b(Model|Modelnummer|Typenummer)\b\D{0,30}([A-Z0-9][A-Z0-9\-_\/\.]{2,})",
                    safe_text,
                    re.IGNORECASE,
                )
                if m:
                    item["model"] = m.group(2)

        # Final normalization + matching helper field
        item["model"] = normalize_bad_model(item["model"])
        item["canonical_name"] = (
            canonicalize_name(item["brand"], item["title"], item["model"])
            or canonicalize_name(None, item["title"], None)
        )

        yield item
