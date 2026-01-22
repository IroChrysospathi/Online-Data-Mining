"""
Product spider (BAX_shop) - Luuk Hoogeveen

Responsibilities:
- Start from Bax category pages (and sitemaps) to discover product listings while respecting robots.txt.
- Follow pagination/listing pages and capture listing metadata such as category, breadcrumb, and priority signals.
- Visit each product detail page, extract structured fields (name, price, availability, specs, identifiers), and normalize them before yielding items for the pipeline.
- Route requests through the shared middlewares; Selenium for JS-heavy pages (if actived by doing USE_SELENIUM) and the Bright Data proxy; to keep the crawl polite and resilient.

"""

import json
import os
import re
import subprocess
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

import scrapy
from scrapy.http import HtmlResponse


# Micro keyword traversal guard.
MICRO_KEYWORDS = {"micro", "microfoon", "mic", "microphone", "microphones"}

PRIORITY_CATEGORY_KEYWORDS = [
    "studiomicrofoons",
    "live-microfoons",
    "draadloze-microfoons",
    "usb-microfoons",
    "multimedia-av-microfoons",
    "microfoon-opnamesets",
    "zang-microfoon-startersets",
    "dynamische-microfoons",
    "condensatormicrofoons",
    "grootmembraan-condensatormicrofoons",
    "installatiemicrofoons",
]

ACCESSORY_SEGMENTS = {
    "audiokabel-per-meter-rol",
    "beltpack-kabels-connectoren",
    "boompole-microfoonhengel",
    "microfoon-accu-s-laders",
    "microfoon-antennes-accessoires",
    "microfoon-beltpack-kabels-connectoren",
    "microfoon-boosters",
    "microfoon-capsules",
    "microfoon-clips",
    "microfoon-grills",
    "microfoon-klemmen-extensions",
    "microfoon-laders-dockingstations",
    "microfoon-opbergtassen-hoezen",
    "microfoon-overige-onderdelen",
    "microfoon-popfilters",
    "microfoon-riemen-houders",
    "microfoon-schroefdraadadapters",
    "microfoon-shockmounts",
    "microfoon-statief-accessoires",
    "microfoon-statief-tas",
    "microfoon-voorversterkers",
    "microfoons-zonder-capsule",
    "overige-stands",
    "social-distancing-hygiene-producten",
    "tablet-smartphone-houder",
    "vlog-microfoons-en-toebehoren",
    "windkappen",
    "microfoon-reflectiefilter",
}


NON_PRODUCT_PATH_SEGMENTS = {
    "aanbiedingen",
    "b-stock-aanbiedingen",
    "hot-new-releases",
    "top-10",
}

PRODUCT_URL_KEYS = {
    "url",
    "producturl",
    "product_url",
    "productlink",
    "product_link",
    "canonicalurl",
    "canonical_url",
    "href",
    "link",
}


# helpers
def clean(text):
    if text is None:
        return None
    s = re.sub(r"\s+", " ", str(text)).strip()
    return s or None


def price_to_float(text):
    if not text:
        return None
    t = re.sub(r"[^\d,\.]", "", str(text))
    if not t:
        return None
    if "," in t and "." in t:
        # 1.234,56 -> 1234.56
        t = t.replace(".", "").replace(",", ".")
    elif "," in t:
        t = t.replace(".", "").replace(",", ".")
    elif "." in t:
        # Treat dot as thousands if it looks like 1.234 or 12.345.678
        if re.match(r"^\d{1,3}(?:\.\d{3})+$", t):
            t = t.replace(".", "")
    try:
        return float(t)
    except ValueError:
        return None


def text_has_any(text, words):
    t = (text or "").lower()
    return any(w.lower() in t for w in words)


def iter_json_ld(obj):
    if isinstance(obj, dict):
        yield obj
        g = obj.get("@graph")
        if isinstance(g, list):
            for x in g:
                yield from iter_json_ld(x)
    elif isinstance(obj, list):
        for x in obj:
            yield from iter_json_ld(x)


def canonicalize(brand, title, model=None):
    parts = [clean(brand), clean(title), clean(model)]
    parts = [p for p in parts if p]
    if not parts:
        return None
    s = " ".join(parts).lower()
    s = re.sub(r"[^a-z0-9]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s or None


def meta_content(response, *names):
    for n in names:
        v = response.css(f'meta[property="{n}"]::attr(content)').get()
        if v:
            return clean(v)
        v = response.css(f'meta[name="{n}"]::attr(content)').get()
        if v:
            return clean(v)
    return None


def pick_first_price_text(texts):
    for t in texts:
        t = clean(t)
        if not t:
            continue
        if "€" in t or re.search(r"\b\d+[,.]\d{2}\b", t):
            return t
    return None


def looks_like_price_text(text):
    if not text:
        return False
    t = clean(text) or ""
    if "€" in t:
        return True
    if re.search(r"\b(?:eur|euro)\b", t, re.IGNORECASE):
        return True
    return bool(re.search(r"\b\d{1,3}(?:[.\s]\d{3})*,\d{2}\b", t))


def normalize_bad_model(model):
    m = clean(model)
    if not m:
        return None

    low = m.lower()
    if low in {"ditiontype", "editiontype", "conditiontype"}:
        return None
    if len(m) > 30 and " " in m:
        return None
    if len(m) < 2:
        return None
    return m


def strip_tracking(url: str) -> str:
    try:
        p = urlparse(url)
        q = parse_qs(p.query)
        for k in list(q.keys()):
            if k.lower() in {"utm_source", "utm_medium", "utm_campaign", "utm_content", "utm_term", "ref"}:
                q.pop(k, None)
        new_query = urlencode(q, doseq=True)
        return urlunparse((p.scheme, p.netloc, p.path, p.params, new_query, p.fragment))
    except Exception:
        return url


def slug_to_label(slug: str) -> str | None:
    if not slug:
        return None
    label = slug.replace("-", " ").replace("_", " ")
    return clean(label)


def safe_filename_from_url(url: str) -> str:
    p = urlparse(url)
    slug = p.path.strip("/") or "root"
    slug = slug.replace("/", "__")
    slug = re.sub(r"[^a-zA-Z0-9_.-]", "_", slug)
    return f"{slug}.html"


def breadcrumbs_from_url(url: str) -> tuple[list[str], list[str]]:
    try:
        p = urlparse(url)
        parts = [x for x in p.path.split("/") if x]
        if not parts:
            return [], []
        if parts[-1].endswith(".html"):
            parts = parts[:-1]
        if len(parts) <= 1:
            return [], []
        category_parts = parts[:-1]
        labels = []
        urls = []
        for i in range(len(category_parts)):
            slug = category_parts[i]
            label = slug_to_label(slug)
            if not label:
                continue
            path = "/" + "/".join(category_parts[: i + 1])
            urls.append(strip_tracking(urlunparse((p.scheme, p.netloc, path, "", "", ""))))
            labels.append(label)
        return labels, urls
    except Exception:
        return [], []


def looks_like_product_url(url: str) -> bool:
    if not url:
        return False
    p = urlparse(url)
    parts = [x for x in p.path.split("/") if x]
    return len(parts) >= 2


def should_follow_url(url: str) -> bool:
    if not url:
        return False
    u = url.lower()
    if "/blog/" in u:
        return False
    if any(x in u for x in ["/notify-product-in-stock/", "/wishlist", "/checkout", "/basket", "/login", "/account"]):
        return False
    if re.search(r"\.(pdf|zip|jpe?g|png|svg)$", urlparse(u).path):
        return False
    return True


def has_accessory_segment(url: str) -> bool:
    if not url:
        return False
    path = urlparse(url).path.lower()
    for seg in [p for p in path.split("/") if p]:
        if seg in ACCESSORY_SEGMENTS:
            return True
    return False


def is_allowed_category_url(url: str) -> bool:
    if not url:
        return False
    path = urlparse(url).path.lower()
    parts = [x for x in path.split("/") if x]
    if any(seg in NON_PRODUCT_PATH_SEGMENTS for seg in parts):
        return False
    return bool(parts)


def is_probable_product_url(url: str, require_category_keyword: bool = True) -> bool:
    if not url:
        return False
    if not should_follow_url(url):
        return False
    path = urlparse(url).path.lower()
    parts = [x for x in path.split("/") if x]
    min_parts = 1
    if len(parts) < min_parts:
        return False
    if any(seg in NON_PRODUCT_PATH_SEGMENTS for seg in parts):
        return False
    if require_category_keyword and not any(kw in path for kw in MICRO_KEYWORDS):
        return False
    if has_accessory_segment(url):
        return False
    return True




def category_priority(url: str) -> int:
    if not url:
        return 0
    path = urlparse(url).path.lower()
    for idx, kw in enumerate(PRIORITY_CATEGORY_KEYWORDS):
        if kw in path:
            return 100 - (idx * 10)
    return 0


def listing_url_allowed(url: str) -> bool:
    if not url:
        return False
    if not should_follow_url(url):
        return False
    path = urlparse(url).path.lower()
    parts = [seg for seg in path.split("/") if seg]
    if not parts:
        return False
    if any(seg in NON_PRODUCT_PATH_SEGMENTS for seg in parts):
        return False
    if has_accessory_segment(url):
        return False
    return True


MICROPHONES_SITEMAP_URL = "https://sitemap.bax-shop.nl/nl_nl/sitemap-microfoons.xml"
NL_NL_SITEMAP_URL = "https://sitemap.bax-shop.nl/nl_nl/sitemap.xml"


def get_git_commit_hash() -> str | None:
    try:
        out = subprocess.check_output(["git", "rev-parse", "HEAD"], stderr=subprocess.DEVNULL)
        return out.decode("utf-8", errors="ignore").strip() or None
    except Exception:
        return None


def parse_discount_percent(text: str) -> float | None:
    if not text:
        return None
    m = re.search(r"(\d{1,2})\s*%\s*(korting|discount)", text, re.IGNORECASE)
    if m:
        try:
            return float(m.group(1))
        except Exception:
            return None
    return None


def extract_prices_from_buyblock_text(full_text: str):
    if not full_text:
        return None, None

    candidates = re.findall(r"€\s*\d[\d\.\s]*[,\.\d]{0,3}\d", full_text)
    if not candidates:
        candidates = re.findall(r"\b\d[\d\.\s]*[,\.\d]{0,3}\d\b", full_text)

    vals = []
    for c in candidates:
        v = price_to_float(c)
        if v is not None:
            vals.append(v)

    if not vals:
        return None, None

    current = vals[0]
    base = vals[1] if len(vals) > 1 else None

    if len(vals) >= 2:
        current2 = min([x for x in vals if x > 0], default=current)
        base2 = max([x for x in vals if x > 0], default=base or current)
        if base2 >= current2:
            current, base = current2, base2

    return current, base


def extract_itemlist_urls(nodes, only_product: bool = False):
    urls = []
    for n in nodes:
        t = n.get("@type")
        if t == "ItemList" or (isinstance(t, list) and "ItemList" in t):
            els = n.get("itemListElement")
            if not isinstance(els, list):
                continue
            for el in els:
                if isinstance(el, dict):
                    url = el.get("url")
                    item = el.get("item")
                    el_type = el.get("@type")
                    if not url and isinstance(item, dict):
                        url = item.get("url") or item.get("@id")

                    if only_product:
                        item_type = None
                        if isinstance(item, dict):
                            item_type = item.get("@type")
                        types = []
                        for v in (el_type, item_type):
                            if isinstance(v, list):
                                types.extend(v)
                            elif isinstance(v, str):
                                types.append(v)
                        if not any(t == "Product" for t in types):
                            continue

                    if isinstance(url, str):
                        urls.append(url)
    return urls


def extract_product_urls(nodes):
    urls = []
    for n in nodes:
        t = n.get("@type")
        if t == "Product" or (isinstance(t, list) and "Product" in t):
            url = n.get("url") or n.get("@id")
            if isinstance(url, str):
                urls.append(url)
    return urls


def extract_urls_from_json(data):
    urls = []
    if isinstance(data, dict):
        for k, v in data.items():
            if isinstance(v, str):
                key = (k or "").lower()
                if key in PRODUCT_URL_KEYS or key.endswith("url") or key.endswith("href"):
                    urls.append(v)
            elif isinstance(v, (dict, list)):
                urls.extend(extract_urls_from_json(v))
    elif isinstance(data, list):
        for v in data:
            urls.extend(extract_urls_from_json(v))
    return urls


def extract_script_json_urls(response):
    urls = []
    scripts = response.css(
        'script[type="application/json"]::text, '
        'script#__NEXT_DATA__::text, '
        'script#__NUXT__::text'
    ).getall()
    for s in scripts:
        t = (s or "").strip()
        if not t:
            continue
        if t.startswith("window.__NUXT__=") or t.startswith("window.__INITIAL_STATE__="):
            t = t.split("=", 1)[1].strip()
            if t.endswith(";"):
                t = t[:-1].strip()
        try:
            data = json.loads(t)
        except Exception:
            continue
        urls.extend(extract_urls_from_json(data))
    return urls


def extract_spec_pairs(response) -> dict[str, str]:
    pairs: dict[str, str] = {}

    for row in response.css("table tr"):
        label = clean(" ".join(row.css("th::text, th *::text").getall()))
        value = clean(" ".join(row.css("td::text, td *::text").getall()))
        if label and value:
            pairs.setdefault(label.lower(), value)

    for dl in response.css("dl"):
        dts = dl.css("dt")
        dds = dl.css("dd")
        for i in range(min(len(dts), len(dds))):
            label = clean(" ".join(dts[i].css("*::text").getall()))
            value = clean(" ".join(dds[i].css("*::text").getall()))
            if label and value:
                pairs.setdefault(label.lower(), value)

    for li in response.css("li"):
        text = clean(" ".join(li.css("*::text").getall()))
        if not text or ":" not in text:
            continue
        label, value = text.split(":", 1)
        label = clean(label)
        value = clean(value)
        if label and value:
            pairs.setdefault(label.lower(), value)

    return pairs


def find_spec_value(pairs: dict[str, str], *keys: str) -> str | None:
    for label, value in pairs.items():
        for key in keys:
            if key in label:
                return value
    return None


# spider
class BaxProductsSpider(scrapy.Spider):
    name = "bax_products"
    allowed_domains = ["bax-shop.nl"]

    start_urls = [
        "https://www.bax-shop.nl/microfoons",
        "https://www.bax-shop.nl/dynamische-microfoons",
        "https://www.bax-shop.nl/condensatormicrofoons",
        "https://www.bax-shop.nl/draadloze-microfoons",
        "https://www.bax-shop.nl/usb-microfoons",
        "https://www.bax-shop.nl/installatiemicrofoons",
        "https://www.bax-shop.nl/multimedia-av-microfoons",
        "https://www.bax-shop.nl/microfoon-opnamesets",
    ]

    DEFAULT_MAX_CATEGORY_DEPTH = 8

    custom_settings = {
        "ROBOTSTXT_OBEY": True,
        "DOWNLOAD_DELAY": 2,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": 1.0,
        "AUTOTHROTTLE_MAX_DELAY": 10.0,
        "CONCURRENT_REQUESTS": 4,
        "USER_AGENT": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0 Safari/537.36"
        ),
        "CLOSESPIDER_PAGECOUNT": 10000,
        "CLOSESPIDER_ITEMCOUNT": 5000,
        "CLOSESPIDER_TIMEOUT": 36000,
    }

    crawler_version = "bax_products/RAW-1.0"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.scrape_run_id = str(uuid.uuid4())
        self.started_at = datetime.now(timezone.utc).isoformat()
        self.git_commit_hash = get_git_commit_hash()
        try:
            self.max_category_depth = int(kwargs.get("max_depth", self.DEFAULT_MAX_CATEGORY_DEPTH))
        except ValueError:
            self.max_category_depth = self.DEFAULT_MAX_CATEGORY_DEPTH
        self.debug_dump_dir = os.getenv("BAX_DEBUG_DIR")
        self.debug_dump_count = 0
        try:
            self.debug_dump_limit = int(kwargs.get("debug_dump_limit", 3))
        except ValueError:
            self.debug_dump_limit = 3
        if self.debug_dump_dir:
            Path(self.debug_dump_dir).expanduser().mkdir(parents=True, exist_ok=True)
        self.use_selenium = os.getenv("USE_SELENIUM", "0").lower() in {"1", "true", "yes", "on"}
        self._selenium_driver = None
        self._selenium_warned = False
        self._seen_sitemaps: set[str] = set()

    def _dump_listing_html(self, response):
        if not self.debug_dump_dir:
            return
        if self.debug_dump_count >= self.debug_dump_limit:
            return
        filename = safe_filename_from_url(response.url)
        path = Path(self.debug_dump_dir).expanduser() / filename
        try:
            path.write_bytes(response.body)
            self.debug_dump_count += 1
            self.logger.info("DUMPED LISTING HTML path=%s url=%s", path, response.url)
        except Exception as exc:
            self.logger.warning("FAILED TO DUMP LISTING HTML url=%s err=%s", response.url, exc)

    def start_requests(self):
        # Emit run metadata once
        yield {
            "type": "run",
            "scrape_run_id": self.scrape_run_id,
            "started_at": self.started_at,
            "git_commit_hash": self.git_commit_hash,
            "crawler_version": self.crawler_version,
            "notes": "bax microphones crawl",
        }

        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse, meta={"category_depth": 0, "category_priority": 0})

        yield scrapy.Request(
            "https://www.bax-shop.nl/sitemap.xml",
            callback=self.parse_sitemap,
            meta={"category_depth": 0, "category_priority": 0},
        )

        # Targeted sitemap feed for the Microfoons section.
        yield scrapy.Request(
            MICROPHONES_SITEMAP_URL,
            callback=self.parse_sitemap,
            meta={"category_depth": 0, "category_priority": 0},
        )

        yield scrapy.Request(
            NL_NL_SITEMAP_URL,
            callback=self.parse_sitemap,
            meta={"category_depth": 0, "category_priority": 0},
        )

    def sitemap_url_allowed(self, url: str) -> bool:
        if not url:
            return False
        path = urlparse(url).path.lower()
        if any(seg in NON_PRODUCT_PATH_SEGMENTS for seg in path.split("/") if seg):
            return False
        if not any(kw in path for kw in MICRO_KEYWORDS):
            return False
        return True

    def parse_sitemap(self, response):
        if response.url in self._seen_sitemaps:
            return
        self._seen_sitemaps.add(response.url)

        sitemaps = response.xpath("//*[local-name()='sitemap']/*[local-name()='loc']/text()").getall()
        for loc in sitemaps:
            loc = clean(loc)
            if not loc:
                continue
            yield scrapy.Request(loc, callback=self.parse_sitemap)

        urls = response.xpath("//*[local-name()='url']/*[local-name()='loc']/text()").getall()
        for loc in urls:
            loc = clean(loc)
            if not loc or not self.sitemap_url_allowed(loc):
                continue
            yield scrapy.Request(loc, callback=self.parse, meta={"category_depth": 0, "category_priority": 0})

    def parse(self, response):
        if self._should_render_with_selenium(response):
            selenium_response = self._render_with_selenium(response)
            if selenium_response:
                yield from self.parse(selenium_response)
                return

        self.logger.info("LISTING status=%s url=%s", response.status, response.url)
        category_depth = response.meta.get("category_depth", 0)
        category_priority_value = response.meta.get("category_priority", 0)
        allow_listing_expansion = category_depth < self.max_category_depth
        source_url = strip_tracking(response.url)

        # JSON-LD ItemList links (most reliable for product grids)
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

        product_nodes = []
        for n in nodes:
            t = n.get("@type")
            if t == "Product" or (isinstance(t, list) and "Product" in t):
                product_nodes.append(n)

        product_ld = product_nodes[0] if product_nodes else None

        itemlist_product_urls = extract_itemlist_urls(nodes, only_product=True)
        product_tile_nodes = response.css(
            '[itemtype*="Product"], [data-product-id], [data-sku], [data-test*="product"], [data-testid*="product"]'
        )

        has_pagination = bool(
            response.css('a[rel="next"], a[aria-label*="Volgende"], a[aria-label*="Next"], a[data-test*="pagination"]')
            .get()
        )

        og_type = (meta_content(response, "og:type") or "").lower()
        has_price_meta = bool(response.css('[itemprop="price"], meta[property="product:price:amount"]').get())
        has_buybox = bool(response.css('form[action*="cart"], [data-test*="add-to-cart"]').get())

        price_offer_on_ld = False
        if product_ld:
            offers = product_ld.get("offers")
            if isinstance(offers, list) and offers:
                offers = offers[0]
            if isinstance(offers, dict) and offers.get("price") is not None:
                price_offer_on_ld = True

        product_evidence = 0
        if len(product_nodes) == 1:
            product_evidence += 1
        if "product" in og_type:
            product_evidence += 1
        if has_price_meta or has_buybox:
            product_evidence += 1
        if price_offer_on_ld:
            product_evidence += 1

        listing_signals = (
            len(itemlist_product_urls) >= 5
            or len(product_tile_nodes) >= 6
            or has_pagination
            or len(product_nodes) > 1
        )

        is_product_page = False
        if not listing_signals:
            if len(product_nodes) == 1:
                is_product_page = True
            elif "product" in og_type and not has_pagination:
                is_product_page = True
            elif product_ld and (has_price_meta or has_buybox or price_offer_on_ld):
                is_product_page = True
            elif has_price_meta or has_buybox:
                is_product_page = True
        else:
            if product_evidence >= 3 and not has_pagination:
                is_product_page = True

        is_listing = not is_product_page and listing_signals

        if is_product_page:
            if has_accessory_segment(source_url):
                self.logger.info("SKIP ACCESSORY PRODUCT url=%s", source_url)
                return
            yield from self.parse_product(response)
            return

        structured_links = []
        structured_links.extend(itemlist_product_urls)
        structured_links.extend(extract_product_urls(product_nodes))
        structured_links.extend(extract_script_json_urls(response))

        product_links = []
        product_links.extend(structured_links)
        product_links.extend(
            response.css(
                ".product-results .result a[href]::attr(href), "
                "a[track-click-product]::attr(href), "
                ".product_label[data-href]::attr(data-href), "
                "[data-product] a[href]::attr(href)"
            ).getall()
        )
        if not product_links:
            product_links = response.css(
                '[itemtype*="Product"] a[href]::attr(href), '
                '[data-product-id] a[href]::attr(href), '
                '[data-sku] a[href]::attr(href), '
                '[data-test*="product"] a[href]::attr(href), '
                '[data-testid*="product"] a[href]::attr(href), '
                '[data-test*="product"][href]::attr(href), '
                '[data-testid*="product"][href]::attr(href), '
                '[data-product-id][href]::attr(href), '
                'li[class*="product"] a[href]::attr(href), '
                'a[class*="product"]::attr(href)'
            ).getall()
        if not product_links:
            product_links = response.css(
                'a[href*="microfoon"]::attr(href), '
                'a[href*="microfoons"]::attr(href)'
            ).getall()

        structured_links = [strip_tracking(response.urljoin(h)) for h in structured_links if h]
        structured_links = [u for u in structured_links if looks_like_product_url(u)]
        structured_links = [u for u in structured_links if is_probable_product_url(u, require_category_keyword=False)]

        product_links = [strip_tracking(response.urljoin(h)) for h in product_links if h]
        product_links = [u for u in product_links if looks_like_product_url(u)]
        product_links = [u for u in product_links if is_probable_product_url(u)]

        product_links.extend(structured_links)
        product_links = list(dict.fromkeys(product_links))

        if not product_links:
            self.logger.info("NO PRODUCT LINKS url=%s", response.url)
            self._dump_listing_html(response)

        for url in product_links:
            yield response.follow(url, callback=self.parse, priority=category_priority_value)

        if allow_listing_expansion:
            listing_links = extract_itemlist_urls(nodes, only_product=False)
            listing_links = [strip_tracking(response.urljoin(h)) for h in listing_links if h]
            listing_links = [
                u
                for u in listing_links
                if should_follow_url(u)
                and listing_url_allowed(u)
                and u != response.url
                and (category_priority(u) > 0 or any(kw in urlparse(u).path.lower() for kw in MICRO_KEYWORDS))
            ]
            listing_links = list(dict.fromkeys(listing_links))

            listing_links = listing_links or response.css(
                'a[href*="/microfoon"]::attr(href), '
                'a[href*="/microfoons"]::attr(href), '
                'a[data-test*="category"]::attr(href)'
            ).getall()
            if listing_links:
                listing_links = [strip_tracking(response.urljoin(h)) for h in listing_links if h]
                listing_links = [
                    u
                    for u in listing_links
                    if should_follow_url(u)
                    and listing_url_allowed(u)
                    and u != response.url
                    and u not in product_links
                    and (category_priority(u) > 0 or any(kw in urlparse(u).path.lower() for kw in MICRO_KEYWORDS))
                ]
                listing_links = list(dict.fromkeys(listing_links))

            for url in listing_links:
                prio = category_priority(url)
                yield response.follow(
                    url,
                    callback=self.parse,
                    meta={"category_depth": category_depth + 1},
                    priority=prio,
                )
        else:
            self.logger.debug("MAX DEPTH reached skip listings url=%s depth=%s", response.url, category_depth)

        # Pagination
        next_page = (
            response.css('a[rel="next"]::attr(href)').get()
            or response.css('a[aria-label*="Volgende"]::attr(href)').get()
            or response.css('a[aria-label*="Next"]::attr(href)').get()
            or response.css('a[data-test*="pagination-next"]::attr(href)').get()
        )
        if next_page:
            yield response.follow(
                next_page,
                callback=self.parse,
                meta={"category_depth": category_depth, "category_priority": category_priority_value},
                priority=category_priority_value,
            )
            return

        # Fallback ?page=
        p = urlparse(response.url)
        q = parse_qs(p.query)
        if "page" in q:
            try:
                cur = int(q["page"][0])
                q["page"] = [str(cur + 1)]
                url2 = urlunparse((p.scheme, p.netloc, p.path, p.params, urlencode(q, doseq=True), p.fragment))
                yield response.follow(url2, callback=self.parse)
            except Exception:
                pass

    def parse_product(self, response):
        scraped_at = datetime.now(timezone.utc).isoformat()
        source_url = strip_tracking(response.url)

        item = {
            "type": "product",
            "scrape_run_id": self.scrape_run_id,
            "scraped_at": scraped_at,
            "source_url": source_url,
            "seed_category": "microfoons",

            # product identity
            "title": None,
            "brand": None,
            "model": None,
            "canonical_name": None,
            "gtin": None,
            "mpn": None,
            "sku": None,

            # content
            "description": None,
            "image_url": None,

            # price snapshot
            "currency": "EUR",
            "current_price": None,
            "base_price": None,
            "discount_amount": None,
            "discount_percent": None,
            "price_text": None,
            "in_stock": None,
            "stock_status_text": None,

            # review aggregate
            "rating_value": None,
            "rating_scale": 5,
            "review_count": None,

            # breadcrumbs
            "breadcrumb_category": None,
            "breadcrumb_parent": None,
            "breadcrumb_url": None,
            "breadcrumb_path": None,
            "breadcrumb_urls": None,

            # customer service (best-effort)
            "shipping_included": None,
            "free_shipping_threshold_amt": None,
            "pickup_point_available": None,
            "delivery_shipping_available": None,
            "delivery_courier_available": None,
            "cooling_off_days": None,
            "free_returns": None,
            "warranty_provider": None,
            "warranty_duration_months": None,
            "customer_service_url": None,
        }

        # JSON-LD
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
            t = n.get("@type")
            if t == "Product" or (isinstance(t, list) and "Product" in t):
                product_ld = product_ld or n
            if t == "BreadcrumbList" or (isinstance(t, list) and "BreadcrumbList" in t):
                breadcrumb_ld = breadcrumb_ld or n

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
                p = offers.get("price")
                if p is not None:
                    item["price_text"] = clean(p)
                    item["current_price"] = price_to_float(p)
                if offers.get("priceCurrency"):
                    item["currency"] = clean(offers.get("priceCurrency"))

                av = offers.get("availability")
                if isinstance(av, str):
                    item["stock_status_text"] = av
                    item["in_stock"] = ("InStock" in av)

            agg = product_ld.get("aggregateRating")
            if isinstance(agg, dict):
                item["rating_value"] = clean(agg.get("ratingValue"))
                item["review_count"] = clean(agg.get("reviewCount") or agg.get("ratingCount"))

        # BreadcrumbList JSON-LD
        breadcrumb_names = []
        breadcrumb_urls = []
        if breadcrumb_ld and isinstance(breadcrumb_ld.get("itemListElement"), list):
            names = []
            urls = []
            for el in breadcrumb_ld["itemListElement"]:
                if isinstance(el, dict):
                    nm = el.get("name")
                    it = el.get("item")
                    names.append(clean(nm))
                    urls.append(clean(it) if isinstance(it, str) else clean((it or {}).get("@id")))
            for nm, u in zip(names, urls):
                if not nm or not u:
                    continue
                u = strip_tracking(u)
                if u == source_url:
                    continue
                breadcrumb_names.append(nm)
                breadcrumb_urls.append(u)

        if not breadcrumb_names:
            crumb_texts = response.css(
                'nav[aria-label*="breadcrumb"] a::text, '
                'nav.breadcrumb a::text, '
                'ol.breadcrumb a::text, '
                'ul.breadcrumb a::text, '
                'a[data-test*="breadcrumb"]::text'
            ).getall()
            crumb_hrefs = response.css(
                'nav[aria-label*="breadcrumb"] a::attr(href), '
                'nav.breadcrumb a::attr(href), '
                'ol.breadcrumb a::attr(href), '
                'ul.breadcrumb a::attr(href), '
                'a[data-test*="breadcrumb"]::attr(href)'
            ).getall()

            crumb_texts = [clean(c) for c in crumb_texts if clean(c)]
            crumb_hrefs = [strip_tracking(response.urljoin(h)) for h in crumb_hrefs if h]
            for nm, u in zip(crumb_texts, crumb_hrefs):
                if not nm or not u:
                    continue
                if u == source_url:
                    continue
                breadcrumb_names.append(nm)
                breadcrumb_urls.append(u)

        if breadcrumb_names:
            item["breadcrumb_path"] = breadcrumb_names
            item["breadcrumb_urls"] = breadcrumb_urls
            item["breadcrumb_category"] = breadcrumb_names[-1]
            item["breadcrumb_url"] = breadcrumb_urls[-1] if breadcrumb_urls else None
            if len(breadcrumb_names) >= 2:
                item["breadcrumb_parent"] = breadcrumb_names[-2]
        else:
            url_names, url_urls = breadcrumbs_from_url(source_url)
            if url_names:
                item["breadcrumb_path"] = url_names
                item["breadcrumb_urls"] = url_urls
                item["breadcrumb_category"] = url_names[-1]
                item["breadcrumb_url"] = url_urls[-1] if url_urls else None
                if len(url_names) >= 2:
                    item["breadcrumb_parent"] = url_names[-2]


        # HTML fallbacks
        if not item["title"]:
            item["title"] = (
                clean(response.css("h1::text").get())
                or meta_content(response, "og:title")
                or clean(response.css("title::text").get())
            )
            if item["title"]:
                item["title"] = re.sub(r"\s*\|\s*bax\s*shop\s*$", "", item["title"], flags=re.IGNORECASE).strip()

        if not item["brand"]:
            item["brand"] = (
                clean(response.css('[data-test*="brand"]::text').get())
                or clean(response.css('a[href*="/merk/"]::text').get())
                or meta_content(response, "product:brand")
            )

        if not item["image_url"]:
            item["image_url"] = meta_content(response, "og:image")

        if not item["description"]:
            item["description"] = meta_content(response, "description", "og:description")

        specs = extract_spec_pairs(response)
        if not item["brand"]:
            item["brand"] = find_spec_value(specs, "merk", "brand", "fabrikant")
        if not item["model"]:
            item["model"] = find_spec_value(specs, "model", "modelnummer", "typenummer")
        if not item["sku"]:
            item["sku"] = find_spec_value(specs, "sku")
        if not item["mpn"]:
            item["mpn"] = find_spec_value(specs, "mpn", "part number", "onderdeelnummer")
        if not item["gtin"]:
            item["gtin"] = find_spec_value(specs, "ean", "gtin")

        # Price parsing from buybox
        buy_block = response.css('[data-test*="buy"], [class*="buy"], form[action*="cart"]')
        buy_text = clean(" ".join(buy_block.css("*::text").getall())) if buy_block else None

        if item["current_price"] is None:
            price_text = None
            price_source = None
            if buy_block:
                candidates = buy_block.css('[data-test*="price"] *::text, [class*="price"] *::text').getall()
                price_text = pick_first_price_text(candidates)
                if price_text:
                    price_source = "buy_block"
            if not price_text:
                price_text = meta_content(response, "product:price:amount", "og:price:amount")
                if price_text:
                    price_source = "meta"
            if not price_text:
                price_text = clean(response.css('[itemprop="price"]::attr(content)').get())
                if price_text:
                    price_source = "itemprop_content"
            if not price_text:
                price_text = clean(response.css('[itemprop="price"]::text').get())
                if price_text:
                    price_source = "itemprop_text"

            if price_source in {"buy_block", "itemprop_text"} and not looks_like_price_text(price_text):
                price_text = None

            if price_text:
                item["price_text"] = item["price_text"] or price_text
                item["current_price"] = price_to_float(price_text)

        if buy_text:
            cur2, base2 = extract_prices_from_buyblock_text(buy_text)
            if cur2 is not None:
                if item["current_price"] is None:
                    item["current_price"] = cur2
                elif base2 is not None and item["current_price"] >= base2 and cur2 < item["current_price"]:
                    # Prefer the lower buybox price when current looks like list price.
                    item["current_price"] = cur2
            if base2 is not None:
                if item["current_price"] is None or base2 >= item["current_price"]:
                    item["base_price"] = base2

            dp = parse_discount_percent(buy_text)
            if dp is not None:
                item["discount_percent"] = dp

        if item["base_price"] is not None and item["current_price"] is not None:
            if item["base_price"] >= item["current_price"]:
                item["discount_amount"] = round(item["base_price"] - item["current_price"], 2)
                if item["discount_percent"] is None and item["base_price"] > 0:
                    item["discount_percent"] = round((item["discount_amount"] / item["base_price"]) * 100, 2)

        # Availability
        if not item["stock_status_text"] or item["in_stock"] is None:
            if buy_text:
                item["stock_status_text"] = item["stock_status_text"] or buy_text
                if item["in_stock"] is None:
                    low = buy_text.lower()
                    if any(x in low for x in ["niet leverbaar", "uitverkocht", "tijdelijk niet beschikbaar"]):
                        item["in_stock"] = False
                    elif any(x in low for x in ["op voorraad", "voor 23:59", "leverbaar", "morgen"]):
                        item["in_stock"] = True

        # Ratings fallback
        if not item["rating_value"] or not item["review_count"]:
            rating_text = clean(
                " ".join(
                    response.css('[data-test*="rating"] *::text, a[href*="reviews"] *::text, [href*="#review"] *::text')
                    .getall()
                )
            ) or ""
            if not item["rating_value"]:
                m = re.search(r"\b(\d(?:[.,]\d)?)\b", rating_text)
                if m:
                    item["rating_value"] = m.group(1).replace(",", ".")
            if not item["review_count"]:
                m = re.search(r"\b(\d+)\b", rating_text)
                if m:
                    item["review_count"] = m.group(1)

        full_text = clean(" ".join(response.css("body *::text").getall())) or ""

        if text_has_any(full_text, ["gratis verzending", "gratis bezorging", "gratis geleverd"]):
            item["shipping_included"] = True
        elif text_has_any(full_text, ["verzendkosten", "bezorgkosten"]):
            item["shipping_included"] = False

        m = re.search(
            r"gratis\s+verzending.{0,80}?vanaf\s*€\s*([0-9]+(?:[.,][0-9]{1,2})?)",
            full_text,
            re.IGNORECASE,
        )
        if m:
            item["free_shipping_threshold_amt"] = price_to_float(m.group(1))

        if text_has_any(full_text, ["afhaalpunt", "ophaalpunt", "afhalen", "pickup point", "pick-up point"]):
            item["pickup_point_available"] = True

        if text_has_any(full_text, ["bezorgen", "bezorgd", "geleverd", "levertijd", "thuisbezorgd", "morgen in huis"]):
            item["delivery_shipping_available"] = True

        if text_has_any(full_text, ["postnl", "dhl", "dpd", "ups", "gls", "bezorger", "koerier"]):
            item["delivery_courier_available"] = True

        m = re.search(r"(\d+)\s*dagen\s*bedenktijd", full_text, re.IGNORECASE)
        if m:
            item["cooling_off_days"] = int(m.group(1))

        if text_has_any(full_text, ["gratis retourneren", "gratis retour", "kosteloos retourneren", "gratis terugsturen"]):
            item["free_returns"] = True

        m = re.search(r"(\d+)\s*(jaar|jaren)\s*garantie", full_text, re.IGNORECASE)
        if m:
            item["warranty_duration_months"] = int(m.group(1)) * 12
        else:
            m = re.search(r"(\d+)\s*(maand|maanden)\s*garantie", full_text, re.IGNORECASE)
            if m:
                item["warranty_duration_months"] = int(m.group(1))

        if item["warranty_duration_months"] is not None and text_has_any(full_text, ["bax", "bax music", "bax-shop"]):
            item["warranty_provider"] = "Bax Music"

        for h in response.css("a::attr(href)").getall():
            if not h:
                continue
            u = response.urljoin(h)
            ul = u.lower()
            if "bax-shop.nl" not in ul:
                continue
            if "/klantenservice" in ul or "/service" in ul or "/contact" in ul:
                item["customer_service_url"] = u
                break

        # Identifier fallbacks
        if not item["gtin"] or not item["mpn"] or not item["model"]:
            body_text = full_text or ""

            if not item["gtin"]:
                m = re.search(r"\b(EAN|GTIN)\b\D{0,30}(\d{8,14})\b", body_text, re.IGNORECASE)
                if m:
                    item["gtin"] = m.group(2)

            if not item["mpn"]:
                m = re.search(
                    r"\b(MPN|Artikelnummer|Part number|Onderdeelnummer)\b\D{0,30}([A-Z0-9][A-Z0-9\-_\/\.]{2,})",
                    body_text,
                    re.IGNORECASE,
                )
                if m:
                    item["mpn"] = m.group(2)

            if not item["model"]:
                m = re.search(
                    r"\b(Model|Modelnummer|Typenummer)\b\D{0,30}([A-Z0-9][A-Z0-9\-_\/\.]{2,})",
                    body_text,
                    re.IGNORECASE,
                )
                if m and re.search(r"\d", m.group(2)):
                    item["model"] = m.group(2)

        item["model"] = normalize_bad_model(item["model"])
        item["canonical_name"] = (
            canonicalize(item["brand"], item["title"], item["model"])
            or canonicalize(None, item["title"], None)
        )

        if item["rating_value"]:
            try:
                if float(item["rating_value"]) > item["rating_scale"]:
                    item["rating_value"] = None
            except ValueError:
                item["rating_value"] = None

        yield item

    def _should_render_with_selenium(self, response):
        if not self.use_selenium or response.meta.get("selenium_rendered"):
            return False
        if response.status != 200:
            return False
        if response.css('meta[property="og:type"][content*="product"]').get():
            return False
        return bool(response.css(".product-results, .product-result-overview"))

    def _render_with_selenium(self, response):
        driver = self._ensure_selenium_driver()
        if not driver:
            return None
        try:
            driver.get(response.url)
            self._wait_for_listing(driver)
            self._load_all_products(driver)
            body = driver.page_source.encode("utf-8")
            meta = dict(response.meta)
            meta["selenium_rendered"] = True
            request = response.request.replace(meta=meta)
            rendered = HtmlResponse(
                url=response.url,
                body=body,
                encoding="utf-8",
                request=request,
            )
            return rendered
        except Exception as exc:
            self.logger.warning("selenium render failed url=%s err=%s", response.url, exc)
            return None

    def _load_all_products(self, driver):
        prev_count = self._count_product_tiles(driver)
        for _ in range(8):
            if not self._click_any_load_more(driver):
                break
            time.sleep(0.6)
            new_count = self._count_product_tiles(driver)
            if new_count <= prev_count:
                break
            prev_count = new_count
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(0.5)

    def _click_any_load_more(self, driver):
        try:
            from selenium.common.exceptions import WebDriverException
            from selenium.webdriver.common.by import By
        except ImportError:
            return False

        selectors = [
            "button.load-more",
            "button[data-test*='load']",
            "button[data-action*='load']",
            "a.load-more",
            "a[data-action*='load']",
            "button[class*='load-more']",
            "button[data-track*='load']",
        ]
        for selector in selectors:
            try:
                button = driver.find_element(By.CSS_SELECTOR, selector)
            except Exception:
                continue
            if not button.is_displayed():
                continue
            driver.execute_script("arguments[0].scrollIntoView(true);", button)
            try:
                button.click()
                return True
            except WebDriverException:
                continue
        return False

    def _count_product_tiles(self, driver):
        try:
            from selenium.webdriver.common.by import By
            tiles = driver.find_elements(By.CSS_SELECTOR, ".result, .product-container")
            return len(tiles)
        except Exception:
            return 0

    def _wait_for_listing(self, driver, timeout=25):
        try:
            from selenium.webdriver.common.by import By
            from selenium.webdriver.support import expected_conditions as EC
            from selenium.webdriver.support.ui import WebDriverWait
        except ImportError:
            return
        try:
            WebDriverWait(driver, timeout).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, ".product-results, .product-result-overview"))
            )
        except Exception:
            self.logger.debug("selenium listing wait timed out url=%s", driver.current_url)

    def _ensure_selenium_driver(self):
        if self._selenium_driver:
            return self._selenium_driver
        try:
            from selenium import webdriver
            from selenium.webdriver.chrome.options import Options
            from selenium.webdriver.chrome.service import Service
            from webdriver_manager.chrome import ChromeDriverManager
        except ImportError as exc:
            if not self._selenium_warned:
                self.logger.warning("selenium import failed, skipping JS rendering: %s", exc)
                self._selenium_warned = True
            return None

        options = Options()
        options.add_argument("--headless=new")
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--window-size=1400,900")

        try:
            service = Service(ChromeDriverManager().install())
            driver = webdriver.Chrome(service=service, options=options)
            self._selenium_driver = driver
            return driver
        except Exception as exc:
            if not self._selenium_warned:
                self.logger.warning("selenium driver initialization failed: %s", exc)
                self._selenium_warned = True
            return None

    def closed(self, reason):
        if self._selenium_driver:
            try:
                self._selenium_driver.quit()
            except Exception:
                pass
