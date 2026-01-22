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


# Keywords to identify microphone-related content
MICRO_KEYWORDS = {"micro", "microfoon", "mic", "microphone", "microphones"}

# Priority categories for microphone products
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

# Accessory segments to skip (not main products)
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


# Non-product path segments to avoid
NON_PRODUCT_PATH_SEGMENTS = {
    "aanbiedingen",
    "b-stock-aanbiedingen",
    "hot-new-releases",
    "top-10",
}

# Possible URL keys for product links
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


# Helper functions for data cleaning and processing

def clean_text(text):
    """Clean and normalize text by removing extra whitespace."""
    if text is None:
        return None
    cleaned = re.sub(r"\s+", " ", str(text)).strip()
    return cleaned or None


def convert_price_to_float(price_text):
    """Convert price text to float, handling various formats."""
    if not price_text:
        return None
    # Remove non-numeric characters except dots and commas
    cleaned = re.sub(r"[^\d,\.]", "", str(price_text))
    if not cleaned:
        return None
    # Handle European format (comma as decimal)
    if "," in cleaned and "." in cleaned:
        # e.g., 1.234,56 -> 1234.56
        cleaned = cleaned.replace(".", "").replace(",", ".")
    elif "," in cleaned:
        cleaned = cleaned.replace(".", "").replace(",", ".")
    elif "." in cleaned:
        # If dot looks like thousands separator, remove it
        if re.match(r"^\d{1,3}(?:\.\d{3})+$", cleaned):
            cleaned = cleaned.replace(".", "")
    try:
        return float(cleaned)
    except ValueError:
        return None


def text_contains_any(text, keywords):
    """Check if text contains any of the keywords (case-insensitive)."""
    if not text:
        return False
    lower_text = (text or "").lower()
    return any(keyword.lower() in lower_text for keyword in keywords)


def iterate_json_ld_objects(obj):
    """Recursively iterate through JSON-LD objects."""
    if isinstance(obj, dict):
        yield obj
        graph = obj.get("@graph")
        if isinstance(graph, list):
            for item in graph:
                yield from iterate_json_ld_objects(item)
    elif isinstance(obj, list):
        for item in obj:
            yield from iterate_json_ld_objects(item)


def create_canonical_name(brand, title, model=None):
    """Create a canonical name by combining brand, title, and model."""
    parts = [clean_text(brand), clean_text(title), clean_text(model)]
    parts = [part for part in parts if part]
    if not parts:
        return None
    combined = " ".join(parts).lower()
    # Remove non-alphanumeric characters
    combined = re.sub(r"[^a-z0-9]+", " ", combined)
    combined = re.sub(r"\s+", " ", combined).strip()
    return combined or None


def get_meta_content(response, *property_names):
    """Extract content from meta tags."""
    for prop in property_names:
        content = response.css(f'meta[property="{prop}"]::attr(content)').get()
        if content:
            return clean_text(content)
        content = response.css(f'meta[name="{prop}"]::attr(content)').get()
        if content:
            return clean_text(content)
    return None


def pick_first_price_text(price_texts):
    """Pick the first text that looks like a price."""
    for text in price_texts:
        cleaned = clean_text(text)
        if not cleaned:
            continue
        if "€" in cleaned or re.search(r"\b\d+[,.]\d{2}\b", cleaned):
            return cleaned
    return None


def looks_like_price_text(text):
    """Check if text resembles a price."""
    if not text:
        return False
    cleaned = clean_text(text) or ""
    if "€" in cleaned:
        return True
    if re.search(r"\b(?:eur|euro)\b", cleaned, re.IGNORECASE):
        return True
    return bool(re.search(r"\b\d{1,3}(?:[.\s]\d{3})*,\d{2}\b", cleaned))


def normalize_model_name(model):
    """Normalize and validate model name."""
    cleaned = clean_text(model)
    if not cleaned:
        return None

    lower_cleaned = cleaned.lower()
    # Remove invalid model names
    if lower_cleaned in {"ditiontype", "editiontype", "conditiontype"}:
        return None
    if len(cleaned) > 30 and " " in cleaned:
        return None
    if len(cleaned) < 2:
        return None
    return cleaned


def strip_tracking_parameters(url):
    """Remove tracking parameters from URL."""
    try:
        parsed = urlparse(url)
        query = parse_qs(parsed.query)
        # Remove common tracking parameters
        tracking_params = {"utm_source", "utm_medium", "utm_campaign", "utm_content", "utm_term", "ref"}
        for param in list(query.keys()):
            if param.lower() in tracking_params:
                query.pop(param, None)
        new_query = urlencode(query, doseq=True)
        return urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, new_query, parsed.fragment))
    except Exception:
        return url


def slug_to_label(slug):
    """Convert URL slug to human-readable label."""
    if not slug:
        return None
    label = slug.replace("-", " ").replace("_", " ")
    return clean_text(label)


def safe_filename_from_url(url):
    """Create a safe filename from URL for debugging."""
    parsed = urlparse(url)
    slug = parsed.path.strip("/") or "root"
    slug = slug.replace("/", "__")
    slug = re.sub(r"[^a-zA-Z0-9_.-]", "_", slug)
    return f"{slug}.html"


def extract_breadcrumbs_from_url(url):
    """Extract breadcrumb labels and URLs from URL path."""
    try:
        parsed = urlparse(url)
        parts = [part for part in parsed.path.split("/") if part]
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
            urls.append(strip_tracking_parameters(urlunparse((parsed.scheme, parsed.netloc, path, "", "", ""))))
            labels.append(label)
        return labels, urls
    except Exception:
        return [], []


def is_product_url(url):
    """Check if URL looks like a product page."""
    if not url:
        return False
    parsed = urlparse(url)
    parts = [part for part in parsed.path.split("/") if part]
    return len(parts) >= 2


def should_follow_url(url):
    """Check if URL should be followed (not blocked)."""
    if not url:
        return False
    lower_url = url.lower()
    blocked_patterns = [
        "/blog/", "/wishlist", "/checkout", "/basket", "/login", "/account"
    ]
    if any(pattern in lower_url for pattern in blocked_patterns):
        return False
    if re.search(r"\.(pdf|zip|jpe?g|png|svg)$", urlparse(lower_url).path):
        return False
    return True


def has_accessory_segment(url):
    """Check if URL contains accessory segments."""
    if not url:
        return False
    path = urlparse(url).path.lower()
    for segment in [part for part in path.split("/") if part]:
        if segment in ACCESSORY_SEGMENTS:
            return True
    return False


def is_allowed_category_url(url):
    """Check if URL is an allowed category page."""
    if not url:
        return False
    path = urlparse(url).path.lower()
    parts = [part for part in path.split("/") if part]
    if any(segment in NON_PRODUCT_PATH_SEGMENTS for segment in parts):
        return False
    return bool(parts)


def is_probable_product_url(url, require_micro_keyword=True):
    """Check if URL is likely a product page."""
    if not url:
        return False
    if not should_follow_url(url):
        return False
    path = urlparse(url).path.lower()
    parts = [part for part in path.split("/") if part]
    if len(parts) < 1:
        return False
    if any(segment in NON_PRODUCT_PATH_SEGMENTS for segment in parts):
        return False
    if require_micro_keyword and not any(keyword in path for keyword in MICRO_KEYWORDS):
        return False
    if has_accessory_segment(url):
        return False
    return True


def get_category_priority(url):
    """Get priority score for category URL."""
    if not url:
        return 0
    path = urlparse(url).path.lower()
    for idx, keyword in enumerate(PRIORITY_CATEGORY_KEYWORDS):
        if keyword in path:
            return 100 - (idx * 10)
    return 0


def is_listing_url_allowed(url):
    """Check if listing URL is allowed."""
    if not url:
        return False
    if not should_follow_url(url):
        return False
    path = urlparse(url).path.lower()
    parts = [segment for segment in path.split("/") if segment]
    if not parts:
        return False
    if any(segment in NON_PRODUCT_PATH_SEGMENTS for segment in parts):
        return False
    if has_accessory_segment(url):
        return False
    return True


# Sitemap URLs
MICROPHONES_SITEMAP_URL = "https://sitemap.bax-shop.nl/nl_nl/sitemap-microfoons.xml"
NL_NL_SITEMAP_URL = "https://sitemap.bax-shop.nl/nl_nl/sitemap.xml"


def get_git_commit_hash():
    """Get current git commit hash."""
    try:
        result = subprocess.check_output(["git", "rev-parse", "HEAD"], stderr=subprocess.DEVNULL)
        return result.decode("utf-8", errors="ignore").strip() or None
    except Exception:
        return None


def parse_discount_percentage(text):
    """Parse discount percentage from text."""
    if not text:
        return None
    match = re.search(r"(\d{1,2})\s*%\s*(korting|discount)", text, re.IGNORECASE)
    if match:
        try:
            return float(match.group(1))
        except Exception:
            return None
    return None


def extract_prices_from_buy_block(full_text):
    """Extract current and base prices from buy block text."""
    if not full_text:
        return None, None

    # Find price-like strings
    candidates = re.findall(r"€\s*\d[\d\.\s]*[,\.\d]{0,3}\d", full_text)
    if not candidates:
        candidates = re.findall(r"\b\d[\d\.\s]*[,\.\d]{0,3}\d\b", full_text)

    prices = []
    for candidate in candidates:
        price = convert_price_to_float(candidate)
        if price is not None:
            prices.append(price)

    if not prices:
        return None, None

    current_price = prices[0]
    base_price = prices[1] if len(prices) > 1 else None

    if len(prices) >= 2:
        # Prefer lower price as current if base is higher
        current_price_alt = min([p for p in prices if p > 0], default=current_price)
        base_price_alt = max([p for p in prices if p > 0], default=base_price or current_price)
        if base_price_alt >= current_price_alt:
            current_price, base_price = current_price_alt, base_price_alt

    return current_price, base_price


def extract_itemlist_urls(nodes, only_products=False):
    """Extract URLs from JSON-LD ItemList objects."""
    urls = []
    for node in nodes:
        node_type = node.get("@type")
        if node_type == "ItemList" or (isinstance(node_type, list) and "ItemList" in node_type):
            elements = node.get("itemListElement")
            if not isinstance(elements, list):
                continue
            for element in elements:
                if isinstance(element, dict):
                    url = element.get("url")
                    item = element.get("item")
                    element_type = element.get("@type")
                    if not url and isinstance(item, dict):
                        url = item.get("url") or item.get("@id")

                    if only_products:
                        item_types = []
                        if isinstance(item, dict):
                            item_type = item.get("@type")
                            if isinstance(item_type, list):
                                item_types.extend(item_type)
                            elif isinstance(item_type, str):
                                item_types.append(item_type)
                        element_types = []
                        if isinstance(element_type, list):
                            element_types.extend(element_type)
                        elif isinstance(element_type, str):
                            element_types.append(element_type)
                        if not any(t == "Product" for t in item_types + element_types):
                            continue

                    if isinstance(url, str):
                        urls.append(url)
    return urls


def extract_product_urls(nodes):
    """Extract product URLs from JSON-LD Product objects."""
    urls = []
    for node in nodes:
        node_type = node.get("@type")
        if node_type == "Product" or (isinstance(node_type, list) and "Product" in node_type):
            url = node.get("url") or node.get("@id")
            if isinstance(url, str):
                urls.append(url)
    return urls


def extract_urls_from_json_data(data):
    """Extract URLs from arbitrary JSON data."""
    urls = []
    if isinstance(data, dict):
        for key, value in data.items():
            if isinstance(value, str):
                lower_key = (key or "").lower()
                if lower_key in PRODUCT_URL_KEYS or lower_key.endswith("url") or lower_key.endswith("href"):
                    urls.append(value)
            elif isinstance(value, (dict, list)):
                urls.extend(extract_urls_from_json_data(value))
    elif isinstance(data, list):
        for item in data:
            urls.extend(extract_urls_from_json_data(item))
    return urls


def extract_script_json_urls(response):
    """Extract URLs from JSON scripts in the page."""
    urls = []
    scripts = response.css(
        'script[type="application/json"]::text, '
        'script#__NEXT_DATA__::text, '
        'script#__NUXT__::text'
    ).getall()
    for script in scripts:
        cleaned_script = (script or "").strip()
        if not cleaned_script:
            continue
        if cleaned_script.startswith("window.__NUXT__=") or cleaned_script.startswith("window.__INITIAL_STATE__="):
            cleaned_script = cleaned_script.split("=", 1)[1].strip()
            if cleaned_script.endswith(";"):
                cleaned_script = cleaned_script[:-1].strip()
        try:
            data = json.loads(cleaned_script)
        except Exception:
            continue
        urls.extend(extract_urls_from_json_data(data))
    return urls


def extract_product_specs(response):
    """Extract product specifications from HTML."""
    specs = {}

    # From table rows
    for row in response.css("table tr"):
        label = clean_text(" ".join(row.css("th::text, th *::text").getall()))
        value = clean_text(" ".join(row.css("td::text, td *::text").getall()))
        if label and value:
            specs.setdefault(label.lower(), value)

    # From definition lists
    for dl in response.css("dl"):
        dts = dl.css("dt")
        dds = dl.css("dd")
        for i in range(min(len(dts), len(dds))):
            label = clean_text(" ".join(dts[i].css("*::text").getall()))
            value = clean_text(" ".join(dds[i].css("*::text").getall()))
            if label and value:
                specs.setdefault(label.lower(), value)

    # From list items
    for li in response.css("li"):
        text = clean_text(" ".join(li.css("*::text").getall()))
        if not text or ":" not in text:
            continue
        label, value = text.split(":", 1)
        label = clean_text(label)
        value = clean_text(value)
        if label and value:
            specs.setdefault(label.lower(), value)

    return specs


def find_spec_value(specs, *keys):
    """Find specification value by key."""
    for label, value in specs.items():
        for key in keys:
            if key in label:
                return value
    return None


# Main spider class
class BaxProductsSpider(scrapy.Spider):
    """Spider for scraping Bax Music microphone products."""

    name = "bax_products"
    allowed_domains = ["bax-shop.nl"]

    # Starting URLs for categories
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

    # Maximum depth for category exploration
    DEFAULT_MAX_CATEGORY_DEPTH = 8

    # Scrapy settings
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

    # Version identifier
    crawler_version = "bax_products/RAW-1.0"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Generate unique run ID
        self.scrape_run_id = str(uuid.uuid4())
        self.started_at = datetime.now(timezone.utc).isoformat()
        self.git_commit_hash = get_git_commit_hash()
        try:
            self.max_category_depth = int(kwargs.get("max_depth", self.DEFAULT_MAX_CATEGORY_DEPTH))
        except ValueError:
            self.max_category_depth = self.DEFAULT_MAX_CATEGORY_DEPTH
        # Debug dump settings
        self.debug_dump_dir = os.getenv("BAX_DEBUG_DIR")
        self.debug_dump_count = 0
        try:
            self.debug_dump_limit = int(kwargs.get("debug_dump_limit", 3))
        except ValueError:
            self.debug_dump_limit = 3
        if self.debug_dump_dir:
            Path(self.debug_dump_dir).expanduser().mkdir(parents=True, exist_ok=True)
        # Selenium settings
        self.use_selenium = os.getenv("USE_SELENIUM", "0").lower() in {"1", "true", "yes", "on"}
        self._selenium_driver = None
        self._selenium_warned = False
        # Track seen sitemaps
        self._seen_sitemaps = set()

    def _dump_listing_html(self, response):
        """Dump HTML for debugging if enabled."""
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
        """Initialize the crawl by yielding initial requests."""
        # Emit run metadata once
        yield {
            "type": "run",
            "scrape_run_id": self.scrape_run_id,
            "started_at": self.started_at,
            "git_commit_hash": self.git_commit_hash,
            "crawler_version": self.crawler_version,
            "notes": "bax microphones crawl",
        }

        # Start from category URLs
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse, meta={"category_depth": 0, "category_priority": 0})

        # Parse sitemaps
        yield scrapy.Request(
            "https://www.bax-shop.nl/sitemap.xml",
            callback=self.parse_sitemap,
            meta={"category_depth": 0, "category_priority": 0},
        )

        # Targeted sitemap for microphones
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

    def sitemap_url_allowed(self, url):
        """Check if sitemap URL should be processed."""
        if not url:
            return False
        path = urlparse(url).path.lower()
        if any(segment in NON_PRODUCT_PATH_SEGMENTS for segment in path.split("/") if segment):
            return False
        if not any(keyword in path for keyword in MICRO_KEYWORDS):
            return False
        return True

    def parse_sitemap(self, response):
        """Parse XML sitemap and yield URLs."""
        if response.url in self._seen_sitemaps:
            return
        self._seen_sitemaps.add(response.url)

        # Find nested sitemaps
        nested_sitemaps = response.xpath("//*[local-name()='sitemap']/*[local-name()='loc']/text()").getall()
        for loc in nested_sitemaps:
            loc = clean_text(loc)
            if not loc:
                continue
            yield scrapy.Request(loc, callback=self.parse_sitemap)

        # Find URLs
        urls = response.xpath("//*[local-name()='url']/*[local-name()='loc']/text()").getall()
        for loc in urls:
            loc = clean_text(loc)
            if not loc or not self.sitemap_url_allowed(loc):
                continue
            yield scrapy.Request(loc, callback=self.parse, meta={"category_depth": 0, "category_priority": 0})

    def parse(self, response):
        """Parse listing or product page."""
        if self._should_render_with_selenium(response):
            selenium_response = self._render_with_selenium(response)
            if selenium_response:
                yield from self.parse(selenium_response)
                return

        self.logger.info("LISTING status=%s url=%s", response.status, response.url)
        category_depth = response.meta.get("category_depth", 0)
        category_priority_value = response.meta.get("category_priority", 0)
        allow_listing_expansion = category_depth < self.max_category_depth
        source_url = strip_tracking_parameters(response.url)

        # Extract JSON-LD data
        blocks = response.css('script[type="application/ld+json"]::text').getall()
        nodes = []
        for block in blocks:
            block = (block or "").strip()
            if not block:
                continue
            try:
                data = json.loads(block)
                nodes.extend(iterate_json_ld_objects(data))
            except Exception:
                continue

        product_nodes = []
        for node in nodes:
            node_type = node.get("@type")
            if node_type == "Product" or (isinstance(node_type, list) and "Product" in node_type):
                product_nodes.append(node)

        product_ld = product_nodes[0] if product_nodes else None

        itemlist_product_urls = extract_itemlist_urls(nodes, only_products=True)
        product_tile_nodes = response.css(
            '[itemtype*="Product"], [data-product-id], [data-sku], [data-test*="product"], [data-testid*="product"]'
        )

        has_pagination = bool(
            response.css('a[rel="next"], a[aria-label*="Volgende"], a[aria-label*="Next"], a[data-test*="pagination"]')
            .get()
        )

        og_type = (get_meta_content(response, "og:type") or "").lower()
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

        # Collect product links from various sources
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

        # Clean and filter product links
        structured_links = [strip_tracking_parameters(response.urljoin(href)) for href in structured_links if href]
        structured_links = [url for url in structured_links if is_product_url(url)]
        structured_links = [url for url in structured_links if is_probable_product_url(url, require_category_keyword=False)]

        product_links = [strip_tracking_parameters(response.urljoin(href)) for href in product_links if href]
        product_links = [url for url in product_links if is_product_url(url)]
        product_links = [url for url in product_links if is_probable_product_url(url)]

        product_links.extend(structured_links)
        product_links = list(dict.fromkeys(product_links))

        if not product_links:
            self.logger.info("NO PRODUCT LINKS url=%s", response.url)
            self._dump_listing_html(response)

        # Yield requests for product pages
        for url in product_links:
            yield response.follow(url, callback=self.parse, priority=category_priority_value)

        if allow_listing_expansion:
            # Find sub-category links
            listing_links = extract_itemlist_urls(nodes, only_products=False)
            listing_links = [strip_tracking_parameters(response.urljoin(href)) for href in listing_links if href]
            listing_links = [
                url
                for url in listing_links
                if should_follow_url(url)
                and is_listing_url_allowed(url)
                and url != response.url
                and (get_category_priority(url) > 0 or any(keyword in urlparse(url).path.lower() for keyword in MICRO_KEYWORDS))
            ]
            listing_links = list(dict.fromkeys(listing_links))

            if not listing_links:
                listing_links = response.css(
                    'a[href*="/microfoon"]::attr(href), '
                    'a[href*="/microfoons"]::attr(href), '
                    'a[data-test*="category"]::attr(href)'
                ).getall()
                if listing_links:
                    listing_links = [strip_tracking_parameters(response.urljoin(href)) for href in listing_links if href]
                    listing_links = [
                        url
                        for url in listing_links
                        if should_follow_url(url)
                        and is_listing_url_allowed(url)
                        and url != response.url
                        and url not in product_links
                        and (get_category_priority(url) > 0 or any(keyword in urlparse(url).path.lower() for keyword in MICRO_KEYWORDS))
                    ]
                    listing_links = list(dict.fromkeys(listing_links))

            # Yield requests for sub-categories
            for url in listing_links:
                priority = get_category_priority(url)
                yield response.follow(
                    url,
                    callback=self.parse,
                    meta={"category_depth": category_depth + 1},
                    priority=priority,
                )
        else:
            self.logger.debug("MAX DEPTH reached skip listings url=%s depth=%s", response.url, category_depth)

        # Handle pagination
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

        # Fallback pagination with ?page=
        parsed_url = urlparse(response.url)
        query = parse_qs(parsed_url.query)
        if "page" in query:
            try:
                current_page = int(query["page"][0])
                query["page"] = [str(current_page + 1)]
                next_url = urlunparse((parsed_url.scheme, parsed_url.netloc, parsed_url.path, parsed_url.params, urlencode(query, doseq=True), parsed_url.fragment))
                yield response.follow(next_url, callback=self.parse)
            except Exception:
                pass

    def parse_product(self, response):
        """Parse individual product page and yield product data."""
        scraped_at = datetime.now(timezone.utc).isoformat()
        source_url = strip_tracking_parameters(response.url)

        # Initialize product item with default values
        item = {
            "type": "product",
            "scrape_run_id": self.scrape_run_id,
            "scraped_at": scraped_at,
            "source_url": source_url,
            "seed_category": "microfoons",

            # Product identity
            "title": None,
            "brand": None,
            "model": None,
            "canonical_name": None,
            "gtin": None,
            "mpn": None,
            "sku": None,

            # Content
            "description": None,
            "image_url": None,

            # Price snapshot
            "currency": "EUR",
            "current_price": None,
            "base_price": None,
            "discount_amount": None,
            "discount_percent": None,
            "price_text": None,
            "in_stock": None,
            "stock_status_text": None,

            # Review aggregate
            "rating_value": None,
            "rating_scale": 5,
            "review_count": None,

            # Breadcrumbs
            "breadcrumb_category": None,
            "breadcrumb_parent": None,
            "breadcrumb_url": None,
            "breadcrumb_path": None,
            "breadcrumb_urls": None,

            # Customer service (best-effort)
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

        # Extract JSON-LD data
        blocks = response.css('script[type="application/ld+json"]::text').getall()
        nodes = []
        for block in blocks:
            block = (block or "").strip()
            if not block:
                continue
            try:
                data = json.loads(block)
                nodes.extend(iterate_json_ld_objects(data))
            except Exception:
                continue

        product_ld = None
        breadcrumb_ld = None
        for node in nodes:
            node_type = node.get("@type")
            if node_type == "Product" or (isinstance(node_type, list) and "Product" in node_type):
                product_ld = product_ld or node
            if node_type == "BreadcrumbList" or (isinstance(node_type, list) and "BreadcrumbList" in node_type):
                breadcrumb_ld = breadcrumb_ld or node

        # Extract data from JSON-LD Product
        if product_ld:
            item["title"] = clean_text(product_ld.get("name"))
            item["description"] = clean_text(product_ld.get("description"))

            brand = product_ld.get("brand")
            if isinstance(brand, dict):
                item["brand"] = clean_text(brand.get("name"))
            elif isinstance(brand, str):
                item["brand"] = clean_text(brand)

            # GTIN variants
            for gtin_key in ("gtin13", "gtin14", "gtin12", "gtin8", "gtin"):
                value = product_ld.get(gtin_key)
                if value:
                    item["gtin"] = clean_text(value)
                    break
            if product_ld.get("mpn"):
                item["mpn"] = clean_text(product_ld.get("mpn"))
            if product_ld.get("sku"):
                item["sku"] = clean_text(product_ld.get("sku"))

            if product_ld.get("model"):
                model = product_ld.get("model")
                if isinstance(model, dict):
                    item["model"] = clean_text(model.get("name") or model.get("model"))
                else:
                    item["model"] = clean_text(model)

            # Image
            image = product_ld.get("image")
            if isinstance(image, list) and image:
                item["image_url"] = clean_text(image[0])
            elif isinstance(image, str):
                item["image_url"] = clean_text(image)

            # Offers/price
            offers = product_ld.get("offers")
            if isinstance(offers, list) and offers:
                offers = offers[0]
            if isinstance(offers, dict):
                price = offers.get("price")
                if price is not None:
                    item["price_text"] = clean_text(price)
                    item["current_price"] = convert_price_to_float(price)
                if offers.get("priceCurrency"):
                    item["currency"] = clean_text(offers.get("priceCurrency"))

                availability = offers.get("availability")
                if isinstance(availability, str):
                    item["stock_status_text"] = availability
                    item["in_stock"] = ("InStock" in availability)

            # Aggregate rating
            agg_rating = product_ld.get("aggregateRating")
            if isinstance(agg_rating, dict):
                item["rating_value"] = clean_text(agg_rating.get("ratingValue"))
                item["review_count"] = clean_text(agg_rating.get("reviewCount") or agg_rating.get("ratingCount"))

        # Extract breadcrumbs from JSON-LD
        breadcrumb_names = []
        breadcrumb_urls = []
        if breadcrumb_ld and isinstance(breadcrumb_ld.get("itemListElement"), list):
            names = []
            urls = []
            for element in breadcrumb_ld["itemListElement"]:
                if isinstance(element, dict):
                    name = element.get("name")
                    item_ref = element.get("item")
                    names.append(clean_text(name))
                    urls.append(clean_text(item_ref) if isinstance(item_ref, str) else clean_text((item_ref or {}).get("@id")))
            for name, url in zip(names, urls):
                if not name or not url:
                    continue
                url = strip_tracking_parameters(url)
                if url == source_url:
                    continue
                breadcrumb_names.append(name)
                breadcrumb_urls.append(url)

        # Fallback breadcrumb extraction from HTML
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

            crumb_texts = [clean_text(text) for text in crumb_texts if clean_text(text)]
            crumb_hrefs = [strip_tracking_parameters(response.urljoin(href)) for href in crumb_hrefs if href]
            for name, url in zip(crumb_texts, crumb_hrefs):
                if not name or not url:
                    continue
                if url == source_url:
                    continue
                breadcrumb_names.append(name)
                breadcrumb_urls.append(url)

        # Set breadcrumb fields
        if breadcrumb_names:
            item["breadcrumb_path"] = breadcrumb_names
            item["breadcrumb_urls"] = breadcrumb_urls
            item["breadcrumb_category"] = breadcrumb_names[-1]
            item["breadcrumb_url"] = breadcrumb_urls[-1] if breadcrumb_urls else None
            if len(breadcrumb_names) >= 2:
                item["breadcrumb_parent"] = breadcrumb_names[-2]
        else:
            # Fallback from URL
            url_names, url_urls = extract_breadcrumbs_from_url(source_url)
            if url_names:
                item["breadcrumb_path"] = url_names
                item["breadcrumb_urls"] = url_urls
                item["breadcrumb_category"] = url_names[-1]
                item["breadcrumb_url"] = url_urls[-1] if url_urls else None
                if len(url_names) >= 2:
                    item["breadcrumb_parent"] = url_names[-2]

        # HTML fallbacks for title, brand, image, description
        if not item["title"]:
            item["title"] = (
                clean_text(response.css("h1::text").get())
                or get_meta_content(response, "og:title")
                or clean_text(response.css("title::text").get())
            )
            if item["title"]:
                item["title"] = re.sub(r"\s*\|\s*bax\s*shop\s*$", "", item["title"], flags=re.IGNORECASE).strip()

        if not item["brand"]:
            item["brand"] = (
                clean_text(response.css('[data-test*="brand"]::text').get())
                or clean_text(response.css('a[href*="/merk/"]::text').get())
                or get_meta_content(response, "product:brand")
            )

        if not item["image_url"]:
            item["image_url"] = get_meta_content(response, "og:image")

        if not item["description"]:
            item["description"] = get_meta_content(response, "description", "og:description")

        # Extract specs from HTML
        specs = extract_product_specs(response)
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

        # Price parsing from buy block
        buy_block = response.css('[data-test*="buy"], [class*="buy"], form[action*="cart"]')
        buy_text = clean_text(" ".join(buy_block.css("*::text").getall())) if buy_block else None

        if item["current_price"] is None:
            price_text = None
            price_source = None
            if buy_block:
                candidates = buy_block.css('[data-test*="price"] *::text, [class*="price"] *::text').getall()
                price_text = pick_first_price_text(candidates)
                if price_text:
                    price_source = "buy_block"
            if not price_text:
                price_text = get_meta_content(response, "product:price:amount", "og:price:amount")
                if price_text:
                    price_source = "meta"
            if not price_text:
                price_text = clean_text(response.css('[itemprop="price"]::attr(content)').get())
                if price_text:
                    price_source = "itemprop_content"
            if not price_text:
                price_text = clean_text(response.css('[itemprop="price"]::text').get())
                if price_text:
                    price_source = "itemprop_text"

            if price_source in {"buy_block", "itemprop_text"} and not looks_like_price_text(price_text):
                price_text = None

            if price_text:
                item["price_text"] = item["price_text"] or price_text
                item["current_price"] = convert_price_to_float(price_text)

        # Extract base price from buy text
        if buy_text:
            current_alt, base_alt = extract_prices_from_buy_block(buy_text)
            if current_alt is not None:
                if item["current_price"] is None:
                    item["current_price"] = current_alt
                elif base_alt is not None and item["current_price"] >= base_alt and current_alt < item["current_price"]:
                    # Prefer lower buybox price
                    item["current_price"] = current_alt
            if base_alt is not None:
                if item["current_price"] is None or base_alt >= item["current_price"]:
                    item["base_price"] = base_alt

            discount_pct = parse_discount_percentage(buy_text)
            if discount_pct is not None:
                item["discount_percent"] = discount_pct

        # Calculate discount amount
        if item["base_price"] is not None and item["current_price"] is not None:
            if item["base_price"] >= item["current_price"]:
                item["discount_amount"] = round(item["base_price"] - item["current_price"], 2)
                if item["discount_percent"] is None and item["base_price"] > 0:
                    item["discount_percent"] = round((item["discount_amount"] / item["base_price"]) * 100, 2)

        # Stock status
        if not item["stock_status_text"] or item["in_stock"] is None:
            if buy_text:
                item["stock_status_text"] = item["stock_status_text"] or buy_text
                if item["in_stock"] is None:
                    lower_buy_text = buy_text.lower()
                    if any(phrase in lower_buy_text for phrase in ["niet leverbaar", "uitverkocht", "tijdelijk niet beschikbaar"]):
                        item["in_stock"] = False
                    elif any(phrase in lower_buy_text for phrase in ["op voorraad", "voor 23:59", "leverbaar", "morgen"]):
                        item["in_stock"] = True

        # Rating fallback
        if not item["rating_value"] or not item["review_count"]:
            rating_text = clean_text(
                " ".join(
                    response.css('[data-test*="rating"] *::text, a[href*="reviews"] *::text, [href*="#review"] *::text')
                    .getall()
                )
            ) or ""
            if not item["rating_value"]:
                match = re.search(r"\b(\d(?:[.,]\d)?)\b", rating_text)
                if match:
                    item["rating_value"] = match.group(1).replace(",", ".")
            if not item["review_count"]:
                match = re.search(r"\b(\d+)\b", rating_text)
                if match:
                    item["review_count"] = match.group(1)

        # Extract customer service info from page text
        full_text = clean_text(" ".join(response.css("body *::text").getall())) or ""

        if text_contains_any(full_text, ["gratis verzending", "gratis bezorging", "gratis geleverd"]):
            item["shipping_included"] = True
        elif text_contains_any(full_text, ["verzendkosten", "bezorgkosten"]):
            item["shipping_included"] = False

        match = re.search(
            r"gratis\s+verzending.{0,80}?vanaf\s*€\s*([0-9]+(?:[.,][0-9]{1,2})?)",
            full_text,
            re.IGNORECASE,
        )
        if match:
            item["free_shipping_threshold_amt"] = convert_price_to_float(match.group(1))

        if text_contains_any(full_text, ["afhaalpunt", "ophaalpunt", "afhalen", "pickup point", "pick-up point"]):
            item["pickup_point_available"] = True

        if text_contains_any(full_text, ["bezorgen", "bezorgd", "geleverd", "levertijd", "thuisbezorgd", "morgen in huis"]):
            item["delivery_shipping_available"] = True

        if text_contains_any(full_text, ["postnl", "dhl", "dpd", "ups", "gls", "bezorger", "koerier"]):
            item["delivery_courier_available"] = True

        match = re.search(r"(\d+)\s*dagen\s*bedenktijd", full_text, re.IGNORECASE)
        if match:
            item["cooling_off_days"] = int(match.group(1))

        if text_contains_any(full_text, ["gratis retourneren", "gratis retour", "kosteloos retourneren", "gratis terugsturen"]):
            item["free_returns"] = True

        match = re.search(r"(\d+)\s*(jaar|jaren)\s*garantie", full_text, re.IGNORECASE)
        if match:
            item["warranty_duration_months"] = int(match.group(1)) * 12
        else:
            match = re.search(r"(\d+)\s*(maand|maanden)\s*garantie", full_text, re.IGNORECASE)
            if match:
                item["warranty_duration_months"] = int(match.group(1))

        if item["warranty_duration_months"] is not None and text_contains_any(full_text, ["bax", "bax music", "bax-shop"]):
            item["warranty_provider"] = "Bax Music"

        # Find customer service URL
        for href in response.css("a::attr(href)").getall():
            if not href:
                continue
            url = response.urljoin(href)
            lower_url = url.lower()
            if "bax-shop.nl" not in lower_url:
                continue
            if "/klantenservice" in lower_url or "/service" in lower_url or "/contact" in lower_url:
                item["customer_service_url"] = url
                break

        # Identifier fallbacks from text
        if not item["gtin"] or not item["mpn"] or not item["model"]:
            body_text = full_text or ""

            if not item["gtin"]:
                match = re.search(r"\b(EAN|GTIN)\b\D{0,30}(\d{8,14})\b", body_text, re.IGNORECASE)
                if match:
                    item["gtin"] = match.group(2)

            if not item["mpn"]:
                match = re.search(
                    r"\b(MPN|Artikelnummer|Part number|Onderdeelnummer)\b\D{0,30}([A-Z0-9][A-Z0-9\-_\/\.]{2,})",
                    body_text,
                    re.IGNORECASE,
                )
                if match:
                    item["mpn"] = match.group(2)

            if not item["model"]:
                match = re.search(
                    r"\b(Model|Modelnummer|Typenummer)\b\D{0,30}([A-Z0-9][A-Z0-9\-_\/\.]{2,})",
                    body_text,
                    re.IGNORECASE,
                )
                if match and re.search(r"\d", match.group(2)):
                    item["model"] = match.group(2)

        # Normalize model
        item["model"] = normalize_model_name(item["model"])
        # Create canonical name
        item["canonical_name"] = (
            create_canonical_name(item["brand"], item["title"], item["model"])
            or create_canonical_name(None, item["title"], None)
        )

        # Validate rating
        if item["rating_value"]:
            try:
                if float(item["rating_value"]) > item["rating_scale"]:
                    item["rating_value"] = None
            except ValueError:
                item["rating_value"] = None

        yield item

    def _should_render_with_selenium(self, response):
        """Check if page needs Selenium rendering."""
        if not self.use_selenium or response.meta.get("selenium_rendered"):
            return False
        if response.status != 200:
            return False
        if response.css('meta[property="og:type"][content*="product"]').get():
            return False
        return bool(response.css(".product-results, .product-result-overview"))

    def _render_with_selenium(self, response):
        """Render page with Selenium if needed."""
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
        """Load all products by clicking load more buttons."""
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
        """Click load more button if available."""
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
        """Count product tiles on page."""
        try:
            from selenium.webdriver.common.by import By
            tiles = driver.find_elements(By.CSS_SELECTOR, ".result, .product-container")
            return len(tiles)
        except Exception:
            return 0

    def _wait_for_listing(self, driver, timeout=25):
        """Wait for listing to load."""
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
        """Initialize Selenium driver."""
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
        """Cleanup on spider close."""
        if self._selenium_driver:
            try:
                self._selenium_driver.quit()
            except Exception:
                pass
