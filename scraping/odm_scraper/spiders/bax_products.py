"""
Product spider.

Responsibilities:
- Crawl product listing and product detail pages for a specific webshop
- Extract relevant product information (e.g., name, price, URL)
- Yield structured product data for further processing
"""

import json
import re
import subprocess
import uuid
from datetime import datetime, timezone
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

import scrapy


# Category keyword allowlist based on the provided Bax microphones scope.
ALLOWED_CATEGORY_KEYWORDS = {
    "microfoon",
    "microfoons",
    "studiomicrofoon",
    "live-microfoon",
    "draadloze-microfoon",
    "usb-microfoon",
    "multimedia-av-microfoon",
    "microfoonstatieven",
    "microfoon-statief",
    "zang-microfoon",
    "microfoon-opnameset",
    "microfoon-accessoire",
    "microfoon-onderdeel",
    "microfoon-voorversterker",
    "vocal-effect",
    "vocal-effecten",
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
    if "," in t:
        t = t.replace(".", "").replace(",", ".")
    try:
        return float(t)
    except ValueError:
        return None


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


def is_allowed_category_url(url: str) -> bool:
    if not url:
        return False
    path = urlparse(url).path.lower()
    return any(k in path for k in ALLOWED_CATEGORY_KEYWORDS)


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


# spider
class BaxProductsSpider(scrapy.Spider):
    name = "bax_products"
    allowed_domains = ["bax-shop.nl"]

    start_urls = ["https://www.bax-shop.nl/microfoons"]

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
        "CLOSESPIDER_PAGECOUNT": 200,
    }

    crawler_version = "bax_products/RAW-1.0"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.scrape_run_id = str(uuid.uuid4())
        self.started_at = datetime.now(timezone.utc).isoformat()
        self.git_commit_hash = get_git_commit_hash()
        try:
            self.max_category_depth = int(kwargs.get("max_depth", 3))
        except ValueError:
            self.max_category_depth = 3

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
            yield scrapy.Request(url, callback=self.parse, meta={"category_depth": 0})

    def parse(self, response):
        self.logger.info("LISTING status=%s url=%s", response.status, response.url)
        category_depth = response.meta.get("category_depth", 0)

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
            '[itemtype*="Product"], [data-product-id], [data-sku], [data-test*="product"]'
        )

        has_pagination = bool(
            response.css('a[rel="next"], a[aria-label*="Volgende"], a[aria-label*="Next"], a[data-test*="pagination"]')
            .get()
        )

        og_type = (meta_content(response, "og:type") or "").lower()
        has_price_meta = bool(response.css('[itemprop="price"], meta[property="product:price:amount"]').get())
        has_buybox = bool(response.css('form[action*="cart"], [data-test*="add-to-cart"]').get())

        is_product_page = False
        if len(product_nodes) == 1:
            is_product_page = True
        elif "product" in og_type and not has_pagination:
            is_product_page = True
        elif product_ld and (
            has_price_meta
            or has_buybox
            or (isinstance(product_ld.get("offers"), dict) and product_ld["offers"].get("price"))
        ):
            is_product_page = True

        is_listing = not is_product_page and (
            len(itemlist_product_urls) >= 5
            or len(product_tile_nodes) >= 6
            or has_pagination
            or len(product_nodes) > 1
        )

        if is_product_page:
            yield from self.parse_product(response)
            return

        links = itemlist_product_urls
        listing_links = []
        listing_links = extract_itemlist_urls(nodes, only_product=False)

        # HTML fallback selectors
        if not links:
            links = response.css(
                '[itemtype*="Product"] a[href]::attr(href), '
                '[data-product-id] a[href]::attr(href), '
                '[data-sku] a[href]::attr(href), '
                '[data-test*="product"] a[href]::attr(href), '
                'li[class*="product"] a[href]::attr(href)'
            ).getall()

        links = [strip_tracking(response.urljoin(h)) for h in links if h]
        links = [u for u in links if looks_like_product_url(u) and should_follow_url(u)]
        links = list(dict.fromkeys(links))

        for url in links:
            yield response.follow(url, callback=self.parse_product)

        if category_depth < self.max_category_depth:
            if not listing_links:
                listing_links = response.css(
                    'a[href*="/microfoon"]::attr(href), '
                    'a[href*="/microfoons"]::attr(href), '
                    'a[data-test*="category"]::attr(href)'
                ).getall()

            listing_links = [strip_tracking(response.urljoin(h)) for h in listing_links if h]
            listing_links = [
                u
                for u in listing_links
                if should_follow_url(u) and is_allowed_category_url(u) and u != response.url
            ]
            listing_links = list(dict.fromkeys(listing_links))

            for url in listing_links:
                yield response.follow(
                    url,
                    callback=self.parse,
                    meta={"category_depth": category_depth + 1},
                )

        # Pagination
        next_page = (
            response.css('a[rel="next"]::attr(href)').get()
            or response.css('a[aria-label*="Volgende"]::attr(href)').get()
            or response.css('a[aria-label*="Next"]::attr(href)').get()
            or response.css('a[data-test*="pagination-next"]::attr(href)').get()
        )
        if next_page:
            yield response.follow(next_page, callback=self.parse)
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
        if breadcrumb_ld and isinstance(breadcrumb_ld.get("itemListElement"), list):
            names = []
            urls = []
            for el in breadcrumb_ld["itemListElement"]:
                if isinstance(el, dict):
                    nm = el.get("name")
                    it = el.get("item")
                    names.append(clean(nm))
                    urls.append(clean(it) if isinstance(it, str) else clean((it or {}).get("@id")))
            pairs = []
            for nm, u in zip(names, urls):
                if not nm or not u:
                    continue
                if strip_tracking(u) == source_url:
                    continue
                pairs.append((nm, u))
            if pairs:
                item["breadcrumb_category"], item["breadcrumb_url"] = pairs[-1]
                if len(pairs) >= 2:
                    item["breadcrumb_parent"] = pairs[-2][0]

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

        # Price parsing from buybox
        buy_block = response.css('[data-test*="buy"], [class*="buy"], form[action*="cart"]')
        buy_text = clean(" ".join(buy_block.css("*::text").getall())) if buy_block else None

        if item["current_price"] is None:
            price_text = None
            if buy_block:
                candidates = buy_block.css('[data-test*="price"] *::text, [class*="price"] *::text').getall()
                price_text = pick_first_price_text(candidates)
            price_text = price_text or meta_content(response, "product:price:amount", "og:price:amount")
            if not price_text:
                price_text = clean(response.css('[itemprop="price"]::attr(content)').get()) or clean(
                    response.css('[itemprop="price"]::text').get()
                )

            item["price_text"] = item["price_text"] or price_text
            item["current_price"] = price_to_float(price_text)

        if buy_text:
            cur2, base2 = extract_prices_from_buyblock_text(buy_text)
            if item["current_price"] is None and cur2 is not None:
                item["current_price"] = cur2
            if base2 is not None:
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

        # Identifier fallbacks
        if not item["gtin"] or not item["mpn"] or not item["model"]:
            body_text = clean(" ".join(response.css("body *::text").getall())) or ""

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
                if m:
                    item["model"] = m.group(2)

        item["model"] = normalize_bad_model(item["model"])
        item["canonical_name"] = (
            canonicalize(item["brand"], item["title"], item["model"])
            or canonicalize(None, item["title"], None)
        )

        yield item
