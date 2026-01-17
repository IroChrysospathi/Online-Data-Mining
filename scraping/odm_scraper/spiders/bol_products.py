"""
Bol.com product spider (microphones category).

Improvements over baseline:
- Extract more product links reliably from listing pages (use product card selectors).
- Follow pagination more reliably (multiple selectors + URL page fallback).
- Better alignment on product pages:
  - Prefer JSON-LD Product + WebPage breadcrumbs
  - Stronger buy-box-only price extraction (avoid “other products” prices)
  - Breadcrumb filtering: only accept real category breadcrumbs (/l/) and ignore /prijsoverzicht/
  - Better image/brand/title fallbacks via meta tags
  - Avoid noisy model extraction; only accept plausible model tokens
- Adds extra fields useful for your ERD: gtin, mpn, sku, canonical_name, breadcrumb fields.
"""

import json
import re
import scrapy
from datetime import datetime, timezone
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse


# baseline funcs 

def clean(text):
    if text is None:
        return None
    s = re.sub(r"\s+", " ", str(text)).strip()
    return s or None


def price_to_float(text):
    """
    Parse common European formats:
    '€ 129,99' -> 129.99
    '129,99' -> 129.99
    '59.99' -> 59.99
    """
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
    """Yield dict nodes from JSON-LD (handles @graph)."""
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
    """
    Return first non-empty meta content for given names.
    Supports:
      - meta[property="og:title"]
      - meta[name="description"]
      - meta[property="product:price:amount"]
    """
    for n in names:
        v = response.css(f'meta[property="{n}"]::attr(content)').get()
        if v:
            return clean(v)
        v = response.css(f'meta[name="{n}"]::attr(content)').get()
        if v:
            return clean(v)
    return None


def pick_first_price_text(texts):
    """Pick the first plausible price-like text from a list of strings."""
    for t in texts:
        t = clean(t)
        if not t:
            continue
        if "€" in t or re.search(r"\b\d+[,.]\d{2}\b", t):
            return t
    return None


def normalize_bad_model(model):
    """
    Keep only plausible model strings; reject common junk or too-long sentences.
    """
    m = clean(model)
    if not m:
        return None

    low = m.lower()
    # known garbage we saw from loose regex matches
    if low in {"ditiontype", "editiontype", "conditiontype"}:
        return None

    # too long 
    if len(m) > 30 and " " in m:
        return None

    # too short
    if len(m) < 2:
        return None

    return m


def strip_tracking(url: str) -> str:
    """Remove bol tracking params like cid, bltgh, etc."""
    try:
        p = urlparse(url)
        q = parse_qs(p.query)
        for k in list(q.keys()):
            if k.lower() in {"cid", "bltgh", "bltg", "blt", "ref", "promo"}:
                q.pop(k, None)
        new_query = urlencode(q, doseq=True)
        return urlunparse((p.scheme, p.netloc, p.path, p.params, new_query, p.fragment))
    except Exception:
        return url


def looks_like_category_url(url: str) -> bool:
    """Accept only category URLs (bol lists often contain /l/). Reject price overview."""
    if not url:
        return False
    u = url.lower()
    if "/prijsoverzicht/" in u:
        return False
    # bol category/list pages often have /l/ in path
    return "/l/" in u


# spider
class BolProductsSpider(scrapy.Spider):
    name = "bol_products"
    allowed_domains = ["bol.com"]

    start_urls = ["https://www.bol.com/nl/nl/l/microfoons/7119/"]

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
        # 200 limits = number of downloaded pages 
        "CLOSESPIDER_PAGECOUNT": 200,
    }

    # listing pages
    def parse(self, response):
        self.logger.info(
            "LISTING status=%s title=%s url=%s",
            response.status,
            clean(response.css("title::text").get()),
            response.url,
        )

        # product-card title links (more reliable than href contains)
        links = response.css('a[data-test="product-title"]::attr(href)').getall()

        # fallbacks (bol sometimes changes structure)
        if not links:
            links = response.css('li[data-test="product-item"] a[href*="/nl/nl/p/"]::attr(href)').getall()
        if not links:
            links = response.css('a[href*="/nl/nl/p/"]::attr(href)').getall()

        links = [strip_tracking(response.urljoin(h)) for h in links if h]
        links = list(dict.fromkeys(links))
        self.logger.info("Found %d product links", len(links))

        for url in links:
            yield response.follow(url, callback=self.parse_product)

        # pagination: try common next links; if missing, try URL-based ?page=
        next_page = (
            response.css('a[rel="next"]::attr(href)').get()
            or response.css('a[data-test="pagination-next"]::attr(href)').get()
            or response.css('a[aria-label*="Volgende"]::attr(href)').get()
            or response.css('a[aria-label*="Next"]::attr(href)').get()
        )

        if next_page:
            yield response.follow(next_page, callback=self.parse)
            return

        # URL-based fallback: if a page parameter exists, increment it
        # if it doesn't, add page=2 as a last resort 
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
        else:
            # only add page=2 for the first page (avoid looping weird URLs)
            if response.url.rstrip("/").endswith("/7119"):
                q["page"] = ["2"]
                url2 = urlunparse((p.scheme, p.netloc, p.path, p.params, urlencode(q, doseq=True), p.fragment))
                yield response.follow(url2, callback=self.parse)

    # product pages
    def parse_product(self, response):
        item = {
            "shop": "bol",
            "category_seed": "microfoons/7119",
            "source_url": strip_tracking(response.url),
            "scraped_at": datetime.now(timezone.utc).isoformat(),

            # PRODUCT-ish fields
            "title": None,
            "brand": None,
            "model": None,
            "canonical_name": None,
            "gtin": None,
            "mpn": None,
            "sku": None,

            # Listing-ish fields
            "description": None,
            "image_url": None,

            # Price snapshot-ish fields
            "currency": "EUR",
            "price": None,
            "price_raw": None,
            "availability_raw": None,
            "in_stock": None,

            # Review-ish fields
            "rating_value": None,
            "review_count": None,
            "rating_scale": 5,

            # Category-ish fields
            "breadcrumb_category": None,
            "breadcrumb_parent": None,
            "breadcrumb_url": None,
        }

        # JSON-LD extraction (Product + BreadcrumbList/WebPage)
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

        # Product JSON-LD
        if product_ld:
            item["title"] = clean(product_ld.get("name"))
            item["description"] = clean(product_ld.get("description"))

            brand = product_ld.get("brand")
            if isinstance(brand, dict):
                item["brand"] = clean(brand.get("name"))
            elif isinstance(brand, str):
                item["brand"] = clean(brand)

            # IDs
            for k in ("gtin13", "gtin14", "gtin12", "gtin8", "gtin"):
                v = product_ld.get(k)
                if v:
                    item["gtin"] = clean(v)
                    break
            if product_ld.get("mpn"):
                item["mpn"] = clean(product_ld.get("mpn"))
            if product_ld.get("sku"):
                item["sku"] = clean(product_ld.get("sku"))

            # model 
            if product_ld.get("model"):
                m = product_ld.get("model")
                if isinstance(m, dict):
                    item["model"] = clean(m.get("name") or m.get("model"))
                else:
                    item["model"] = clean(m)

            # image
            img = product_ld.get("image")
            if isinstance(img, list) and img:
                item["image_url"] = clean(img[0])
            elif isinstance(img, str):
                item["image_url"] = clean(img)

            # offers
            offers = product_ld.get("offers")
            if isinstance(offers, list) and offers:
                offers = offers[0]
            if isinstance(offers, dict):
                p = offers.get("price")
                if p is not None:
                    item["price_raw"] = clean(p)
                    item["price"] = price_to_float(p)

                av = offers.get("availability")
                if isinstance(av, str):
                    item["availability_raw"] = av
                    item["in_stock"] = ("InStock" in av)

            # rating
            agg = product_ld.get("aggregateRating")
            if isinstance(agg, dict):
                rv = agg.get("ratingValue")
                rc = agg.get("reviewCount") or agg.get("ratingCount")
                item["rating_value"] = clean(rv)
                item["review_count"] = clean(rc)

        # BreadcrumbList JSON-LD (best for categories when available)
        if breadcrumb_ld and isinstance(breadcrumb_ld.get("itemListElement"), list):
            names = []
            urls = []
            for el in breadcrumb_ld["itemListElement"]:
                if isinstance(el, dict):
                    nm = el.get("name")
                    it = el.get("item")
                    names.append(clean(nm))
                    urls.append(clean(it) if isinstance(it, str) else clean((it or {}).get("@id")))
            names = [n for n in names if n]
            urls = [u for u in urls if u]
            # last breadcrumb should be category or product name depending on page
            # the last *category* URL that looks like /l/
            cat_candidates = [(n, u) for n, u in zip(names, urls) if u and looks_like_category_url(u)]
            if cat_candidates:
                item["breadcrumb_category"], item["breadcrumb_url"] = cat_candidates[-1]
                if len(cat_candidates) >= 2:
                    item["breadcrumb_parent"] = cat_candidates[-2][0]

        # Visual/HTML fallbacks (strong alignment)
        # title: h1, else og:title, else <title>
        if not item["title"]:
            item["title"] = (
                clean(response.css("h1::text").get())
                or meta_content(response, "og:title")
                or clean(response.css("title::text").get())
            )
            if item["title"]:
                item["title"] = re.sub(r"\s*\|\s*bol\s*$", "", item["title"], flags=re.IGNORECASE).strip()

        # brand: brand link/text, else meta
        if not item["brand"]:
            item["brand"] = (
                clean(response.css('[data-test="brandLink"]::text').get())
                or clean(response.css('a[href*="/nl/nl/b/"]::text').get())
                or meta_content(response, "product:brand")
            )

        # image: og:image is very reliable on bol
        if not item["image_url"]:
            item["image_url"] = meta_content(response, "og:image")

        # description: meta fallback
        if not item["description"]:
            item["description"] = meta_content(response, "description", "og:description")

        # price: restrict to buy box only; then meta
        if item["price"] is None:
            buy_block = response.css('[data-test="buy-block"], [data-test="buybox"], [data-test="buyBox"]')
            price_text = None
            if buy_block:
                candidates = buy_block.css('[data-test*="price"] *::text').getall()
                price_text = pick_first_price_text(candidates)

            price_text = price_text or meta_content(response, "product:price:amount", "og:price:amount")

            if not price_text:
                price_text = clean(response.css('[itemprop="price"]::attr(content)').get()) or clean(
                    response.css('[itemprop="price"]::text').get()
                )

            item["price_raw"] = item["price_raw"] or price_text
            item["price"] = price_to_float(price_text)

        # availability (cheap heuristic, keep raw buy-box text)
        if not item["availability_raw"] or item["in_stock"] is None:
            buy_text = clean(" ".join(response.css(
                '[data-test="buy-block"], [data-test="buybox"], [data-test="buyBox"] *::text'
            ).getall()))
            if buy_text:
                item["availability_raw"] = item["availability_raw"] or buy_text
                if item["in_stock"] is None:
                    low = buy_text.lower()
                    if any(x in low for x in ["niet leverbaar", "uitverkocht", "tijdelijk niet beschikbaar"]):
                        item["in_stock"] = False
                    elif any(x in low for x in ["morgen in huis", "op voorraad", "voor 23:59", "leverbaar"]):
                        item["in_stock"] = True

        # ratings: visible fallback if JSON-LD missing
        if not item["rating_value"] or not item["review_count"]:
            rating_text = clean(" ".join(response.css(
                '[data-test*="rating"] *::text, a[href*="reviews"] *::text, [href*="#review"] *::text'
            ).getall())) or ""
            if not item["rating_value"]:
                m = re.search(r"\b(\d(?:[.,]\d)?)\b", rating_text)
                if m:
                    item["rating_value"] = m.group(1).replace(",", ".")
            if not item["review_count"]:
                m = re.search(r"\b(\d+)\b", rating_text)
                if m:
                    item["review_count"] = m.group(1)

        # Breadcrumb HTML fallback (filtered)
        # only accept true category crumbs (/l/) and ignore /prijsoverzicht/
        if not item["breadcrumb_url"] or not looks_like_category_url(item["breadcrumb_url"]):
            crumb_texts = response.css(
                'nav a[href]::text, ol a[href]::text, a[data-test*="breadcrumb"]::text'
            ).getall()
            crumb_hrefs = response.css(
                'nav a[href]::attr(href), ol a[href]::attr(href), a[data-test*="breadcrumb"]::attr(href)'
            ).getall()

            crumb_texts = [clean(c) for c in crumb_texts if clean(c)]
            crumb_hrefs = [strip_tracking(response.urljoin(h)) for h in crumb_hrefs if h]

            # pair them by index, filter to category URLs
            pairs = []
            for i in range(min(len(crumb_texts), len(crumb_hrefs))):
                nm = crumb_texts[i]
                u = crumb_hrefs[i]
                if nm and u and looks_like_category_url(u):
                    pairs.append((nm, u))

            if pairs:
                item["breadcrumb_category"], item["breadcrumb_url"] = pairs[-1]
                if len(pairs) >= 2:
                    item["breadcrumb_parent"] = pairs[-2][0]

        # identifier fallbacks (only if missing)
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

        # canonical name (for PRODUCT.canonical_name NOT NULL)
        item["canonical_name"] = (
            canonicalize(item["brand"], item["title"], item["model"])
            or canonicalize(None, item["title"], None)
        )

        yield item
