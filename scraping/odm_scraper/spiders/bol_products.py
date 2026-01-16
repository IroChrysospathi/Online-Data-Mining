"""
Product spider.

Responsibilities:
- Crawl product listing and product detail pages for a specific webshop
- Extract relevant product information (e.g., name, price, URL)
- Yield structured product data for further processing
"""

import json
import re
import scrapy
from datetime import datetime, timezone


def clean(text):
    if not text:
        return None
    return re.sub(r"\s+", " ", text).strip() or None


def price_to_float(text):
    """
    Parse common European formats:
    '€ 129,99' -> 129.99
    '129,99' -> 129.99
    """
    if not text:
        return None
    t = re.sub(r"[^\d,\.]", "", text)
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


class BolProductsSpider(scrapy.Spider):
    name = "bol_products"
    allowed_domains = ["bol.com"]

    # Allowed microphone category (clean URL, no tracking params)
    start_urls = [
        "https://www.bol.com/nl/nl/l/microfoons/7119/"
    ]

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
        # While testing, stop after some pages; increase later
        "CLOSESPIDER_PAGECOUNT": 300,
    }

    def parse(self, response):
        self.logger.info("LISTING status=%s title=%s", response.status, clean(response.css("title::text").get()))

        # 1) Collect product links
        links = response.css('a[href*="/nl/nl/p/"]::attr(href)').getall()
        links = [response.urljoin(h) for h in links]
        links = list(dict.fromkeys(links))
        self.logger.info("Found %d product links", len(links))

        for url in links:
            yield response.follow(url, callback=self.parse_product)

        # 2) Pagination (bol can vary; try several patterns)
        next_page = (
            response.css('a[rel="next"]::attr(href)').get()
            or response.css('a[data-test="pagination-next"]::attr(href)').get()
            or response.css('a[aria-label*="Volgende"]::attr(href)').get()
            or response.css('a[aria-label*="Next"]::attr(href)').get()
        )
        if next_page:
            yield response.follow(next_page, callback=self.parse)

    def parse_product(self, response):
        # Base item (always yield at least URL + time)
        item = {
            "shop": "bol",
            "category_seed": "microfoons/7119",
            "source_url": response.url,
            "scraped_at": datetime.now(timezone.utc).isoformat(),

            "title": None,
            "brand": None,
            "description": None,
            "image_url": None,

            "currency": "EUR",
            "price": None,
            "price_raw": None,
            "availability_raw": None,
            "in_stock": None,

            "rating_value": None,
            "review_count": None,
            "rating_scale": 5,

            "breadcrumb_category": None,
            "breadcrumb_parent": None,
            "breadcrumb_url": None,
        }

        # A) JSON-LD extraction 
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
        for n in nodes:
            t = n.get("@type")
            if t == "Product" or (isinstance(t, list) and "Product" in t):
                product_ld = n
                break

        if product_ld:
            item["title"] = clean(product_ld.get("name"))
            item["description"] = clean(product_ld.get("description"))

            brand = product_ld.get("brand")
            if isinstance(brand, dict):
                item["brand"] = clean(brand.get("name"))
            elif isinstance(brand, str):
                item["brand"] = clean(brand)

            img = product_ld.get("image")
            if isinstance(img, list) and img:
                item["image_url"] = img[0]
            elif isinstance(img, str):
                item["image_url"] = img

            offers = product_ld.get("offers")
            if isinstance(offers, dict):
                p = offers.get("price")
                item["price_raw"] = str(p) if p is not None else None
                item["price"] = price_to_float(str(p)) if p is not None else None

                av = offers.get("availability")
                if isinstance(av, str):
                    item["availability_raw"] = av
                    item["in_stock"] = ("InStock" in av)

            agg = product_ld.get("aggregateRating")
            if isinstance(agg, dict):
                item["rating_value"] = agg.get("ratingValue")
                item["review_count"] = agg.get("reviewCount") or agg.get("ratingCount")

        # B) HTML fallbacks (if JSON-LD missing)
        if not item["title"]:
            item["title"] = clean(response.css("h1::text").get())

        if item["price"] is None:
            price_text = clean(" ".join(response.css('[data-test="price"] *::text').getall()))
            item["price_raw"] = item["price_raw"] or price_text
            item["price"] = price_to_float(price_text)

        # Breadcrumbs (category info)
        crumbs = [clean(x) for x in response.css('nav[aria-label*="breadcrumb"] a::text').getall() if clean(x)]
        if crumbs:
            item["breadcrumb_category"] = crumbs[-1]
            if len(crumbs) >= 2:
                item["breadcrumb_parent"] = crumbs[-2]
        crumb_hrefs = response.css('nav[aria-label*="breadcrumb"] a::attr(href)').getall()
        if crumb_hrefs:
            item["breadcrumb_url"] = response.urljoin(crumb_hrefs[-1])

        yield item
