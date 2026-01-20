from __future__ import annotations

import json
import os
import re
import uuid
import subprocess
from pathlib import Path
from datetime import datetime, timezone
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

import scrapy


# -------------------------
# category alignment keywords
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

PRIORITY_CATEGORY_KEYWORDS = [
    "studiomicrofoon",
    "live-microfoon",
    "draadloze-microfoon",
    "usb-microfoon",
    "multimedia-av-microfoon",
    "zang-microfoon",
    "microfoon-opnameset",
]

EXCLUDED_CATEGORY_KEYWORDS = {
    "accessoire",
    "accessoires",
    "toebehoren",
    "onderdeel",
    "onderdelen",
    "statieven",
    "statief",
    "kabel",
    "kabels",
    "clip",
    "clips",
    "klem",
    "klemmen",
    "windkap",
    "windkappen",
    "popfilter",
    "popfilters",
    "capsule",
    "capsules",
    "shockmount",
    "shockmounts",
    "pistoolgreep",
    "pistoolgrepen",
    "opbergtassen",
    "hoezen",
    "flightcase",
    "flightcases",
    "accu",
    "lader",
    "laders",
    "booster",
    "boosters",
    "reflectiefilter",
    "reflectiefilters",
    "voorversterker",
    "voorversterkers",
    "vocal-effect",
    "vocal-effecten",
}


# -------------------------
# helpers
# -------------------------

def clean(text):
    if text is None:
        return None
    s = re.sub(r"\s+", " ", str(text)).strip()
    return s or None


def brightdata_mode() -> str:
    # Unlocker has priority if token+zone are set
    if os.getenv("BRIGHTDATA_TOKEN") and os.getenv("BRIGHTDATA_ZONE"):
        return "unlocker_api"

    # Proxy mode: either full proxy URL, or user/pass + host/port
    if os.getenv("BRIGHTDATA_PROXY"):
        return "proxy"
    if (
        os.getenv("BRIGHTDATA_USERNAME")
        and os.getenv("BRIGHTDATA_PASSWORD")
        and os.getenv("BRIGHTDATA_HOST")
        and os.getenv("BRIGHTDATA_PORT")
    ):
        return "proxy"

    return "disabled"


def build_proxy_url() -> str | None:
    """
    Builds proxy URL from env vars.
    Supports:
      - BRIGHTDATA_PROXY  (full url)
      - BRIGHTDATA_USERNAME / BRIGHTDATA_PASSWORD / BRIGHTDATA_HOST / BRIGHTDATA_PORT
    """
    p = os.getenv("BRIGHTDATA_PROXY")
    if p:
        return p

    user = os.getenv("BRIGHTDATA_USERNAME")
    pwd = os.getenv("BRIGHTDATA_PASSWORD")
    host = os.getenv("BRIGHTDATA_HOST")
    port = os.getenv("BRIGHTDATA_PORT")
    if user and pwd and host and port:
        return f"http://{user}:{pwd}@{host}:{port}"

    return None


def apply_brightdata_meta(request: scrapy.Request) -> scrapy.Request:
    """
    Adds request.meta hints for Bright Data usage.
    - In proxy mode: sets request.meta['proxy'] so Scrapy uses it.
    - In unlocker_api mode: sets request.meta['brightdata'] for middleware (if any).
    """
    mode = brightdata_mode()
    if mode == "proxy":
        proxy_url = build_proxy_url()
        if proxy_url:
            request.meta["proxy"] = proxy_url
        request.meta["brightdata_mode"] = "proxy"
    elif mode == "unlocker_api":
        # Many Bright Data unlocker middlewares look for a meta config block
        request.meta["brightdata_mode"] = "unlocker_api"
        request.meta.setdefault("brightdata", {})
        request.meta["brightdata"].update({
            "token_env": "BRIGHTDATA_TOKEN",
            "zone_env": "BRIGHTDATA_ZONE",
        })
    else:
        request.meta["brightdata_mode"] = "disabled"
    return request


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
            if k.lower() in {"cid", "bltgh", "bltg", "blt", "ref", "promo"}:
                q.pop(k, None)
        new_query = urlencode(q, doseq=True)
        return urlunparse((p.scheme, p.netloc, p.path, p.params, new_query, p.fragment))
    except Exception:
        return url


def looks_like_category_url(url: str) -> bool:
    if not url:
        return False
    u = url.lower()
    if "/prijsoverzicht/" in u:
        return False
    return "/l/" in u


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


def looks_blocked_title(title: str | None) -> bool:
    if not title:
        return False
    t = title.lower()
    needles = [
        "toestemming", "consent", "cookie",
        "verify", "verific", "access denied", "blocked",
        "captcha", "robot", "attention required",
    ]
    return any(n in t for n in needles)


def _norm_tokens(text: str | None) -> set[str]:
    if not text:
        return set()
    s = text.lower()
    s = re.sub(r"[^a-z0-9]+", " ", s)
    parts = [p for p in s.split() if p]
    stop = {"de", "het", "een", "and", "the", "with", "voor", "met"}
    return {p for p in parts if p not in stop and len(p) > 1}


def _keyword_hit(haystack: str, keywords: set[str]) -> bool:
    hs = (haystack or "").lower()
    for kw in keywords:
        if kw in hs:
            return True
        if "-" in kw and kw.replace("-", " ") in hs:
            return True
    return False


def should_keep_item(item: dict) -> tuple[bool, str]:
    query_title = item.get("query_title")

    blob_parts = [
        item.get("title"),
        item.get("breadcrumb_category"),
        item.get("breadcrumb_parent"),
        item.get("breadcrumb_url"),
        item.get("source_url"),
    ]
    blob = " ".join([p for p in blob_parts if p]).lower()

    if _keyword_hit(blob, EXCLUDED_CATEGORY_KEYWORDS):
        return False, "excluded_keyword"

    if not query_title:
        if _keyword_hit(blob, ALLOWED_CATEGORY_KEYWORDS):
            return True, "allowed_keyword"
        return False, "not_in_allowed_categories"

    return True, "search_mode_keep"


def extract_query_from_bax_title(title: str | None) -> str | None:
    t = clean(title)
    if not t:
        return None
    toks = t.split()
    if not toks:
        return t

    brand = toks[0]
    modelish = []
    for tok in toks[1:]:
        if re.search(r"[A-Za-z]+\d|\d+[A-Za-z]", tok) and 2 <= len(tok) <= 15:
            modelish.append(tok.strip("()[]{}.,;:"))
        elif tok.isupper() and 2 <= len(tok) <= 6:
            modelish.append(tok.strip("()[]{}.,;:"))

    modelish = list(dict.fromkeys([m for m in modelish if m]))
    if modelish:
        return " ".join([brand] + modelish[:3])

    return " ".join(toks[:3])


def is_bax_input_allowed(bax_item: dict) -> tuple[bool, str]:
    title = clean(bax_item.get("title") or bax_item.get("name"))
    url = clean(bax_item.get("source_url") or bax_item.get("url"))
    breadcrumb = clean(bax_item.get("breadcrumb_category")) or ""

    blob = " ".join([x for x in [title, url, breadcrumb] if x]).lower()

    if _keyword_hit(blob, EXCLUDED_CATEGORY_KEYWORDS):
        return False, "bax_excluded_keyword"

    if "microfoon" not in blob and "microfoons" not in blob:
        return False, "bax_not_microfoonish"

    return True, "bax_ok"


# -------------------------
# spider
# -------------------------

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
        "CLOSESPIDER_PAGECOUNT": 200,
    }

    crawler_version = "bol_products/RAW-2.4-brightdata-proxy-or-unlocker"

    def __init__(self, *args, bax_json_path=None, max_titles=50, debug_dump=1, **kwargs):
        super().__init__(*args, **kwargs)
        self.scrape_run_id = str(uuid.uuid4())
        self.started_at = datetime.now(timezone.utc).isoformat()
        self.git_commit_hash = get_git_commit_hash()

        self.bd_mode = brightdata_mode()
        self.bax_json_path = bax_json_path
        try:
            self.max_titles = int(max_titles)
        except Exception:
            self.max_titles = 50

        self.debug_dump = str(debug_dump).strip() not in {"0", "false", "False", "no", "NO"}

    def _dump_response(self, response, label: str):
        if not self.debug_dump:
            return
        try:
            out_dir = Path("debug")
            out_dir.mkdir(parents=True, exist_ok=True)
            fn = out_dir / f"{self.name}_{label}_{response.status}.html"
            fn.write_bytes(response.body or b"")
            self.logger.warning("Saved debug HTML to %s", fn.resolve())
        except Exception as exc:
            self.logger.warning("Could not save debug HTML err=%s", exc)

    def load_bax_items(self) -> list[dict]:
        if not self.bax_json_path:
            return []

        items = []
        path = self.bax_json_path

        try:
            with open(path, "r", encoding="utf-8") as f:
                first = f.read(1)
                f.seek(0)

                def add_obj(obj):
                    if not isinstance(obj, dict):
                        return
                    title = clean(obj.get("title") or obj.get("name"))
                    source_url = clean(obj.get("source_url") or obj.get("url"))
                    if not title:
                        return
                    items.append({
                        "title": title,
                        "source_url": source_url,
                        "seed_category": clean(obj.get("seed_category")),
                        "breadcrumb_category": clean(obj.get("breadcrumb_category")),
                    })

                if first == "[":
                    data = json.load(f)
                    if isinstance(data, list):
                        for obj in data:
                            add_obj(obj)
                else:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            obj = json.loads(line)
                        except Exception:
                            continue
                        add_obj(obj)

        except Exception as exc:
            self.logger.warning("Could not read bax_json_path=%s err=%s", path, exc)
            return []

        seen = set()
        deduped = []
        for it in items:
            key = (it.get("title"), it.get("source_url"))
            if key in seen:
                continue
            seen.add(key)
            deduped.append(it)

        return deduped[: self.max_titles]

    def start_requests(self):
        # guardrails
        if self.bd_mode == "disabled":
            raise RuntimeError(
                "Bright Data is required. Set BRIGHTDATA_TOKEN+BRIGHTDATA_ZONE (Unlocker) "
                "OR BRIGHTDATA_PROXY / (USERNAME+PASSWORD+HOST+PORT) (proxy mode)."
            )

        # warn if both are set
        if (os.getenv("BRIGHTDATA_TOKEN") and os.getenv("BRIGHTDATA_ZONE")) and (
            os.getenv("BRIGHTDATA_PROXY")
            or (os.getenv("BRIGHTDATA_USERNAME") and os.getenv("BRIGHTDATA_PASSWORD"))
        ):
            self.logger.warning("Both Unlocker API and proxy creds are set; using Unlocker API.")

        yield {
            "type": "run",
            "scrape_run_id": self.scrape_run_id,
            "started_at": self.started_at,
            "git_commit_hash": self.git_commit_hash,
            "crawler_version": self.crawler_version,
            "notes": "bol crawl (category fallback OR bax-item search input)",
            "brightdata_mode": self.bd_mode,
            "bax_json_path": self.bax_json_path,
            "max_titles": self.max_titles,
        }

        bax_items = self.load_bax_items()
        if bax_items:
            for bax in bax_items:
                ok, _reason = is_bax_input_allowed(bax)
                if not ok:
                    continue

                query = extract_query_from_bax_title(bax["title"]) or bax["title"]
                search_url = "https://www.bol.com/nl/nl/s/?" + urlencode({"searchtext": query})

                req = scrapy.Request(
                    search_url,
                    callback=self.parse_search,
                    meta={
                        "query_title": bax["title"],
                        "query_text": query,
                        "bax_source_url": bax.get("source_url"),
                        "bax_seed_category": bax.get("seed_category"),
                        "bax_breadcrumb_category": bax.get("breadcrumb_category"),
                    },
                )
                yield apply_brightdata_meta(req)
            return

        # fallback crawl
        for url in self.start_urls:
            req = scrapy.Request(url, callback=self.parse)
            yield apply_brightdata_meta(req)

    def parse_search(self, response):
        title = clean(response.css("title::text").get())
        self.logger.info("SEARCH status=%s url=%s title=%s", response.status, response.url, title)

        if response.status in (403, 429, 503) or looks_blocked_title(title):
            self._dump_response(response, "search_blocked")
            return

        query_title = clean(response.meta.get("query_title"))
        query_text = clean(response.meta.get("query_text"))

        links = response.css('a[data-test="product-title"]::attr(href)').getall()
        if not links:
            links = response.css('li[data-test="product-item"] a[href*="/nl/nl/p/"]::attr(href)').getall()
        if not links:
            links = response.css('a[href*="/nl/nl/p/"]::attr(href)').getall()

        links = [strip_tracking(response.urljoin(h)) for h in links if h]
        links = list(dict.fromkeys(links))
        if not links:
            self._dump_response(response, "search_no_links")
            return

        q_tokens = _norm_tokens(query_title or query_text)
        scored = []
        for a in response.css('a[data-test="product-title"]'):
            href = a.attrib.get("href")
            if not href:
                continue
            url = strip_tracking(response.urljoin(href))
            txt = clean(" ".join(a.css("*::text").getall())) or ""
            t_tokens = _norm_tokens(txt)
            overlap = len(q_tokens & t_tokens)
            scored.append((overlap, url))

        if scored:
            scored.sort(key=lambda x: x[0], reverse=True)
            top_urls, seen = [], set()
            for _, u in scored:
                if u in seen:
                    continue
                seen.add(u)
                top_urls.append(u)
                if len(top_urls) >= 3:
                    break
        else:
            top_urls = links[:3]

        for url in top_urls:
            req = response.follow(
                url,
                callback=self.parse_product,
                meta={
                    "query_title": query_title,
                    "query_text": query_text,
                    "bax_source_url": response.meta.get("bax_source_url"),
                    "bax_seed_category": response.meta.get("bax_seed_category"),
                    "bax_breadcrumb_category": response.meta.get("bax_breadcrumb_category"),
                },
            )
            yield apply_brightdata_meta(req)

    def parse(self, response):
        title = clean(response.css("title::text").get())
        self.logger.info("LISTING status=%s url=%s title=%s", response.status, response.url, title)

        if response.status in (403, 429, 503) or looks_blocked_title(title):
            self._dump_response(response, "listing_blocked")
            return

        links = response.css('a[data-test="product-title"]::attr(href)').getall()
        if not links:
            links = response.css('li[data-test="product-item"] a[href*="/nl/nl/p/"]::attr(href)').getall()
        if not links:
            links = response.css('a[href*="/nl/nl/p/"]::attr(href)').getall()

        links = [strip_tracking(response.urljoin(h)) for h in links if h]
        links = list(dict.fromkeys(links))

        if not links:
            self._dump_response(response, "listing_no_links")
            return

        for url in links:
            req = response.follow(url, callback=self.parse_product)
            yield apply_brightdata_meta(req)

        next_page = (
            response.css('a[rel="next"]::attr(href)').get()
            or response.css('a[data-test="pagination-next"]::attr(href)').get()
            or response.css('a[aria-label*="Volgende"]::attr(href)').get()
            or response.css('a[aria-label*="Next"]::attr(href)').get()
        )
        if next_page:
            req = response.follow(next_page, callback=self.parse)
            yield apply_brightdata_meta(req)

    def parse_product(self, response):
        scraped_at = datetime.now(timezone.utc).isoformat()
        source_url = strip_tracking(response.url)

        item = {
            "type": "product",
            "scrape_run_id": self.scrape_run_id,
            "scraped_at": scraped_at,
            "source_url": source_url,

            "query_title": clean(response.meta.get("query_title")),
            "query_text": clean(response.meta.get("query_text")),
            "bax_source_url": clean(response.meta.get("bax_source_url")),
            "bax_seed_category": clean(response.meta.get("bax_seed_category")),
            "bax_breadcrumb_category": clean(response.meta.get("bax_breadcrumb_category")),

            "seed_category": "microfoons/7119",

            "title": None,
            "brand": None,
            "model": None,
            "canonical_name": None,
            "gtin": None,
            "mpn": None,
            "sku": None,

            "description": None,
            "image_url": None,

            "currency": "EUR",
            "current_price": None,
            "base_price": None,
            "discount_amount": None,
            "discount_percent": None,
            "price_text": None,
            "in_stock": None,
            "stock_status_text": None,

            "rating_value": None,
            "rating_scale": 5,
            "review_count": None,

            "breadcrumb_category": None,
            "breadcrumb_parent": None,
            "breadcrumb_url": None,

            "kept": None,
            "drop_reason": None,
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
            names = [n for n in names if n]
            urls = [u for u in urls if u]

            cat_candidates = [(n, u) for n, u in zip(names, urls) if u and looks_like_category_url(u)]
            if cat_candidates:
                item["breadcrumb_category"], item["breadcrumb_url"] = cat_candidates[-1]
                if len(cat_candidates) >= 2:
                    item["breadcrumb_parent"] = cat_candidates[-2][0]

        # HTML fallbacks
        if not item["title"]:
            item["title"] = (
                clean(response.css("h1::text").get())
                or meta_content(response, "og:title")
                or clean(response.css("title::text").get())
            )
            if item["title"]:
                item["title"] = re.sub(r"\s*\|\s*bol\s*$", "", item["title"], flags=re.IGNORECASE).strip()

        if not item["brand"]:
            item["brand"] = (
                clean(response.css('[data-test="brandLink"]::text').get())
                or clean(response.css('a[href*="/nl/nl/b/"]::text').get())
                or meta_content(response, "product:brand")
            )

        if not item["image_url"]:
            item["image_url"] = meta_content(response, "og:image")

        if not item["description"]:
            item["description"] = meta_content(response, "description", "og:description")

        # Price parsing
        buy_block = response.css('[data-test="buy-block"], [data-test="buybox"], [data-test="buyBox"]')
        buy_text = clean(" ".join(buy_block.css("*::text").getall())) if buy_block else None

        if item["current_price"] is None:
            price_text = None
            if buy_block:
                candidates = buy_block.css('[data-test*="price"] *::text').getall()
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

        # Identifier fallbacks (EAN/GTIN regex)
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

        keep, reason = should_keep_item(item)
        item["kept"] = keep
        item["drop_reason"] = None if keep else reason
        if not keep:
            return

        yield item
