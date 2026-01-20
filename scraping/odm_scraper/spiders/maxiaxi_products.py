# 0. Environment Setup


# Core
import re
import json
import time
import math
import hashlib
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from urllib.parse import urljoin, urlparse

import pandas as pd
import numpy as np

# Database
import sqlite3

# Scrapy
import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

print("Setup OK. Timestamp:", datetime.now().isoformat())

# 1. Global Configuration (MaxiAxi - Microfoons only)

from datetime import datetime

RUN_ID = datetime.now().strftime("%Y%m%d_%H%M%S")

CONFIG = {
    "run_id": RUN_ID,
    "currency": "EUR",
    "max_pages_per_category": 20,       # adjust if needed
    "download_delay_s": 1.25,
    "concurrent_requests": 4,
    "user_agent": "AUAS-ODM-Scraper/1.0 (educational use)",
    "timeout_s": 25,
}

# Task seed (use the exact URL you provided)
MAXIAXI_MICROFOONS_URL = (
    "https://www.maxiaxi.com/microfoons/"
    "?_gl=1*wun1p3*_up*MQ..*_gs*MQ.."
    "&gclid=CjwKCAiAybfLBhAjEiwAI0mBBtBabnYALUttCuiFDxKEWjAqyPC4M-DxsOfcTrDui7s_I3pyu6CXMxoClCwQAvD_BwE"
    "&gbraid=0AAAAADo6YHPbIVtlWk2zNZUOX0tH2Wu3R"
)

# Only MaxiAxi
RETAILERS = {
    "maxiaxi": {
        "name": "MaxiAxi",
        "base_url": "https://www.maxiaxi.com/",
        "is_marketplace": False,
        "policy_urls": {
            "shipping_returns": "https://www.maxiaxi.com/klantenservice/",
        },
        "expert_support_urls": {
            "advice": "https://www.maxiaxi.com/advies/",
        },
        "category_seeds": {
            "microphones": MAXIAXI_MICROFOONS_URL,
        }
    }
}

# Only the selected task category
CATEGORIES = [
    {"category_id": 1, "category_name": "Microphones", "key": "microphones"},
]

print("Config loaded. Retailers:", list(RETAILERS.keys()), "Run:", RUN_ID)
print("Seed:", MAXIAXI_MICROFOONS_URL)

import sqlite3
from pathlib import Path
from datetime import datetime, timezone

# 1. Run identifier

try:
    RUN_ID
except NameError:
    RUN_ID = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

# 2. Desktop path

DESKTOP_DIR = Path("/Users/feddekoster/Desktop")

if not DESKTOP_DIR.exists():
    raise OSError(
        f"Desktop directory not found at {DESKTOP_DIR}. "
        "Check that the username is correct or that Desktop exists."
    )

# Test write permissions explicitly
try:
    test_file = DESKTOP_DIR / ".odm_write_test"
    test_file.write_text("ok", encoding="utf-8")
    test_file.unlink()
except Exception as e:
    raise PermissionError(
        "Desktop is not writable by this Python/Jupyter process.\n\n"
        "macOS fix:\n"
        "System Settings → Privacy & Security → Files and Folders (or Full Disk Access)\n"
        "→ Enable Desktop access for the app running Jupyter (Terminal / VS Code / Anaconda).\n\n"
        f"Original error: {e}"
    )

# 3. Assignment output folder

OUT_DIR = DESKTOP_DIR / "ODM_Assignment2"
OUT_DIR.mkdir(parents=True, exist_ok=True)

DB_PATH = OUT_DIR / f"odm_competitor_benchmark_{RUN_ID}.sqlite"


# 4. Database schema

DDL = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS retailer (
  retailer_id INTEGER PRIMARY KEY AUTOINCREMENT,
  retailer_key TEXT UNIQUE NOT NULL,
  name TEXT NOT NULL,
  base_url TEXT NOT NULL,
  is_marketplace INTEGER NOT NULL,
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS category (
  category_id INTEGER PRIMARY KEY,
  category_key TEXT UNIQUE NOT NULL,
  category_name TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS product_page (
  product_page_id INTEGER PRIMARY KEY AUTOINCREMENT,
  retailer_id INTEGER NOT NULL,
  category_id INTEGER NOT NULL,
  url TEXT NOT NULL,
  retailer_product_id TEXT,
  page_title TEXT,
  brand TEXT,
  gtin_ean TEXT,
  last_seen_at TEXT NOT NULL,
  UNIQUE(retailer_id, url),
  FOREIGN KEY(retailer_id) REFERENCES retailer(retailer_id),
  FOREIGN KEY(category_id) REFERENCES category(category_id)
);

CREATE TABLE IF NOT EXISTS offer_observation (
  observation_id INTEGER PRIMARY KEY AUTOINCREMENT,
  product_page_id INTEGER NOT NULL,
  observed_at TEXT NOT NULL,
  price_current REAL,
  price_reference REAL,
  discount_pct REAL,
  promo_flag INTEGER,
  stock_text_raw TEXT,
  delivery_promise_text TEXT,
  currency TEXT NOT NULL,
  http_status INTEGER,
  scrape_run_id TEXT NOT NULL,
  FOREIGN KEY(product_page_id) REFERENCES product_page(product_page_id)
);

CREATE TABLE IF NOT EXISTS retailer_page_capture (
  capture_id INTEGER PRIMARY KEY AUTOINCREMENT,
  retailer_id INTEGER NOT NULL,
  page_type TEXT NOT NULL,
  source_url TEXT NOT NULL,
  captured_at TEXT NOT NULL,
  content_text TEXT,
  http_status INTEGER,
  scrape_run_id TEXT NOT NULL,
  FOREIGN KEY(retailer_id) REFERENCES retailer(retailer_id)
);
"""

# 5. Create database

def db_connect(path: Path):
    con = sqlite3.connect(str(path))
    con.execute("PRAGMA foreign_keys = ON;")
    return con

with db_connect(DB_PATH) as con:
    con.executescript(DDL)

from datetime import datetime, timezone

# 2b. Seed reference tables

def upsert_retailers(con):
    now = datetime.now(timezone.utc).isoformat()
    for r_key, r in RETAILERS.items():
        con.execute("""
            INSERT INTO retailer (retailer_key, name, base_url, is_marketplace, created_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(retailer_key) DO UPDATE SET
                name=excluded.name,
                base_url=excluded.base_url,
                is_marketplace=excluded.is_marketplace
        """, (r_key, r["name"], r["base_url"], int(r["is_marketplace"]), now))
    con.commit()

def seed_categories(con):
    for c in CATEGORIES:
        con.execute("""
            INSERT OR REPLACE INTO category (category_id, category_key, category_name)
            VALUES (?, ?, ?)
        """, (c["category_id"], c["key"], c["category_name"]))
    con.commit()

with db_connect() as con:  
    upsert_retailers(con)
    seed_categories(con)

from pathlib import Path

OUTPUT_DIR = OUT_DIR  
PROD_CSV_PATH = OUTPUT_DIR / f"product_observations_{RUN_ID}.csv"
RET_CSV_PATH  = OUTPUT_DIR / f"retailer_pages_{RUN_ID}.csv"

# 3. Helper functions (Scraper 2.0 - MaxiAxi Microfoons)

import re
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

def clean_text(x):
    if x is None:
        return None
    x = str(x)
    x = re.sub(r"\s+", " ", x).strip()
    return x if x else None

def parse_price(price_text):
    """
    Parse price strings like:
    '€ 39,95' -> 39.95
    '39,95'   -> 39.95
    """
    if not price_text:
        return None
    t = clean_text(price_text)
    if not t:
        return None

    # Keep digits, comma, dot
    t = re.sub(r"[^\d,\.]", "", t)

    # If comma is used as decimal separator
    # Example: 39,95 -> 39.95
    if t.count(",") == 1 and t.count(".") == 0:
        t = t.replace(",", ".")
    # If both appear, assume dot is thousands and comma is decimal: 1.299,95 -> 1299.95
    elif t.count(",") == 1 and t.count(".") >= 1:
        t = t.replace(".", "").replace(",", ".")

    try:
        return float(t)
    except:
        return None

def calc_discount_pct(price_reference, price_current):
    if price_reference is None or price_current is None:
        return None
    if price_reference <= 0:
        return None
    if price_current >= price_reference:
        return 0.0
    return round((price_reference - price_current) / price_reference * 100.0, 2)

def strip_tracking(url: str) -> str:
    """
    Benchmark-inspired URL normaliser (based on your classmate’s bol_products.py).
    Removes common marketing/tracking parameters so URLs are stable across runs.
    """
    if not url:
        return url
    try:
        p = urlparse(url)
        q = parse_qs(p.query)

        drop_keys = {
            "gclid", "gbraid", "wbraid", "fbclid",
            "utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content",
            "_gl", "_ga", "_gid", "mc_cid", "mc_eid",
            "ref", "cid", "source"
        }

        for k in list(q.keys()):
            if k.lower() in drop_keys:
                q.pop(k, None)

        new_query = urlencode(q, doseq=True)
        return urlunparse((p.scheme, p.netloc, p.path, p.params, new_query, p.fragment))
    except Exception:
        return url

# 4. Scrapy Items

class ProductObservationItem(scrapy.Item):
    retailer_key = scrapy.Field()
    category_key = scrapy.Field()
    product_url = scrapy.Field()
    page_title = scrapy.Field()
    retailer_product_id = scrapy.Field()
    brand = scrapy.Field()
    gtin_ean = scrapy.Field()

    price_current = scrapy.Field()
    price_reference = scrapy.Field()
    discount_pct = scrapy.Field()
    promo_flag = scrapy.Field()

    stock_text_raw = scrapy.Field()
    delivery_promise_text = scrapy.Field()

    observed_at = scrapy.Field()
    http_status = scrapy.Field()
    scrape_run_id = scrapy.Field()

class RetailerPageItem(scrapy.Item):
    retailer_key = scrapy.Field()
    page_type = scrapy.Field()     # 'policy' or 'expert_support'
    source_url = scrapy.Field()
    captured_at = scrapy.Field()
    content_text = scrapy.Field()
    http_status = scrapy.Field()
    scrape_run_id = scrapy.Field()

# 5. Spider (Scraper 2.0 - MaxiAxi Microfoons)

import scrapy
from urllib.parse import urljoin
from datetime import datetime, timezone
import re

class CompetitorBenchmarkSpider(scrapy.Spider):
    name = "maxiaxi_microfoons"
    allowed_domains = ["www.maxiaxi.com", "maxiaxi.com"]

    custom_settings = {
        "USER_AGENT": CONFIG["user_agent"],
        "DOWNLOAD_DELAY": CONFIG["download_delay_s"],
        "CONCURRENT_REQUESTS": CONFIG["concurrent_requests"],
        "LOG_LEVEL": "INFO",
        "DOWNLOAD_TIMEOUT": CONFIG["timeout_s"],
        "ROBOTSTXT_OBEY": True,
        "ITEM_PIPELINES": {
            "__main__.SQLitePipeline": 300,
            "__main__.CSVPipeline": 400,
        }
    }

    def start_requests(self):
        r_key = "maxiaxi"
        r = RETAILERS[r_key]

        # Retailer-level pages (optional but useful for ERD completeness)
        for page_type, url in (r.get("policy_urls") or {}).items():
            yield scrapy.Request(
                url=strip_tracking(url),
                callback=self.parse_retailer_page,
                meta={"retailer_key": r_key, "page_type": page_type}
            )

        for page_type, url in (r.get("expert_support_urls") or {}).items():
            yield scrapy.Request(
                url=strip_tracking(url),
                callback=self.parse_retailer_page,
                meta={"retailer_key": r_key, "page_type": page_type}
            )

        # Category seed: microphones only
        seed = r["category_seeds"]["microphones"]
        yield scrapy.Request(
            url=strip_tracking(seed),
            callback=self.parse_listing,
            meta={"retailer_key": r_key, "category_key": "microphones", "page_no": 1}
        )

    def parse_retailer_page(self, response):
        retailer_key = response.meta["retailer_key"]
        page_type = response.meta["page_type"]

        body_text = clean_text(" ".join(response.css("body *::text").getall()))
        if body_text:
            body_text = body_text[:5000]

        yield RetailerPageItem(
            retailer_key=retailer_key,
            page_type=page_type,
            source_url=strip_tracking(response.url),
            captured_at=datetime.now(timezone.utc).isoformat(),
            content_text=body_text,
            http_status=response.status,
            scrape_run_id=RUN_ID
        )

    def parse_listing(self, response):
        retailer_key = response.meta["retailer_key"]
        category_key = response.meta["category_key"]
        page_no = response.meta.get("page_no", 1)

        self.logger.info("LISTING page=%s status=%s url=%s", page_no, response.status, response.url)

        raw_links = response.css(
            "ol.products li.product-item a.product-item-link::attr(href),"
            "a.product-item-link::attr(href)"
        ).getall()

        links = [strip_tracking(urljoin(response.url, h)) for h in raw_links if h]
        links = list(dict.fromkeys(links))

        def is_product_url(u: str) -> bool:
            if not u or "maxiaxi.com" not in u:
                return False

            low = u.lower()

            # Drop category itself and irrelevant areas
            if "/microfoons/" in low:
                return False
            if any(x in low for x in ["/klantenservice", "/advies", "/blog", "/account", "/checkout"]):
                return False

            # MaxiAxi product URLs often look like:
            # https://www.maxiaxi.com/<slug>/
            return bool(re.match(r"^https://www\.maxiaxi\.com/[^/]+/?$", u))

        product_links = [u for u in links if is_product_url(u)]

        self.logger.info("LISTING found_links=%s product_links=%s", len(links), len(product_links))

        for u in product_links:
            yield scrapy.Request(
                url=u,
                callback=self.parse_product,
                meta={"retailer_key": retailer_key, "category_key": category_key}
            )

        # Pagination
        if page_no < CONFIG["max_pages_per_category"]:
            next_href = response.css(
                "li.pages-item-next a::attr(href), a.action.next::attr(href)"
            ).get()

            if next_href:
                next_url = strip_tracking(urljoin(response.url, next_href))
                yield scrapy.Request(
                    url=next_url,
                    callback=self.parse_listing,
                    meta={"retailer_key": retailer_key, "category_key": category_key, "page_no": page_no + 1}
                )

    def parse_product(self, response):
        retailer_key = response.meta["retailer_key"]
        category_key = response.meta["category_key"]

        product_url = strip_tracking(response.url)

        page_title = clean_text(response.css("h1.page-title span::text, h1::text").get())
        if not page_title:
            page_title = clean_text(response.css("title::text").get())

        # Prices
        price_cur_raw = clean_text(response.css(
            "span.price-final_price span.price::text,"
            "span.special-price span.price::text,"
            "span.price-wrapper span.price::text"
        ).get() or "")

        price_ref_raw = clean_text(response.css(
            "span.old-price span.price::text,"
            "span.regular-price span.price::text"
        ).get() or "")

        price_current = parse_price(price_cur_raw)
        price_reference = parse_price(price_ref_raw)
        discount_pct = calc_discount_pct(price_reference, price_current)
        promo_flag = int(discount_pct is not None and discount_pct > 0)

        # Stock
        stock_text_raw = clean_text(" ".join(response.css(
            ".stock.available *::text, .stock.unavailable *::text, .availability *::text"
        ).getall()))

        # Delivery promise
        delivery_promise_text = clean_text(" ".join(response.xpath(
            "//*[contains(normalize-space(), 'Bestel voor') or contains(normalize-space(), 'morgen')]/text()"
        ).getall()))

        # Specs extraction
        def value_after_label(label: str):
            v = response.xpath(
                f"//th[normalize-space()='{label}']/following-sibling::td[1]//text()"
            ).getall()
            if v:
                return clean_text(" ".join(v))

            v2 = response.xpath(
                f"//*[normalize-space()='{label}']/following::*[1]//text()"
            ).getall()
            return clean_text(" ".join(v2)) if v2 else None

        brand = value_after_label("Merk")
        retailer_product_id = value_after_label("SKU")     # mapping SKU -> retailer_product_id
        gtin_ean = value_after_label("EAN Code")

        yield ProductObservationItem(
            retailer_key=retailer_key,
            category_key=category_key,
            product_url=product_url,
            page_title=page_title,
            retailer_product_id=retailer_product_id,
            brand=brand,
            gtin_ean=gtin_ean,

            price_current=price_current,
            price_reference=price_reference,
            discount_pct=discount_pct,
            promo_flag=promo_flag,

            stock_text_raw=stock_text_raw,
            delivery_promise_text=delivery_promise_text,

            observed_at=datetime.now(timezone.utc).isoformat(),
            http_status=response.status,
            scrape_run_id=RUN_ID
        )
import pandas as pd

# 6. Pipelines


class SQLitePipeline:
    def open_spider(self, spider):
        self.con = db_connect()
        self.cur = self.con.cursor()

    def close_spider(self, spider):
        self.con.commit()
        self.con.close()

    def _get_retailer_id(self, retailer_key):
        row = self.cur.execute(
            "SELECT retailer_id FROM retailer WHERE retailer_key=?",
            (retailer_key,)
        ).fetchone()
        if not row:
            raise RuntimeError(
                f"Retailer '{retailer_key}' not found in DB. "
                "Did you run the seeding cell (upsert_retailers)?"
            )
        return row[0]

    def _get_category_id(self, category_key):
        row = self.cur.execute(
            "SELECT category_id FROM category WHERE category_key=?",
            (category_key,)
        ).fetchone()
        if not row:
            raise RuntimeError(
                f"Category '{category_key}' not found in DB. "
                "Did you run the seeding cell (seed_categories)?"
            )
        return row[0]

    def _upsert_product_page(self, retailer_id, category_id, item):
        now = datetime.now(timezone.utc).isoformat()
        self.cur.execute("""
            INSERT INTO product_page (
              retailer_id, category_id, url, retailer_product_id, page_title, brand, gtin_ean, last_seen_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(retailer_id, url) DO UPDATE SET
                retailer_product_id=excluded.retailer_product_id,
                page_title=excluded.page_title,
                brand=excluded.brand,
                gtin_ean=excluded.gtin_ean,
                last_seen_at=excluded.last_seen_at
        """, (
            retailer_id, category_id,
            item.get("product_url"),
            item.get("retailer_product_id"),
            item.get("page_title"),
            item.get("brand"),
            item.get("gtin_ean"),
            now
        ))
        row = self.cur.execute(
            "SELECT product_page_id FROM product_page WHERE retailer_id=? AND url=?",
            (retailer_id, item.get("product_url"))
        ).fetchone()
        return row[0]

    def process_item(self, item, spider):
        d = dict(item)

        # Retailer-level pages (policy/support)
        if isinstance(item, RetailerPageItem):
            retailer_id = self._get_retailer_id(d["retailer_key"])
            self.cur.execute("""
                INSERT INTO retailer_page_capture (
                  retailer_id, page_type, source_url, captured_at, content_text, http_status, scrape_run_id
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                retailer_id,
                d["page_type"],
                d["source_url"],
                d["captured_at"],
                d.get("content_text"),
                d.get("http_status"),
                d["scrape_run_id"]
            ))
            self.con.commit()
            return item

        # Product observation (price/stock/delivery)
        if isinstance(item, ProductObservationItem):
            retailer_id = self._get_retailer_id(d["retailer_key"])
            category_id = self._get_category_id(d["category_key"])
            pp_id = self._upsert_product_page(retailer_id, category_id, d)

            self.cur.execute("""
                INSERT INTO offer_observation (
                  product_page_id, observed_at, price_current, price_reference, discount_pct,
                  promo_flag, stock_text_raw, delivery_promise_text, currency, http_status, scrape_run_id
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                pp_id,
                d["observed_at"],
                d.get("price_current"),
                d.get("price_reference"),
                d.get("discount_pct"),
                d.get("promo_flag"),
                d.get("stock_text_raw"),
                d.get("delivery_promise_text"),
                CONFIG["currency"],
                d.get("http_status"),
                d["scrape_run_id"]
            ))
            self.con.commit()
            return item

        return item


class CSVPipeline:
    def open_spider(self, spider):
        self.products = []
        self.pages = []

    def close_spider(self, spider):
        if self.products:
            dfp = pd.DataFrame(self.products)
            dfp.to_csv(PROD_CSV_PATH, index=False)

        if self.pages:
            dfr = pd.DataFrame(self.pages)
            dfr.to_csv(RET_CSV_PATH, index=False)

        print("CSV saved to:")
        if self.products:
            print("-", PROD_CSV_PATH)
        if self.pages:
            print("-", RET_CSV_PATH)

    def process_item(self, item, spider):
        if isinstance(item, ProductObservationItem):
            self.products.append(dict(item))
        elif isinstance(item, RetailerPageItem):
            self.pages.append(dict(item))
        return item

# 7. Run Spider (ONE run method only)

from scrapy.crawler import CrawlerProcess

# IMPORTANT:
# Twisted reactor can only run once per Jupyter kernel.
# If you want to run the spider again:
# Kernel -> Restart Kernel -> Run all cells once

process = CrawlerProcess(settings={})
process.crawl(CompetitorBenchmarkSpider)
process.start()

print("Scrape finished. Outputs saved:")
print("-", PROD_CSV_PATH)
print("-", RET_CSV_PATH)
print("-", DB_PATH)

# 8. Load and QA

import pandas as pd

df_prod = pd.read_csv(PROD_CSV_PATH) if PROD_CSV_PATH.exists() else pd.DataFrame()
df_ret  = pd.read_csv(RET_CSV_PATH)  if RET_CSV_PATH.exists()  else pd.DataFrame()

display(df_prod.head(10))
display(df_ret.head(10))

if not df_prod.empty:
    print("Rows:", len(df_prod))
    print("\nRows per retailer:")
    print(df_prod["retailer_key"].value_counts(dropna=False))

    print("\nHTTP status distribution:")
    print(df_prod["http_status"].value_counts(dropna=False).head(10))

    print("\nMissing price_current %:", round(df_prod["price_current"].isna().mean()*100, 2))
    print("Discount pct summary:")
    display(df_prod["discount_pct"].describe())

# 9. Export DB tables

with db_connect() as con:
    for table in ["retailer", "category", "product_page", "offer_observation", "retailer_page_capture"]:
        df = pd.read_sql_query(f"SELECT * FROM {table}", con)
        out = OUTPUT_DIR / f"{table}_{RUN_ID}.csv"
        df.to_csv(out, index=False)
        print("Exported:", out, "rows:", len(df))
