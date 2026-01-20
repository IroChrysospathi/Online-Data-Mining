# =========================
# MaxiAxi Microfoons Scraper (DB schema aligned)
# Writes to existing repo SQLite DB (db/odm.sqlite) + JSONL output
# Blocked-page detection + debug HTML dump
# NO CSV output
# =========================

import json
import os
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urljoin, urlparse, parse_qs, urlencode, urlunparse

import pandas as pd
import scrapy
from scrapy.crawler import CrawlerProcess


# =========================
# 1) CONFIG
# =========================

RUN_ID = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

CONFIG = {
    "run_id": RUN_ID,
    "currency": "EUR",
    "max_pages_per_category": 20,
    "download_delay_s": 1.25,
    "concurrent_requests": 4,
    "user_agent": "AUAS-ODM-Scraper/1.0 (educational use)",
    "timeout_s": 25,
    "debug_dump": str(os.getenv("DEBUG_DUMP", "1")).strip() not in {"0", "false", "False", "no", "NO"},
}

MAXIAXI_MICROFOONS_URL = (
    "https://www.maxiaxi.com/microfoons/"
    "?_gl=1*wun1p3*_up*MQ..*_gs*MQ.."
    "&gclid=CjwKCAiAybfLBhAjEiwAI0mBBtBabnYALUttCuiFDxKEWjAqyPC4M-DxsOfcTrDui7s_I3pyu6CXMxoClCwQAvD_BwE"
    "&gbraid=0AAAAADo6YHPbIVtlWk2zNZUOX0tH2Wu3R"
)

RETAILERS = {
    "maxiaxi": {
        "name": "MaxiAxi",
        "country": "NL",
        "base_url": "https://www.maxiaxi.com/",
        "is_marketplace": False,
        "policy_urls": {"shipping_returns": "https://www.maxiaxi.com/klantenservice/"},
        "expert_support_urls": {"advice": "https://www.maxiaxi.com/advies/"},
        "category_seeds": {"microphones": MAXIAXI_MICROFOONS_URL},
    }
}


# =========================
# 2) OUTPUT PATHS (JSON + DEBUG)
# =========================

DEFAULT_OUT_DIR = "~/Desktop/Assignment/Onlune-Data-Mining/data/raw/maxiaxi"
OUT_DIR = Path(os.getenv("OUTPUT_DIR", DEFAULT_OUT_DIR)).expanduser()
OUT_DIR.mkdir(parents=True, exist_ok=True)

DEBUG_DIR = OUT_DIR / "debug"
DEBUG_DIR.mkdir(parents=True, exist_ok=True)

JSONL_PATH = OUT_DIR / f"maxiaxi_items_{RUN_ID}.jsonl"
DB_EXPORT_JSON_PATH = OUT_DIR / f"odm_db_export_{RUN_ID}.json"


# =========================
# 3) DATABASE (CONNECT TO EXISTING REPO DB)
# =========================

def get_repo_root() -> Path:
    gh = os.getenv("GITHUB_WORKSPACE")
    if gh:
        return Path(gh).resolve()

    p = Path(__file__).resolve()
    if p.parent.name.lower() == "scraping":
        return p.parent.parent
    return p.parent


def get_db_path() -> Path:
    env = os.getenv("DB_PATH")
    if env:
        return Path(env).expanduser().resolve()
    return (get_repo_root() / "db" / "odm.sqlite").resolve()


DB_PATH = get_db_path()


def db_connect(path: Path) -> sqlite3.Connection:
    if not path.exists():
        raise FileNotFoundError(
            f"SQLite DB not found: {path}\n"
            "Expected: <repo>/db/odm.sqlite or set DB_PATH env var."
        )
    con = sqlite3.connect(str(path))
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA foreign_keys = ON;")
    return con


def table_exists(con: sqlite3.Connection, table: str) -> bool:
    row = con.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    ).fetchone()
    return row is not None


def get_table_columns(con: sqlite3.Connection, table: str) -> set[str]:
    cols = set()
    try:
        for r in con.execute(f"PRAGMA table_info({table})").fetchall():
            cols.add(r["name"])
    except sqlite3.Error:
        pass
    return cols


def ensure_competitor(con: sqlite3.Connection, retailer_key: str) -> int:
    r = RETAILERS[retailer_key]
    name = r["name"]
    country = r.get("country")
    base_url = r.get("base_url")

    row = con.execute("SELECT competitor_id FROM competitor WHERE name = ?", (name,)).fetchone()
    if row:
        cid = int(row["competitor_id"])
        con.execute(
            "UPDATE competitor SET country = COALESCE(?, country), base_url = COALESCE(?, base_url) WHERE competitor_id = ?",
            (country, base_url, cid),
        )
        con.commit()
        return cid

    con.execute(
        "INSERT INTO competitor (name, country, base_url) VALUES (?, ?, ?)",
        (name, country, base_url),
    )
    con.commit()
    return int(con.execute("SELECT last_insert_rowid() AS id").fetchone()["id"])


def ensure_category_row(
    con: sqlite3.Connection,
    competitor_id: int,
    name: str,
    url: str | None,
    parent_category_id: int | None = None,
) -> int:
    """
    category schema:
      category_id PK
      competitor_id NOT NULL
      name
      url
      parent_category_id
    """
    if not name:
        raise ValueError("Category.name is required.")

    row = con.execute(
        "SELECT category_id FROM category WHERE competitor_id = ? AND name = ?",
        (competitor_id, name),
    ).fetchone()

    if row:
        cat_id = int(row["category_id"])
        con.execute(
            "UPDATE category SET url = COALESCE(?, url), parent_category_id = COALESCE(?, parent_category_id) WHERE category_id = ?",
            (url, parent_category_id, cat_id),
        )
        con.commit()
        return cat_id

    con.execute(
        "INSERT INTO category (competitor_id, name, url, parent_category_id) VALUES (?, ?, ?, ?)",
        (competitor_id, name, url, parent_category_id),
    )
    con.commit()
    return int(con.execute("SELECT last_insert_rowid() AS id").fetchone()["id"])


# =========================
# 4) HELPERS
# =========================

def clean_text(x):
    if x is None:
        return None
    s = re.sub(r"\s+", " ", str(x)).strip()
    return s or None


def strip_tracking(url: str) -> str:
    if not url:
        return url
    try:
        p = urlparse(url)
        q = parse_qs(p.query)

        drop_keys = {
            "gclid", "gbraid", "wbraid", "fbclid",
            "utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content",
            "_gl", "_ga", "_gid", "mc_cid", "mc_eid",
            "ref", "cid", "source",
        }

        for k in list(q.keys()):
            if k.lower() in drop_keys:
                q.pop(k, None)

        new_query = urlencode(q, doseq=True)
        return urlunparse((p.scheme, p.netloc, p.path, p.params, new_query, p.fragment))
    except Exception:
        return url


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


def is_blocked_response(response: scrapy.http.Response) -> bool:
    title = clean_text(response.css("title::text").get())
    if response.status in (403, 429, 503):
        return True
    if looks_blocked_title(title):
        return True
    return False


# =========================
# 5) SCRAPY ITEMS
# =========================

class CategoryItem(scrapy.Item):
    competitor_key = scrapy.Field()
    name = scrapy.Field()
    url = scrapy.Field()
    parent_category_id = scrapy.Field()


class ProductListingItem(scrapy.Item):
    competitor_key = scrapy.Field()
    category_name = scrapy.Field()

    product_url = scrapy.Field()             # maps to productlisting.product_url
    product_name = scrapy.Field()            # maps to productlisting.product_name
    ean = scrapy.Field()                     # maps to productlisting.ean
    sku = scrapy.Field()                     # maps to productlisting.sku
    image_url_on_pdp = scrapy.Field()        # maps to productlisting.image_url_on_pdp

    # extras for JSONL only
    description_clean = scrapy.Field()
    brand = scrapy.Field()
    model = scrapy.Field()


class PageRawItem(scrapy.Item):
    competitor_key = scrapy.Field()
    url = scrapy.Field()


# =========================
# 6) PIPELINES (DB + JSONL OUTPUT)
# =========================

class SQLitePipeline:
    """
    DB schema alignment (your schema):

    productlisting(listing_id PK, competitor_id, category_id, product_url, product_name, ean, sku, image_url_on_pdp)
    product(product_id PK, listing_id FK, canonical_name, model)
    pageraw(page_id PK, competitor_id, url)
    """

    def open_spider(self, spider):
        self.con = db_connect(DB_PATH)
        self.cur = self.con.cursor()

        self.competitor_key = "maxiaxi"
        self.competitor_id = ensure_competitor(self.con, self.competitor_key)

        # Table presence
        self.has_pageraw = table_exists(self.con, "pageraw")
        self.has_product = table_exists(self.con, "product")
        self.has_productlisting = table_exists(self.con, "productlisting")

        # Column caches
        self.productlisting_cols = get_table_columns(self.con, "productlisting") if self.has_productlisting else set()
        self.product_cols = get_table_columns(self.con, "product") if self.has_product else set()

        spider.logger.info("DB detected: pageraw=%s product=%s productlisting=%s",
                           self.has_pageraw, self.has_product, self.has_productlisting)
        spider.logger.info("DB columns: productlisting=%s", sorted(list(self.productlisting_cols)))
        spider.logger.info("DB columns: product=%s", sorted(list(self.product_cols)))

        seed_url = RETAILERS[self.competitor_key]["category_seeds"]["microphones"]
        self.default_category_id = ensure_category_row(
            self.con,
            competitor_id=self.competitor_id,
            name="Microphones",
            url=strip_tracking(seed_url),
            parent_category_id=None,
        )

    def close_spider(self, spider):
        con = getattr(self, "con", None)
        if con is not None:
            try:
                con.commit()
            finally:
                con.close()

    def _insert_pageraw(self, url: str) -> None:
        if not self.has_pageraw or not url:
            return
        try:
            self.cur.execute(
                "INSERT INTO pageraw (competitor_id, url) VALUES (?, ?)",
                (self.competitor_id, url),
            )
        except sqlite3.IntegrityError:
            pass

    def _upsert_productlisting(self, category_id: int, d: dict) -> int:
        """
        Upsert into productlisting using ONLY your schema columns.
        """
        if not self.has_productlisting:
            raise sqlite3.OperationalError("DB missing required table: productlisting")

        product_url = d.get("product_url")
        if not product_url:
            raise ValueError("product_url is required for productlisting.")

        # Map scraped fields -> DB columns
        candidate = {
            "competitor_id": self.competitor_id,
            "category_id": category_id,
            "product_url": product_url,
            "product_name": d.get("product_name"),
            "ean": d.get("ean"),
            "sku": d.get("sku"),
            "image_url_on_pdp": d.get("image_url_on_pdp"),
        }

        data = {k: v for k, v in candidate.items() if k in self.productlisting_cols}

        row = self.cur.execute(
            "SELECT listing_id FROM productlisting WHERE competitor_id = ? AND product_url = ?",
            (self.competitor_id, product_url),
        ).fetchone()

        if row:
            listing_id = int(row["listing_id"])
            up_fields = {k: v for k, v in data.items() if k not in {"competitor_id", "product_url"} and v is not None}
            if up_fields:
                sets = ", ".join([f"{k} = COALESCE(?, {k})" for k in up_fields.keys()])
                sql = f"UPDATE productlisting SET {sets} WHERE listing_id = ?"
                self.cur.execute(sql, (*up_fields.values(), listing_id))
            return listing_id

        cols = list(data.keys())
        vals = [data[c] for c in cols]
        placeholders = ", ".join(["?"] * len(cols))
        sql = f"INSERT INTO productlisting ({', '.join(cols)}) VALUES ({placeholders})"
        self.cur.execute(sql, vals)
        return int(self.cur.execute("SELECT last_insert_rowid() AS id").fetchone()["id"])

    def _upsert_product_row(self, listing_id: int, canonical_name: str | None, model: str | None) -> None:
        """
        product schema:
          product_id PK
          listing_id FK NOT NULL
          canonical_name
          model
        In your DB, product is per listing, so we update/insert by listing_id.
        """
        if not self.has_product:
            return

        row = self.cur.execute(
            "SELECT product_id FROM product WHERE listing_id = ?",
            (listing_id,),
        ).fetchone()

        if row:
            pid = int(row["product_id"])
            self.cur.execute(
                """
                UPDATE product
                SET canonical_name = COALESCE(?, canonical_name),
                    model = COALESCE(?, model)
                WHERE product_id = ?
                """,
                (canonical_name, model, pid),
            )
            return

        self.cur.execute(
            "INSERT INTO product (listing_id, canonical_name, model) VALUES (?, ?, ?)",
            (listing_id, canonical_name, model),
        )

    def process_item(self, item, spider):
        d = dict(item)

        if isinstance(item, CategoryItem):
            ensure_category_row(
                self.con,
                competitor_id=self.competitor_id,
                name=d.get("name"),
                url=d.get("url"),
                parent_category_id=d.get("parent_category_id"),
            )
            self.con.commit()
            return item

        if isinstance(item, PageRawItem):
            if self.has_pageraw:
                self._insert_pageraw(d.get("url"))
                self.con.commit()
            return item

        if isinstance(item, ProductListingItem):
            category_id = self.default_category_id
            listing_id = self._upsert_productlisting(category_id, d)

            # Build canonical name for product table row
            brand = clean_text(d.get("brand"))
            model = clean_text(d.get("model"))
            product_name = clean_text(d.get("product_name"))

            canonical_name = None
            if brand and model:
                canonical_name = f"{brand} {model}"
            else:
                canonical_name = product_name

            # Upsert product row linked to listing_id
            self._upsert_product_row(listing_id, canonical_name=canonical_name, model=model)

            self.con.commit()
            return item

        return item


class JSONLPipeline:
    def open_spider(self, spider):
        self.f = JSONL_PATH.open("w", encoding="utf-8")

        run_record = {
            "type": "run",
            "run_id": RUN_ID,
            "started_at": datetime.now(timezone.utc).isoformat(),
            "spider": spider.name,
            "seed": MAXIAXI_MICROFOONS_URL,
            "repo_root": str(get_repo_root()),
            "db_path": str(DB_PATH),
            "db_exists": DB_PATH.exists(),
            "output_dir": str(OUT_DIR),
            "debug_dump": CONFIG["debug_dump"],
            "config": {
                "max_pages_per_category": CONFIG["max_pages_per_category"],
                "download_delay_s": CONFIG["download_delay_s"],
                "concurrent_requests": CONFIG["concurrent_requests"],
                "timeout_s": CONFIG["timeout_s"],
                "user_agent": CONFIG["user_agent"],
            },
        }
        self.f.write(json.dumps(run_record, ensure_ascii=False) + "\n")

    def close_spider(self, spider):
        try:
            self.f.close()
        except Exception:
            pass
        print("JSONL saved to:", JSONL_PATH)

    def process_item(self, item, spider):
        now = datetime.now(timezone.utc).isoformat()
        t = "item"
        if isinstance(item, ProductListingItem):
            t = "productlisting"
        elif isinstance(item, CategoryItem):
            t = "category"
        elif isinstance(item, PageRawItem):
            t = "pageraw"

        rec = {"type": t, "scraped_at": now, **dict(item)}
        self.f.write(json.dumps(rec, ensure_ascii=False) + "\n")
        return item


# =========================
# 7) SPIDER (BLOCKED DETECTION + DEBUG DUMP)
# =========================

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
            "__main__.JSONLPipeline": 400,
        },
    }

    def _dump_response(self, response, label: str):
        if not CONFIG["debug_dump"]:
            return
        try:
            fn = DEBUG_DIR / f"{self.name}_{label}_{response.status}.html"
            fn.write_bytes(response.body or b"")
            self.logger.warning("Saved debug HTML to %s", fn.resolve())
        except Exception as exc:
            self.logger.warning("Could not save debug HTML err=%s", exc)

    def start_requests(self):
        r_key = "maxiaxi"
        r = RETAILERS[r_key]

        # Store raw policy/support pages in pageraw (since no pagelink table exists)
        for _, url in (r.get("policy_urls") or {}).items():
            yield scrapy.Request(
                url=strip_tracking(url),
                callback=self.parse_raw_page,
                meta={"retailer_key": r_key},
            )

        for _, url in (r.get("expert_support_urls") or {}).items():
            yield scrapy.Request(
                url=strip_tracking(url),
                callback=self.parse_raw_page,
                meta={"retailer_key": r_key},
            )

        seed = r["category_seeds"]["microphones"]
        yield scrapy.Request(
            url=strip_tracking(seed),
            callback=self.parse_listing,
            meta={"retailer_key": r_key, "category_key": "microphones", "page_no": 1},
        )

    def parse_raw_page(self, response):
        title = clean_text(response.css("title::text").get())
        self.logger.info("RAW_PAGE status=%s url=%s title=%s", response.status, response.url, title)

        if is_blocked_response(response):
            self._dump_response(response, "raw_page_blocked")
            return

        # Write to pageraw table (via pipeline) + JSONL
        yield PageRawItem(
            competitor_key=response.meta["retailer_key"],
            url=strip_tracking(response.url),
        )

    def parse_listing(self, response):
        page_no = response.meta.get("page_no", 1)
        title = clean_text(response.css("title::text").get())
        self.logger.info("LISTING page=%s status=%s url=%s title=%s", page_no, response.status, response.url, title)

        if is_blocked_response(response):
            self._dump_response(response, f"listing_p{page_no}_blocked")
            return

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
            if "/microfoons/" in low:
                return False
            if any(x in low for x in ["/klantenservice", "/advies", "/blog", "/account", "/checkout"]):
                return False
            return bool(re.match(r"^https://www\.maxiaxi\.com/[^/]+/?$", u))

        product_links = [u for u in links if is_product_url(u)]
        self.logger.info("LISTING found_links=%s product_links=%s", len(links), len(product_links))

        if not product_links:
            self._dump_response(response, f"listing_p{page_no}_no_links")
            return

        for u in product_links:
            yield scrapy.Request(
                url=u,
                callback=self.parse_product,
                meta={"retailer_key": response.meta["retailer_key"], "category_key": response.meta["category_key"]},
            )

        if page_no < CONFIG["max_pages_per_category"]:
            next_href = response.css("li.pages-item-next a::attr(href), a.action.next::attr(href)").get()
            if next_href:
                next_url = strip_tracking(urljoin(response.url, next_href))
                yield scrapy.Request(
                    url=next_url,
                    callback=self.parse_listing,
                    meta={
                        "retailer_key": response.meta["retailer_key"],
                        "category_key": response.meta["category_key"],
                        "page_no": page_no + 1,
                    },
                )

    def parse_product(self, response):
        title = clean_text(response.css("title::text").get())
        self.logger.info("PRODUCT status=%s url=%s title=%s", response.status, response.url, title)

        if is_blocked_response(response):
            self._dump_response(response, "product_blocked")
            return

        product_url = strip_tracking(response.url)

        # product_name (DB column)
        product_name = clean_text(response.css("h1.page-title span::text, h1::text").get())
        if not product_name:
            product_name = clean_text(response.css("title::text").get())

        def value_after_label(label: str):
            v = response.xpath(f"//th[normalize-space()='{label}']/following-sibling::td[1]//text()").getall()
            if v:
                return clean_text(" ".join(v))
            v2 = response.xpath(f"//*[normalize-space()='{label}']/following::*[1]//text()").getall()
            return clean_text(" ".join(v2)) if v2 else None

        brand = value_after_label("Merk")
        sku = value_after_label("SKU")
        ean = value_after_label("EAN Code")

        image_url_on_pdp = clean_text(
            response.css(
                "img.fotorama__img::attr(src),"
                "img.product-image-photo::attr(src),"
                "meta[property='og:image']::attr(content)"
            ).get()
        )

        description_clean = clean_text(" ".join(
            response.css(
                ".product.attribute.description *::text,"
                ".product-info-main .value *::text"
            ).getall()
        ))
        if description_clean:
            description_clean = description_clean[:5000]

        # Model signal: MaxiAxi often has SKU that behaves like model
        model = sku

        yield ProductListingItem(
            competitor_key=response.meta["retailer_key"],
            category_name=response.meta["category_key"],
            product_url=product_url,
            product_name=product_name,
            ean=ean,
            sku=sku,
            image_url_on_pdp=image_url_on_pdp,
            description_clean=description_clean,
            brand=brand,
            model=model,
        )


# =========================
# 8) RUN + OPTIONAL DB EXPORT (JSON)
# =========================

def run_scrape():
    process = CrawlerProcess(settings={})
    process.crawl(CompetitorBenchmarkSpider)
    process.start()

    print("\nScrape finished.")
    print("JSONL:", JSONL_PATH)
    print("DB:", DB_PATH)
    print("Debug dir:", DEBUG_DIR)

    # Optional DB export to JSON
    if DB_PATH.exists():
        export = {}
        with db_connect(DB_PATH) as con:
            for table in [
                "competitor", "category", "productlisting", "product",
                "pricesnapshot", "scanresult", "expert_support",
                "customer_service", "review", "pageraw", "productvendor"
            ]:
                if not table_exists(con, table):
                    continue
                try:
                    df = pd.read_sql_query(f"SELECT * FROM {table}", con)
                    export[table] = df.to_dict(orient="records")
                except Exception as e:
                    export[table] = {"_error": str(e)}

        DB_EXPORT_JSON_PATH.write_text(json.dumps(export, ensure_ascii=False, indent=2), encoding="utf-8")
        print("DB export JSON:", DB_EXPORT_JSON_PATH)
    else:
        print("Skipped DB export because DB file does not exist:", DB_PATH)


if __name__ == "__main__":
    print("Setup OK. Timestamp:", datetime.now(timezone.utc).isoformat())
    print("Config loaded. Retailers:", list(RETAILERS.keys()), "Run:", RUN_ID)
    print("Seed:", MAXIAXI_MICROFOONS_URL)
    print("Output dir:", OUT_DIR)

    print("Repo root:", get_repo_root())
    print("Resolved DB:", DB_PATH)
    print("DB exists:", DB_PATH.exists())

    run_scrape()
