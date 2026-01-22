# =========================
# MaxiAxi Microfoons Scraper
# =========================

from __future__ import annotations

import json
import os
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlparse, parse_qs, urlencode, urlunparse

import scrapy
from scrapy.http import HtmlResponse

# Bright Data credentials 

# Use Unlocker API 
os.environ["BRIGHTDATA_TOKEN"] = "454a358a-9a98-4861-abe9-6d5f996fd79c"
os.environ["BRIGHTDATA_ZONE"] = "scraping_browser1"

# IMPORTANT: disable proxy mode so Scrapy does NOT tunnel via :9222
os.environ.pop("BRIGHTDATA_PROXY", None)

os.environ["BRIGHTDATA_USERNAME"] = "brd-customer-hl_53943da9-zone-scraping_browser1"
os.environ["BRIGHTDATA_PASSWORD"] = "p5zj9yfl8jes"
os.environ["BRIGHTDATA_HOST"] = "brd.superproxy.io"
os.environ["BRIGHTDATA_PORT"] = "9222"

# 1) CONFIG

RUN_ID = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

CONFIG = {
    "run_id": RUN_ID,
    "currency": "EUR",
    "max_pages_per_category": int(os.getenv("MAX_PAGES", "20")),
    "download_delay_s": float(os.getenv("DOWNLOAD_DELAY", "1.25")),
    "concurrent_requests": int(os.getenv("CONCURRENT_REQUESTS", "4")),
    "timeout_s": int(os.getenv("DOWNLOAD_TIMEOUT", "75")),
    "user_agent": os.getenv("USER_AGENT", "AUAS-ODM-Scraper/1.0 (educational use)"),
    "debug_dump": str(os.getenv("DEBUG_DUMP", "1")).strip().lower() not in {"0", "false", "no"},
    "use_selenium": str(os.getenv("USE_SELENIUM", "1")).strip().lower() in {"1", "true", "yes", "y", "on"},
    "selenium_wait_s": int(os.getenv("SELENIUM_WAIT", "6")),
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
# 2) PATHS 

def get_repo_root() -> Path:
    gh = os.getenv("GITHUB_WORKSPACE")
    if gh:
        return Path(gh).resolve()

    # Scrapy project layout:
    if "__file__" in globals():
        p = Path(__file__).resolve()
        # climb
        for _ in range(6):
            if (p / "data").exists() or (p / "db").exists():
                return p
            p = p.parent
        return Path(__file__).resolve().parents[3]  
    return Path.cwd().resolve()

def get_db_path() -> Path:
    env = os.getenv("DB_PATH")
    if env:
        return Path(env).expanduser().resolve()
    return (get_repo_root() / "db" / "odm.sqlite").resolve()

DB_PATH = get_db_path()

# Desktop target
DESKTOP_OUT_DIR = Path("~/Desktop/Assignment/Onlune-Data-Mining/data/raw/maxiaxi").expanduser()
# Repo target
REPO_OUT_DIR = (get_repo_root() / "data" / "raw" / "maxiaxi").resolve()
# Optional override for primary output dir
PRIMARY_OUT_DIR = Path(os.getenv("OUTPUT_DIR", str(REPO_OUT_DIR))).expanduser().resolve()

EXPORT_DIRS: list[Path] = []
for d in [PRIMARY_OUT_DIR, REPO_OUT_DIR, DESKTOP_OUT_DIR]:
    d = d.expanduser().resolve()
    if d not in EXPORT_DIRS:
        EXPORT_DIRS.append(d)

for d in EXPORT_DIRS:
    d.mkdir(parents=True, exist_ok=True)

OUT_DIR = PRIMARY_OUT_DIR
DEBUG_DIR = OUT_DIR / "debug"
DEBUG_DIR.mkdir(parents=True, exist_ok=True)

JSONL_NAME = f"maxiaxi_items_{RUN_ID}.jsonl"
JSONL_PATH = OUT_DIR / JSONL_NAME

# 3) HELPERS

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def clean_text(x: Any) -> str | None:
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
    """
    Treat responses as "blocked/useless" when they are:
    - explicit block statuses (403/429/503)
    - consent/captcha/etc detected
    - or when BrightData returns a tiny HTML shell
    """
    title = clean_text(response.css("title::text").get())

    # explicit block statuses
    if response.status in (403, 429, 503):
        return True

    # title-based block signals
    if looks_blocked_title(title):
        return True

    body_len = len(response.body or b"")

    if body_len < 5_000:
        return True

    # Missing title
    if title is None and body_len < 20_000:
        return True

    # consent/captcha keywords in smaller HTML pages
    if response.text and len(response.text) < 80_000:
        low = response.text.lower()
        if any(x in low for x in ["cookie", "toestemming", "consent", "captcha", "access denied"]):
            return True

    return False
def copy_file_to_dirs(src: Path, dirs: list[Path]) -> list[Path]:
    written: list[Path] = []
    if not src.exists():
        return written
    import shutil
    for d in dirs:
        try:
            d.mkdir(parents=True, exist_ok=True)
            dst = d / src.name
            if dst.resolve() == src.resolve():
                written.append(dst)
                continue
            shutil.copy2(src, dst)
            written.append(dst)
        except Exception:
            pass
    return written

def brightdata_mode() -> str:
    # Unlocker API 
    if os.getenv("BRIGHTDATA_TOKEN") and os.getenv("BRIGHTDATA_ZONE"):
        return "unlocker_api"
    # Proxy mode if proxy or username/password exist
    if os.getenv("BRIGHTDATA_PROXY") or (os.getenv("BRIGHTDATA_USERNAME") and os.getenv("BRIGHTDATA_PASSWORD")):
        return "proxy"
    return "disabled"

def resolve_brightdata_proxy_url() -> str | None:
    p = os.getenv("BRIGHTDATA_PROXY")
    if p:
        return p.strip()

    user = os.getenv("BRIGHTDATA_USERNAME")
    pwd = os.getenv("BRIGHTDATA_PASSWORD")
    host = os.getenv("BRIGHTDATA_HOST")
    port = os.getenv("BRIGHTDATA_PORT")
    if user and pwd and host and port:
        return f"http://{user}:{pwd}@{host}:{port}"
    return None

# 4) Selenium renderer 

def render_with_selenium(url: str, wait_seconds: int = 6) -> str:
    """
    Minimal Selenium renderer:
    - headless Chrome
    - attempts cookie/consent click
    - returns page_source
    """
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.chrome.service import Service
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    import time

    chromedriver_path = os.getenv("CHROMEDRIVER")
    service = Service(chromedriver_path) if chromedriver_path else Service()

    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--window-size=1365,900")
    options.add_argument(f"--user-agent={CONFIG['user_agent']}")

    driver = webdriver.Chrome(service=service, options=options)
    try:
        driver.get(url)
        time.sleep(1.2)

        # Best-effort cookie acceptance
        for xpath in [
            "//button[contains(translate(., 'AKKOORDACCEPT', 'akkoordaccept'), 'akkoord')]",
            "//button[contains(translate(., 'AKKOORDACCEPT', 'akkoordaccept'), 'accept')]",
            "//button[contains(translate(., 'ALLES', 'alles'), 'alles')]",
            "//button[contains(translate(., 'TOESTEMMING', 'toestemming'), 'toestemming')]",
        ]:
            try:
                btn = WebDriverWait(driver, 1.5).until(EC.element_to_be_clickable((By.XPATH, xpath)))
                btn.click()
                time.sleep(0.6)
                break
            except Exception:
                pass

        # Wait for likely content markers (listing or PDP)
        wait = WebDriverWait(driver, max(2, int(wait_seconds)))
        try:
            wait.until(
                lambda d: (
                    len(d.find_elements(By.CSS_SELECTOR, "ol.products li.product-item")) > 0
                    or len(d.find_elements(By.CSS_SELECTOR, "h1.page-title")) > 0
                    or len(d.find_elements(By.CSS_SELECTOR, "script[type='application/ld+json']")) > 0
                )
            )
        except Exception:
            pass

        time.sleep(0.8)
        return driver.page_source
    finally:
        driver.quit()

# 5) Bright Data Unlocker 

class GlobalProxyMiddleware:
    """
    Ensures proxy is applied to every request unless already set.
    This prevents some requests going direct (a common cause of blocks/timeouts).
    """
    def process_request(self, request, spider):
        proxy_url = getattr(spider, "proxy_url", None)
        if proxy_url and not request.meta.get("proxy"):
            request.meta["proxy"] = proxy_url
        return None

class BrightDataUnlockerMiddleware:
    """
    If BRIGHTDATA_TOKEN + BRIGHTDATA_ZONE are set, fetch pages via Bright Data Unlocker API in Python,
    and return HtmlResponse to Scrapy.
    """
    def __init__(self, token: str, zone: str):
        self.token = token
        self.zone = zone

    @classmethod
    def from_crawler(cls, crawler):
        token = os.getenv("BRIGHTDATA_TOKEN", "").strip()
        zone = os.getenv("BRIGHTDATA_ZONE", "").strip()
        return cls(token=token, zone=zone)

    def process_request(self, request: scrapy.Request, spider):
        if not self.token or not self.zone:
            return None
        if request.meta.get("skip_brightdata"):
            return None

        url = request.url
        if not (url.startswith("http://") or url.startswith("https://")):
            return None

        try:
            import requests
        except Exception:
            spider.logger.warning("requests not installed; Bright Data Unlocker API disabled.")
            return None

        endpoint = os.getenv("BRIGHTDATA_UNLOCKER_ENDPOINT", "https://api.brightdata.com/request").strip()
        timeout = int(os.getenv("BRIGHTDATA_TIMEOUT", "60"))

        headers = {"Authorization": f"Bearer {self.token}", "Content-Type": "application/json"}
        payload = {"zone": self.zone, "url": url, "format": "raw"}

        try:
            r = requests.post(endpoint, headers=headers, json=payload, timeout=timeout)
            body = r.content or b""
            return HtmlResponse(url=url, status=r.status_code, body=body, encoding="utf-8", request=request)
        except Exception as exc:
            spider.logger.warning("Bright Data Unlocker API failed url=%s err=%s", url, exc)
            return None

# 6) Items

class PageRawItem(scrapy.Item):
    competitor_key = scrapy.Field()
    url = scrapy.Field()

class ProductListingItem(scrapy.Item):
    competitor_key = scrapy.Field()
    category_name = scrapy.Field()

    product_url = scrapy.Field()
    product_name = scrapy.Field()
    ean = scrapy.Field()
    sku = scrapy.Field()
    image_url_on_pdp = scrapy.Field()

    description_clean = scrapy.Field()
    brand = scrapy.Field()
    model = scrapy.Field()

# 7) Optional SQLite pipeline 

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

def ensure_category_row(con: sqlite3.Connection, competitor_id: int, name: str, url: str | None, parent_category_id: int | None) -> int:
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

class SQLitePipeline:
    """
    Writes to your existing repo DB schema:
      productlisting(listing_id PK, competitor_id, category_id, product_url, product_name, ean, sku, image_url_on_pdp)
      product(product_id PK, listing_id FK NOT NULL, canonical_name, model)
      pageraw(page_id PK, competitor_id, url)
    """

    def open_spider(self, spider):
        self.con = db_connect(DB_PATH)
        self.cur = self.con.cursor()
        self.competitor_key = "maxiaxi"
        self.competitor_id = ensure_competitor(self.con, self.competitor_key)

        self.has_pageraw = table_exists(self.con, "pageraw")
        self.has_product = table_exists(self.con, "product")
        self.has_productlisting = table_exists(self.con, "productlisting")
        self.productlisting_cols = get_table_columns(self.con, "productlisting") if self.has_productlisting else set()
        self.product_cols = get_table_columns(self.con, "product") if self.has_product else set()

        seed_url = RETAILERS[self.competitor_key]["category_seeds"]["microphones"]
        self.default_category_id = ensure_category_row(
            self.con,
            competitor_id=self.competitor_id,
            name="Microphones",
            url=strip_tracking(seed_url),
            parent_category_id=None,
        )

        spider.logger.info(
            "DB detected: productlisting=%s product=%s pageraw=%s",
            self.has_productlisting, self.has_product, self.has_pageraw
        )

    def close_spider(self, spider):
        try:
            self.con.commit()
        finally:
            self.con.close()

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
        if not self.has_productlisting:
            raise sqlite3.OperationalError("DB missing required table: productlisting")

        product_url = d.get("product_url")
        if not product_url:
            raise ValueError("product_url is required for productlisting.")

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
                self.cur.execute(f"UPDATE productlisting SET {sets} WHERE listing_id = ?", (*up_fields.values(), listing_id))
            return listing_id

        cols = list(data.keys())
        vals = [data[c] for c in cols]
        placeholders = ", ".join(["?"] * len(cols))
        self.cur.execute(f"INSERT INTO productlisting ({', '.join(cols)}) VALUES ({placeholders})", vals)
        return int(self.cur.execute("SELECT last_insert_rowid() AS id").fetchone()["id"])

    def _upsert_product(self, listing_id: int, canonical_name: str | None, model: str | None) -> None:
        if not self.has_product:
            return

        row = self.cur.execute("SELECT product_id FROM product WHERE listing_id = ?", (listing_id,)).fetchone()
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

        if isinstance(item, PageRawItem):
            self._insert_pageraw(d.get("url"))
            self.con.commit()
            return item

        if isinstance(item, ProductListingItem):
            category_id = self.default_category_id
            listing_id = self._upsert_productlisting(category_id, d)

            brand = clean_text(d.get("brand"))
            model = clean_text(d.get("model"))
            product_name = clean_text(d.get("product_name"))
            canonical_name = f"{brand} {model}" if (brand and model) else product_name

            self._upsert_product(listing_id, canonical_name=canonical_name, model=model)
            self.con.commit()
            return item

        return item

# 8) JSONL pipeline 

class JSONLPipeline:
    def open_spider(self, spider):
        self.f = JSONL_PATH.open("w", encoding="utf-8")

        run_record = {
            "type": "run",
            "run_id": RUN_ID,
            "started_at": utc_now_iso(),
            "spider": spider.name,
            "seed": strip_tracking(MAXIAXI_MICROFOONS_URL),
            "repo_root": str(get_repo_root()),
            "db_path": str(DB_PATH),
            "db_exists": DB_PATH.exists(),
            "output_dir_primary": str(OUT_DIR),
            "output_dirs_all": [str(d) for d in EXPORT_DIRS],
            "debug_dump": CONFIG["debug_dump"],
            "brightdata_mode": spider.bd_mode,
            "use_selenium": CONFIG["use_selenium"],
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

        copies = copy_file_to_dirs(JSONL_PATH, EXPORT_DIRS)

        print("JSONL saved to (primary):", JSONL_PATH)
        if copies:
            print("JSONL also copied to:")
            for p in copies:
                print("-", p)

    def process_item(self, item, spider):
        rec = {"type": "item", "scraped_at": utc_now_iso(), **dict(item)}
        self.f.write(json.dumps(rec, ensure_ascii=False) + "\n")
        return item

# 9) SPIDER 

class GlobalProxyMiddleware:
    """
    Force Bright Data proxy on EVERY request (unless already set).
    Automatically disabled when BrightData Unlocker API is active.
    """
    def process_request(self, request, spider):

        if getattr(spider, "bd_mode", None) == "unlocker_api":
            return None

        proxy_url = getattr(spider, "proxy_url", None)
        if proxy_url and not request.meta.get("proxy"):
            request.meta["proxy"] = proxy_url

            cnt = getattr(spider, "_proxy_applied_count", 0)
            if cnt < 8:
                spider.logger.info("Proxy applied -> %s | %s", proxy_url, request.url)
            spider._proxy_applied_count = cnt + 1

        return None

class CompetitorBenchmarkSpider(scrapy.Spider):
    name = "maxiaxi_microfoons"
    allowed_domains = ["www.maxiaxi.com", "maxiaxi.com"]

    custom_settings = {
        "USER_AGENT": CONFIG["user_agent"],

        # Stable defaults
        "CONCURRENT_REQUESTS": 2,
        "DOWNLOAD_DELAY": 2.0,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": 2.0,
        "AUTOTHROTTLE_MAX_DELAY": 20.0,

        "DOWNLOAD_TIMEOUT": 120,
        "RETRY_TIMES": 2,
        "RETRY_HTTP_CODES": [403, 408, 429, 500, 502, 503, 504],

        # IMPORTANT for stability
        "ROBOTSTXT_OBEY": False,

        "COOKIES_ENABLED": False,
        "LOG_LEVEL": "INFO",

        "DOWNLOADER_MIDDLEWARES": {

            f"{__name__}.BrightDataUnlockerMiddleware": 20,

            f"{__name__}.GlobalProxyMiddleware": 40,
            "scrapy.downloadermiddlewares.httpproxy.HttpProxyMiddleware": 110,
        },

        "ITEM_PIPELINES": {
            f"{__name__}.JSONLPipeline": 400,
        },
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.bd_mode = brightdata_mode()
        self.proxy_url = self._resolve_proxy_url() if self.bd_mode == "proxy" else None

        self.use_selenium = bool(CONFIG["use_selenium"])
        self.selenium_wait_s = int(CONFIG["selenium_wait_s"])

        if self.bd_mode == "disabled":
            raise RuntimeError(
                "Bright Data is required. Configure either:\n"
                "  - BRIGHTDATA_TOKEN + BRIGHTDATA_ZONE (Unlocker API), or\n"
                "  - BRIGHTDATA_PROXY (or BRIGHTDATA_USERNAME/BRIGHTDATA_PASSWORD + BRIGHTDATA_HOST + BRIGHTDATA_PORT)\n"
            )

        if self.bd_mode == "proxy" and not self.proxy_url:
            raise RuntimeError(
                "Bright Data proxy mode detected but proxy URL not configured.\n"
                "Set BRIGHTDATA_PROXY or BRIGHTDATA_USERNAME/BRIGHTDATA_PASSWORD/BRIGHTDATA_HOST/BRIGHTDATA_PORT"
            )

        self.logger.info("BrightData mode: %s", self.bd_mode)
        self.logger.info("Proxy URL set: %s", bool(self.proxy_url))
        self.logger.info("Use selenium: %s wait: %s", self.use_selenium, self.selenium_wait_s)

    def _resolve_proxy_url(self) -> str | None:
        p = os.getenv("BRIGHTDATA_PROXY")
        if p:
            return p.strip()

        user = os.getenv("BRIGHTDATA_USERNAME")
        pwd = os.getenv("BRIGHTDATA_PASSWORD")
        host = os.getenv("BRIGHTDATA_HOST")
        port = os.getenv("BRIGHTDATA_PORT")
        if user and pwd and host and port:
            return f"http://{user}:{pwd}@{host}:{port}"
        return None

    def _dump_response(self, response, label: str):
        if not CONFIG["debug_dump"]:
            return
        try:
            fn = DEBUG_DIR / f"{self.name}_{label}_{response.status}.html"
            fn.write_bytes(response.body or b"")
            self.logger.warning("Saved debug HTML to %s", fn.resolve())
        except Exception as exc:
            self.logger.warning("Could not save debug HTML err=%s", exc)

    def maybe_render_with_selenium(self, url: str) -> str | None:
        if not self.use_selenium:
            return None
        try:
            return render_with_selenium(url, wait_seconds=self.selenium_wait_s)
        except Exception as exc:
            self.logger.warning("Selenium render failed url=%s err=%s", url, exc)
            return None

    def errback_main(self, failure):
        req = failure.request
        self.logger.warning("Request failed: %s err=%s", req.url, repr(failure.value))

        html = self.maybe_render_with_selenium(req.url)
        if not html:
            return

        self.logger.warning("Selenium fallback succeeded: %s", req.url)
        cb = req.callback or self.parse_listing
        fake = HtmlResponse(url=req.url, body=html, encoding="utf-8", request=req)
        return cb(fake)

    def errback_aux(self, failure):
        req = failure.request
        self.logger.warning("AUX page failed (ignored): %s err=%s", req.url, repr(failure.value))

    def start_requests(self):
        self.logger.info("start_requests() called - scheduling initial URLs")

        r_key = "maxiaxi"
        r = RETAILERS[r_key]

        # auxiliary pages
        for _, url in (r.get("policy_urls") or {}).items():
            yield scrapy.Request(
                url=strip_tracking(url),
                callback=self.parse_raw_page,
                meta={"retailer_key": r_key},
                dont_filter=True,
                errback=self.errback_aux,
            )

        for _, url in (r.get("expert_support_urls") or {}).items():
            yield scrapy.Request(
                url=strip_tracking(url),
                callback=self.parse_raw_page,
                meta={"retailer_key": r_key},
                dont_filter=True,
                errback=self.errback_aux,
            )

        seed = r["category_seeds"]["microphones"]
        yield scrapy.Request(
            url=strip_tracking(seed),
            callback=self.parse_listing,
            meta={"retailer_key": r_key, "category_key": "microphones", "page_no": 1},
            dont_filter=True,
            errback=self.errback_main,
        )

    def parse_raw_page(self, response):
        title = clean_text(response.css("title::text").get())
        self.logger.info("RAW_PAGE status=%s url=%s title=%s", response.status, response.url, title)

        if is_blocked_response(response):
            self._dump_response(response, "raw_page_blocked_or_useless")
            return

        yield PageRawItem(
            competitor_key=response.meta["retailer_key"],
            url=strip_tracking(response.url),
        )

    def parse_listing(self, response):
        page_no = response.meta.get("page_no", 1)
        title = clean_text(response.css("title::text").get())
        self.logger.info("LISTING page=%s status=%s url=%s title=%s", page_no, response.status, response.url, title)

        if is_blocked_response(response):
            self._dump_response(response, f"listing_p{page_no}_blocked_or_useless")
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
                errback=self.errback_main,
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
                    errback=self.errback_main,
                )

    def parse_product(self, response):
        title = clean_text(response.css("title::text").get())
        self.logger.info("PRODUCT status=%s url=%s title=%s", response.status, response.url, title)

        if is_blocked_response(response):
            self._dump_response(response, "product_blocked_or_useless")
            return

        product_url = strip_tracking(response.url)

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

# 10) RUN

from scrapy.crawler import CrawlerProcess

def run_scrape():
    print("Starting MaxiAxi spider (script mode). Run:", RUN_ID)
    print("Primary output:", JSONL_PATH)
    print("All export dirs:", [str(d) for d in EXPORT_DIRS])
    print("DB path:", DB_PATH, "exists:", DB_PATH.exists())
    print("BrightData mode:", brightdata_mode())
    print("Proxy URL set:", bool(resolve_brightdata_proxy_url()))
    print("Use selenium:", CONFIG["use_selenium"], "wait:", CONFIG["selenium_wait_s"])
    print("Timeout:", int(os.getenv("DOWNLOAD_TIMEOUT", "75")))

    process = CrawlerProcess(settings={})
    process.crawl(CompetitorBenchmarkSpider)
    process.start()

    print("Scrape finished. JSONL:", JSONL_PATH)

if __name__ == "__main__":
    run_scrape()
