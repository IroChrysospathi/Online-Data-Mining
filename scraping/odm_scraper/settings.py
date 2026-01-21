"""
Global Scrapy settings.

- Enables Bright Data middlewares (Proxy + Unlocker)
- Ensures blocked responses (403/429) reach the spider
- Stable configuration for Bol.com
"""

from pathlib import Path


BOT_NAME = "odm_scraper"

PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw"
RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)

SPIDER_MODULES = ["odm_scraper.spiders"]
NEWSPIDER_MODULE = "odm_scraper.spiders"

# --------------------
# Crawling behaviour
# --------------------

ROBOTSTXT_OBEY = True

DOWNLOAD_DELAY = 2
CONCURRENT_REQUESTS = 4

AUTOTHROTTLE_ENABLED = True
AUTOTHROTTLE_START_DELAY = 1.0
AUTOTHROTTLE_MAX_DELAY = 10.0

DOWNLOAD_TIMEOUT = 60

# --------------------
# Identity
# --------------------

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0 Safari/537.36"
)

DEFAULT_REQUEST_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "nl-NL,nl;q=0.9,en;q=0.8",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}

COOKIES_ENABLED = False

# --------------------
# IMPORTANT: allow blocked responses
# --------------------

HTTPERROR_ALLOWED_CODES = [403, 429, 500, 502, 503]

RETRY_ENABLED = True
RETRY_TIMES = 6
RETRY_HTTP_CODES = [403, 429, 500, 502, 503, 504]

# --------------------
# Bright Data middlewares
# --------------------

DOWNLOADER_MIDDLEWARES = {
    "odm_scraper.middlewares.BrightDataUnlockerAPIMiddleware": 540,
    "odm_scraper.middlewares.BrightDataProxyMiddleware": 610,
}

# --------------------
# Pipelines
# --------------------

ITEM_PIPELINES = {
    "odm_scraper.pipelines.InitDbPipeline": 100,
}

<<<<<<< HEAD
FEEDS = {
    str(RAW_DATA_DIR / "%(name)s.json"): {
        "format": "jsonlines",
        "encoding": "utf-8",
    }
=======

DOWNLOADER_MIDDLEWARES = {
    "scrapy.downloadermiddlewares.useragent.UserAgentMiddleware": 400,

    "odm_scraper.middlewares.BrightDataUnlockerAPIMiddleware": 543,

    # comment out / remove this for the test:
    # "odm_scraper.middlewares.BrightDataProxyMiddleware": 750,

    "scrapy.downloadermiddlewares.retry.RetryMiddleware": 550,
    "scrapy.downloadermiddlewares.redirect.RedirectMiddleware": 600,
    "scrapy.downloadermiddlewares.cookies.CookiesMiddleware": 700,
    "scrapy.downloadermiddlewares.httpproxy.HttpProxyMiddleware": 800,
>>>>>>> 9bf4f49123bca26104a65622fda562bd3f613dae
}
