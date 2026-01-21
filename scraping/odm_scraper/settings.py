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
    # Keep this if you need Scrapy to apply USER_AGENT automatically
    "scrapy.downloadermiddlewares.useragent.UserAgentMiddleware": 400,

    "odm_scraper.middlewares.BrightDataUnlockerAPIMiddleware": 543,

    # Enable this when not testing:
    # "odm_scraper.middlewares.BrightDataProxyMiddleware": 610,

    "scrapy.downloadermiddlewares.retry.RetryMiddleware": 550,
    "scrapy.downloadermiddlewares.redirect.RedirectMiddleware": 600,

    # Remove if COOKIES_ENABLED = False
    # "scrapy.downloadermiddlewares.cookies.CookiesMiddleware": 700,

    # Keep if your proxy middleware sets request.meta["proxy"]
    "scrapy.downloadermiddlewares.httpproxy.HttpProxyMiddleware": 800,
}

# --------------------
# Pipelines
# --------------------
ITEM_PIPELINES = {
    "odm_scraper.pipelines.InitDbPipeline": 100,
}

FEEDS = {
    str(RAW_DATA_DIR / "%(name)s.json"): {
        "format": "jsonlines",
        "encoding": "utf-8",
    }
}
