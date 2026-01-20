"""
Global Scrapy settings.

- Enables Bright Data middlewares (Proxy + Unlocker)
- Uses environment variables for credentials (no secrets in code)
"""

BOT_NAME = "odm_scraper"

SPIDER_MODULES = ["odm_scraper.spiders"]
NEWSPIDER_MODULE = "odm_scraper.spiders"

# Respect robots.txt (your course might discuss this)
ROBOTSTXT_OBEY = True

# Polite crawling defaults (spiders can override via custom_settings)
DOWNLOAD_DELAY = 2
CONCURRENT_REQUESTS = 4

AUTOTHROTTLE_ENABLED = True
AUTOTHROTTLE_START_DELAY = 1.0
AUTOTHROTTLE_MAX_DELAY = 10.0

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0 Safari/537.36"
)

# Bright Data middlewares
# NOTE: with your current middleware code:
# - If BRIGHTDATA_TOKEN + BRIGHTDATA_ZONE are set -> Unlocker handles ALL requests
# - Else if BRIGHTDATA_USERNAME + BRIGHTDATA_PASSWORD are set -> Proxy handles ALL requests
DOWNLOADER_MIDDLEWARES = {
    "odm_scraper.middlewares.BrightDataUnlockerAPIMiddleware": 540,
    "odm_scraper.middlewares.BrightDataProxyMiddleware": 610,
}

ITEM_PIPELINES = {
    "odm_scraper.pipelines.InitDbPipeline": 100,
}
