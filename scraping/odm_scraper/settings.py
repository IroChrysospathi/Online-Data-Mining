"""
Global Scrapy settings.

Responsibilities:
- Configure crawler behavior (delays, concurrency, robots.txt)
- Enable or disable pipelines and middlewares
- Control overall scraping configuration
"""

BOT_NAME = "odm_scraper"

SPIDER_MODULES = ["odm_scraper.spiders"]
NEWSPIDER_MODULE = "odm_scraper.spiders"

# Respect robots.txt (can be discussed in report)
ROBOTSTXT_OBEY = True

# Polite crawling
DOWNLOAD_DELAY = 2
CONCURRENT_REQUESTS = 4

# Adaptive throttling
AUTOTHROTTLE_ENABLED = True
AUTOTHROTTLE_START_DELAY = 1.0
AUTOTHROTTLE_MAX_DELAY = 10.0

# Browser-like user agent
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0 Safari/537.36"
)

# Downloader middlewares
# IMPORTANT:
# - Unlocker runs first (540). If enabled via env vars, it replaces Scrapy downloading.
# - Proxy runs later (610) and is used only if Unlocker is NOT active.
DOWNLOADER_MIDDLEWARES = {
    "odm_scraper.middlewares.BrightDataUnlockerAPIMiddleware": 540,
    "odm_scraper.middlewares.BrightDataProxyMiddleware": 610,
}

# Data persistence
ITEM_PIPELINES = {
    "odm_scraper.pipelines.InitDbPipeline": 100,
}
