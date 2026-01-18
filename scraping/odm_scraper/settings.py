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

ROBOTSTXT_OBEY = True

DOWNLOAD_DELAY = 2
AUTOTHROTTLE_ENABLED = True
AUTOTHROTTLE_START_DELAY = 1.0
AUTOTHROTTLE_MAX_DELAY = 10.0
CONCURRENT_REQUESTS = 4

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0 Safari/537.36"
)

DOWNLOADER_MIDDLEWARES = {
    "odm_scraper.middlewares.BrightDataProxyMiddleware": 610,
}

ITEM_PIPELINES = {
    "odm_scraper.pipelines.InitDbPipeline": 100,
}

