"""
Item pipelines.

Responsibilities:
- Process scraped items after extraction
- Validate and clean data
- Store data in raw files and/or a database
"""

# scraping/odm_scraper/pipelines.py
from __future__ import annotations

from odm_scraper.db import connect, init_db


class InitDbPipeline:
    def open_spider(self, spider):
        self.conn = connect()
        init_db(self.conn)

    def close_spider(self, spider):
        try:
            self.conn.commit()
        finally:
            self.conn.close()

    def process_item(self, item, spider):
        return item
