# scraping/odm_scraper/db.py
from __future__ import annotations

import os
import sqlite3
from typing import Optional

# Hardcoded competitors 
COMPETITORS = [
    (1, "Bax-shop", "NL", "https://www.bax-shop.nl"),
    (2, "bol.com", "NL", "https://www.bol.com"),
    (3, "MaxiAxi", "NL", "https://www.maxiaxi.com"),
    (4, "Thomann", "DE", "https://www.thomann.nl")
]


def get_db_path() -> str:
    """
    Returns absolute path to db/odm.sqlite from within scraping/odm_scraper/.
    """
    here = os.path.dirname(__file__)  # .../scraping/odm_scraper
    repo_root = os.path.abspath(os.path.join(here, "..", ".."))  # .../Online-Data-Mining
    return os.path.join(repo_root, "db", "odm.sqlite")


def connect(db_path: Optional[str] = None) -> sqlite3.Connection:
    path = db_path or get_db_path()
    os.makedirs(os.path.dirname(path), exist_ok=True)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    create_tables(conn)
    seed_competitors(conn)
    conn.commit()


def create_tables(conn: sqlite3.Connection) -> None:
    """
    Creates tables to match the ERD image:
      - competitor
      - scraperun
      - category
      - productlisting
      - pricesnapshot
      - product
      - productmatch
      - review
      - pagelink
      - customer_service
      - expert_support
    """
    conn.executescript(
        """
        -- COMPETITOR
        CREATE TABLE IF NOT EXISTS competitor (
          competitor_id INTEGER PRIMARY KEY,
          name          VARCHAR(100) NOT NULL,
          country       VARCHAR(50),
          base_url      VARCHAR(200)
        );

        -- SCRAPERUN
        CREATE TABLE IF NOT EXISTS scraperun (
          scrape_run_id     INTEGER PRIMARY KEY AUTOINCREMENT,
          started_at        TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
          gpt_context_hash  VARCHAR(50),
          crawler_version   VARCHAR(20),
          notes             TEXT
        );

        -- CATEGORY
        CREATE TABLE IF NOT EXISTS category (
          category_id         INTEGER PRIMARY KEY AUTOINCREMENT,
          competitor_id       INTEGER NOT NULL REFERENCES competitor(competitor_id),
          name                VARCHAR(200),
          url                 VARCHAR(200),
          parent_category_id  INTEGER,
          FOREIGN KEY(parent_category_id) REFERENCES category(category_id)
        );

        -- PRODUCTLISTING
        CREATE TABLE IF NOT EXISTS productlisting (
          listing_id            INTEGER PRIMARY KEY AUTOINCREMENT,
          competitor_id         INTEGER NOT NULL REFERENCES competitor(competitor_id),
          category_id           INTEGER REFERENCES category(category_id),
          product_url           VARCHAR(500) NOT NULL,
          title_on_page         VARCHAR(300),
          image_url_src         VARCHAR(500),
          img_url_cdn           VARCHAR(500),
          gtin_ean_upc_on_page  VARCHAR(50),
          description_clean     TEXT
        );

        -- PRICESNAPSHOT
        CREATE TABLE IF NOT EXISTS pricesnapshot (
          price_snapshot_id  INTEGER PRIMARY KEY AUTOINCREMENT,
          listing_id         INTEGER NOT NULL REFERENCES productlisting(listing_id),
          scrape_run_id      INTEGER NOT NULL REFERENCES scraperun(scrape_run_id),
          scraped_at         TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
          currency           VARCHAR(3),
          current_price      DECIMAL(10,2),
          base_price         DECIMAL(10,2),
          discount_amount    DECIMAL(10,2),
          discount_percent   DECIMAL(5,2),
          price_text         VARCHAR(100),
          in_stock           BOOLEAN,
          stock_status_text  VARCHAR(50)
        );

        -- PRODUCT
        CREATE TABLE IF NOT EXISTS product (
          product_id      INTEGER PRIMARY KEY AUTOINCREMENT,
          canonical_name  VARCHAR(200) NOT NULL,
          brand           VARCHAR(100),
          model           VARCHAR(100)
        );

        -- PRODUCTMATCH
        CREATE TABLE IF NOT EXISTS productmatch (
          match_id      INTEGER PRIMARY KEY AUTOINCREMENT,
          product_id    INTEGER NOT NULL REFERENCES product(product_id),
          listing_id    INTEGER NOT NULL REFERENCES productlisting(listing_id),
          match_method  TEXT,              -- ERD: Enum
          match_score   DECIMAL(3,2),
          matched_at    TIMESTAMP
        );

        -- REVIEW
        CREATE TABLE IF NOT EXISTS review (
          review_id          INTEGER PRIMARY KEY AUTOINCREMENT,
          listing_id         INTEGER NOT NULL REFERENCES productlisting(listing_id),
          created_at         TIMESTAMP,
          rating_value       DECIMAL(3,1),
          rating_scale       INTEGER,
          review_count       INTEGER,
          review_text        TEXT,
          reviewer_name      VARCHAR(100),
          verified           BOOLEAN,
          verified_purchase  BOOLEAN,
          review_url         VARCHAR(500)
        );

        -- PAGELINK
        CREATE TABLE IF NOT EXISTS pagelink (
          page_id        INTEGER PRIMARY KEY AUTOINCREMENT,
          competitor_id  INTEGER NOT NULL REFERENCES competitor(competitor_id),
          page_type      TEXT,              -- ERD: Enum
          url            VARCHAR(500)
        );

        -- CUSTOMER_SERVICE
        CREATE TABLE IF NOT EXISTS customer_service (
          customer_service_id         INTEGER PRIMARY KEY AUTOINCREMENT,
          competitor_id               INTEGER NOT NULL REFERENCES competitor(competitor_id),
          listing_id                  INTEGER REFERENCES productlisting(listing_id),
          scraped_at                  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
          shipping_included           BOOLEAN,
          free_shipping_threshold_amt DECIMAL(10,2),
          pickup_point_available      BOOLEAN,
          delivery_shipping_available BOOLEAN,
          delivery_courier_available  BOOLEAN,
          cooling_off_days            INTEGER,
          free_returns                BOOLEAN,
          warranty_provider           VARCHAR(100),
          warranty_duration_months    INTEGER,
          customer_service_url        TEXT
        );

        -- EXPERT_SUPPORT
        CREATE TABLE IF NOT EXISTS expert_support (
          expert_support_id       INTEGER PRIMARY KEY AUTOINCREMENT,
          competitor_id           INTEGER NOT NULL REFERENCES competitor(competitor_id),
          scraped_at              TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
          source_url              VARCHAR(300),
          expert_chat_available   BOOLEAN,
          phone_support_available BOOLEAN,
          email_support_available BOOLEAN,
          in_store_support        BOOLEAN,
          expert_support_text     TEXT
        );

        -- Helpful indexes
        CREATE INDEX IF NOT EXISTS idx_productlisting_competitor ON productlisting(competitor_id);
        CREATE INDEX IF NOT EXISTS idx_productlisting_category   ON productlisting(category_id);
        CREATE INDEX IF NOT EXISTS idx_pricesnapshot_listing     ON pricesnapshot(listing_id);
        CREATE INDEX IF NOT EXISTS idx_pricesnapshot_run         ON pricesnapshot(scrape_run_id);
        CREATE INDEX IF NOT EXISTS idx_review_listing            ON review(listing_id);
        CREATE INDEX IF NOT EXISTS idx_productmatch_listing      ON productmatch(listing_id);
        CREATE INDEX IF NOT EXISTS idx_productmatch_product      ON productmatch(product_id);
        """
    )

def seed_competitors(conn: sqlite3.Connection) -> None:
    conn.executemany(
        """
        INSERT OR IGNORE INTO competitor (competitor_id, name, country, base_url)
        VALUES (?, ?, ?, ?)
        """,
        COMPETITORS,
    )

def create_competitor_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS competitor (
            competitor_id INTEGER PRIMARY KEY,
            name          TEXT NOT NULL,
            country       TEXT NOT NULL,
            base_url      TEXT NOT NULL
        );
        """
    )

if __name__ == "__main__":
    conn = connect()
    init_db(conn)

    # Print tables 
    rows = conn.execute("""
        SELECT name
        FROM sqlite_master
        WHERE type='table' AND name NOT LIKE 'sqlite_%'
        ORDER BY name;
    """).fetchall()

    print("Database initialized at:", get_db_path())
    print("Tables:", [r[0] for r in rows])

    conn.close()