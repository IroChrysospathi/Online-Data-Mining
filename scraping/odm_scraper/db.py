# scraping/odm_scraper/db.py
from __future__ import annotations

import os
import sqlite3
from typing import Optional

# Hardcoded competitors (based on your screenshot)
COMPETITORS = [
    (1, "Bax-shop", "NL", "https://www.bax-shop.nl"),
    (2, "bol.com", "NL", "https://www.bol.com"),
    (3, "Thomann", "DE", "https://www.thomann.nl"),
    (4, "MaxiAxi", "NL", "https://www.maxiaxi.com"),
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
    # NOTE: SQLite doesn't support SERIAL; use INTEGER PRIMARY KEY AUTOINCREMENT.
    # NOTE: SQLite uses CURRENT_TIMESTAMP instead of NOW().

    conn.executescript(
        """
        -- 1. COMPETITOR
        CREATE TABLE IF NOT EXISTS competitor (
          competitor_id INTEGER PRIMARY KEY AUTOINCREMENT,
          name VARCHAR(150) NOT NULL,
          country VARCHAR(100),
          base_url VARCHAR(255)
        );

        -- 2. SCANRESULT
        CREATE TABLE IF NOT EXISTS scanresult (
          scan_id INTEGER PRIMARY KEY AUTOINCREMENT,
          competitor_id INT NOT NULL REFERENCES competitor(competitor_id),
          timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
          get_external_feeds VARCHAR(255),
          crawler_version VARCHAR(50),
          notes TEXT
        );

        -- 3. PRICESNAPSHOT
        CREATE TABLE IF NOT EXISTS pricesnapshot (
          price_snapshot_id INTEGER PRIMARY KEY AUTOINCREMENT,
          listing_id INT NOT NULL,
          image_url VARCHAR(255),
          snapshot_url VARCHAR(255),
          currency VARCHAR(10),
          current_price DECIMAL(10,2),
          base_price DECIMAL(10,2),
          discount_amount DECIMAL(10,2),
          discount_percent DECIMAL(5,2),
          price_text VARCHAR(100),
          in_stock BOOLEAN,
          stock_notice VARCHAR(255)
        );

        -- 4. PRODUCTLISTING
        CREATE TABLE IF NOT EXISTS productlisting (
          listing_id INTEGER PRIMARY KEY AUTOINCREMENT,
          competitor_id INT NOT NULL REFERENCES competitor(competitor_id),
          category_id INT,
          product_url VARCHAR(500),
          product_name VARCHAR(255),
          ean VARCHAR(50),
          sku VARCHAR(100),
          image_url_on_pdp VARCHAR(255),
          FOREIGN KEY(category_id) REFERENCES category(category_id)
        );

        -- 5. PRODUCT
        CREATE TABLE IF NOT EXISTS product (
          product_id INTEGER PRIMARY KEY AUTOINCREMENT,
          listing_id INT NOT NULL REFERENCES productlisting(listing_id),
          canonical_name VARCHAR(255),
          model VARCHAR(100)
        );

        -- 6. CATEGORY
        CREATE TABLE IF NOT EXISTS category (
          category_id INTEGER PRIMARY KEY AUTOINCREMENT,
          competitor_id INT NOT NULL REFERENCES competitor(competitor_id),
          name VARCHAR(200),
          url VARCHAR(500),
          parent_category_id INT,
          FOREIGN KEY(parent_category_id) REFERENCES category(category_id)
        );

        -- 7. PRODUCTVENDOR
        CREATE TABLE IF NOT EXISTS productvendor (
          vendor_id INTEGER PRIMARY KEY AUTOINCREMENT,
          listing_id INT NOT NULL REFERENCES productlisting(listing_id),
          listing_url VARCHAR(500),
          vendor_name VARCHAR(200),
          match_score DECIMAL(5,2),
          matched_at TIMESTAMP
        );

        -- 8. PAGERAW
        CREATE TABLE IF NOT EXISTS pageraw (
          page_id INTEGER PRIMARY KEY AUTOINCREMENT,
          competitor_id INT NOT NULL REFERENCES competitor(competitor_id),
          url VARCHAR(500)
        );

        -- 9. REVIEW
        CREATE TABLE IF NOT EXISTS review (
          review_id INTEGER PRIMARY KEY AUTOINCREMENT,
          listing_id INT NOT NULL REFERENCES productlisting(listing_id),
          rating_value DECIMAL(3,1),
          rating_count INTEGER,
          review_count INTEGER,
          review_text VARCHAR(5000),
          verified BOOLEAN,
          verified_purchase BOOLEAN,
          review_url VARCHAR(500)
        );

        -- 10. CUSTOMER_SERVICE
        CREATE TABLE IF NOT EXISTS customer_service (
          customer_service_id INTEGER PRIMARY KEY AUTOINCREMENT,
          competitor_id INT NOT NULL REFERENCES competitor(competitor_id),
          listing_id INT REFERENCES productlisting(listing_id),
          scraped_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
          source_url VARCHAR(300) NOT NULL,
          shipping_included BOOLEAN,
          free_shipping_threshold_amt DECIMAL(10,2),
          pickup_point_available BOOLEAN,
          delivery_evening_available BOOLEAN,
          delivery_sunday_available BOOLEAN,
          cooling_off_days INTEGER,
          free_returns BOOLEAN,
          warranty_provider VARCHAR(150),
          warranty_duration_months INTEGER,
          customer_service_txt TEXT
        );

        -- 11. EXPERT_SUPPORT
        CREATE TABLE IF NOT EXISTS expert_support (
          expert_support_id INTEGER PRIMARY KEY AUTOINCREMENT,
          competitor_id INT NOT NULL REFERENCES competitor(competitor_id),
          scraped_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
          source_url VARCHAR(300),
          expert_chat_available BOOLEAN,
          phone_support_available BOOLEAN,
          email_support_available BOOLEAN,
          in_store_support BOOLEAN,
          expert_support_txt TEXT
        );
        """
    )


def seed_competitors(conn: sqlite3.Connection) -> None:
    # Ensure deterministic IDs (1..4) to match your screenshot.
    conn.execute(
        """
        INSERT OR IGNORE INTO competitor (competitor_id, name, country, base_url)
        VALUES (?, ?, ?, ?)
        """,
        COMPETITORS[0],
    )
    conn.executemany(
        """
        INSERT OR IGNORE INTO competitor (competitor_id, name, country, base_url)
        VALUES (?, ?, ?, ?)
        """,
        COMPETITORS[1:],
    )

