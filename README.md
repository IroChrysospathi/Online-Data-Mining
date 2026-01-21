# Online Data Mining

## Business Question
How does Bax Shop compare to Thomann, Bol and Maxiaxi in terms of microphone availability, pricing, customer service, and expert support?

## Research Question
What are the differences in microphone assortment, prices, customer service, and support between Bax Shop and its main competitors in the Dutch market?

## Problem
Customers can easily compare microphones across multiple webshops, but Bax Shop lacks a structured, data-driven comparison of competitor offerings. Differences in product availability, pricing, discounts, and service policies are spread across websites and not directly comparable.

## Solution
We built a Python-based scraping system that collects and standardizes microphone data from Bax Shop and its competitors. The data is stored in a structured database, enabling direct comparison of products, prices, services, and support features.

## Approach
- Identified microphones as the focus product category using Bax Shop as the reference
- Designed an ERD to standardize products, prices, services, and support data
- Scraped main and competitor websites
- Stored and exported the data for analysis and visualization

## Team Contributions
Each team member is responsible for specific components of the project: 

- Luuk Hoogeveen: Bax products spider

- Fedde Koster: Maxiaxi products spider

- Vanna Pušić: Thomann products spider 

- Iro Chrysospathi: Bol products spider 

## Execution 
Instructions for spider execution: 

### Bax shop: 

Path: /Online-Data-Mining/scraping/odm_scraper/spiders

export BRIGHTDATA_TOKEN="<API key>"

export BRIGHTDATA_ZONE="Name"

export USE_SELENIUM=1

scrapy crawl bax_products -O ../../../data/raw/bax/bax_products.json

### Bol: 

Path: /Online-Data-Mining/scraping/odm_scraper/spiders

export BRIGHTDATA_TOKEN="<API key>"

export BRIGHTDATA_ZONE="Name"

export USE_SELENIUM=1

scrapy crawl bol_products -O ../../../data/raw/bol/bol_products.jsonl

scrapy crawl bol_support -O ../../../data/raw/bol/bol_support.jsonl

### Thomann: 

Path: /Online-Data-Mining/scraping/odm_scraper/spiders

export BRIGHTDATA_TOKEN="<API key>"

export BRIGHTDATA_ZONE="Name"

export USE_SELENIUM=1

scrapy crawl thomann_products -O ../../../data/raw/thomann/thomann_products.json

### Maxiaxi: 

Path: /Online-Data-Mining/scraping/odm_scraper/spiders

export BRIGHTDATA_TOKEN="<API key>"

export BRIGHTDATA_ZONE="Name"

export USE_SELENIUM=1

scrapy crawl maxiaxi_products -O ../../../data/raw/maxiaxi/maxiaxi_products.json


# Database Tables

- SCRAPERUN:Tracks each scraping execution.
- COMPETITOR: Stores webshop information.
- PAGELINK: Stores relevant URLs per competitor.
- PRODUCT: Defines unique microphone products.
- PRODUCTLISTING: Represents a product on a specific webshop.
- PRICESNAPSHOT: Stores pricing data.
- CUSTOMER_SERVICE: Stores customer service policies.
- EXPERT_SUPPORT: Stores expert support features.
- CATEGORY: Stores product categories.
- PRODUCTMATCH: Links identical products across webshops.
- REVIEW : Stores product reviews.

## Use of AI Tools
AI tools were used only as supportive aids during the development of this project. They were applied for conceptual clarification, structuring ideas, and improving documentation clarity (e.g. phrasing research questions and explanations).
All scraping logic, data modeling, database design, and implementation code were written, tested, and adapted by the project team. No scraper code was directly copied from AI-generated outputs. Final design decisions and implementations were made independently by the team.

