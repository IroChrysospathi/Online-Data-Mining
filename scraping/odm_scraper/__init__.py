from pathlib import Path
import scrapy

class MaxiaxiProductsSpider(scrapy.Spider):
    name = "maxiaxi_products"
    allowed_domains = ["maxiaxi.com"]
    start_urls = ["https://www.maxiaxi.com/microfoons/"]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        desktop_dir = Path.home() / "Desktop"
        if not desktop_dir.exists():
            self.logger.warning(
                "Desktop directory not found at %s – continuing without Desktop dependency",
                desktop_dir,
            )
