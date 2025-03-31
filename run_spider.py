import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

# Import your spider
from job_scrap.spiders.job_spider import JobSpider  # Adjust the import path as necessary

# Create a CrawlerProcess to run the spider
process = CrawlerProcess(get_project_settings())

# Start the spider
process.crawl(JobSpider)

# Execute the script
process.start()