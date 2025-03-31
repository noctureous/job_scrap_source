# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy
from settings import logger


class JobScrapItem(scrapy.Item):
    logger.info(f'JobScrapItem')
    # define the fields for your item here like:
    # name = scrapy.Field()
    pass
