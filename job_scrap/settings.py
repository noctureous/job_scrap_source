# Scrapy settings for job_scrap project
#
# For simplicity, this file contains only settings considered important or
# commonly used. You can find more settings consulting the documentation:
#
#     https://docs.scrapy.org/en/latest/topics/settings.html
#     https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
#     https://docs.scrapy.org/en/latest/topics/spider-middleware.html

# Get Date Time
from datetime import datetime
import os
dt = datetime.now()
input_dt_str = dt.strftime("%Y%m%d%H%M%S")

# print(input_dt_str)
BOT_NAME = f"job_scrap_{input_dt_str}"


SPIDER_MODULES = ["job_scrap.spiders"]
NEWSPIDER_MODULE = "job_scrap.spiders"

# Obey robots.txt rules
ROBOTSTXT_OBEY = True


CONCURRENT_REQUESTS = 1000

# Custom Config
# REACTOR_THREADPOOL_MAXSIZE = 100
LOG_LEVEL = "INFO"


# FEED_URI="job_ads_output.csv"

# Configure maximum concurrent requests performed by Scrapy (default: 16)
#CONCURRENT_REQUESTS = 32

# Configure a delay for requests for the same website (default: 0)
# See https://docs.scrapy.org/en/latest/topics/settings.html#download-delay
# See also autothrottle settings and docs
#DOWNLOAD_DELAY = 3
# The download delay setting will honor only one of:
#CONCURRENT_REQUESTS_PER_DOMAIN = 16
#CONCURRENT_REQUESTS_PER_IP = 16

# Disable cookies (enabled by default)
#COOKIES_ENABLED = False

# Disable Telnet Console (enabled by default)
#TELNETCONSOLE_ENABLED = False

# Override the default request headers:
#DEFAULT_REQUEST_HEADERS = {
#    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
#    "Accept-Language": "en",
#}

# Enable or disable spider middlewares
# See https://docs.scrapy.org/en/latest/topics/spider-middleware.html
#SPIDER_MIDDLEWARES = {
#    "job_scrap.middlewares.JobScrapSpiderMiddleware": 543,
#}

# Enable or disable downloader middlewares
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
#DOWNLOADER_MIDDLEWARES = {
#    "job_scrap.middlewares.JobScrapDownloaderMiddleware": 543,
#}

# Enable or disable extensions
# See https://docs.scrapy.org/en/latest/topics/extensions.html
#EXTENSIONS = {
#    "scrapy.extensions.telnet.TelnetConsole": None,
#}

# Configure item pipelines
# See https://docs.scrapy.org/en/latest/topics/item-pipeline.html
# ITEM_PIPELINES = {
#    "job_scrap.pipelines.JobScrapPipeline": 300,
# }

# Enable and configure the AutoThrottle extension (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/autothrottle.html
#AUTOTHROTTLE_ENABLED = True
# The initial download delay
#AUTOTHROTTLE_START_DELAY = 5
# The maximum download delay to be set in case of high latencies
#AUTOTHROTTLE_MAX_DELAY = 60
# The average number of requests Scrapy should be sending in parallel to
# each remote server
#AUTOTHROTTLE_TARGET_CONCURRENCY = 1.0
# Enable showing throttling stats for every response received:
#AUTOTHROTTLE_DEBUG = False

# Enable and configure HTTP caching (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html#httpcache-middleware-settings
#HTTPCACHE_ENABLED = True
#HTTPCACHE_EXPIRATION_SECS = 0
#HTTPCACHE_DIR = "httpcache"
#HTTPCACHE_IGNORE_HTTP_CODES = []
#HTTPCACHE_STORAGE = "scrapy.extensions.httpcache.FilesystemCacheStorage"
HTTPCACHE_ENABLED = False
HTTPCACHE_EXPIRATION_SECS = 0
HTTPCACHE_DIR = "httpcache"
HTTPCACHE_IGNORE_HTTP_CODES = []
HTTPCACHE_STORAGE = "scrapy.extensions.httpcache.FilesystemCacheStorage"

# Set settings whose default value is deprecated to a future-proof value
TWISTED_REACTOR = "twisted.internet.asyncioreactor.AsyncioSelectorReactor"
FEED_EXPORT_ENCODING = "utf-8"

import os
import traceback
from yaml import safe_load
import logging
from datetime import datetime



dt = datetime.now()
script_name = os.path.basename(__name__)
script_name_clean=os.path.splitext(os.path.basename(script_name))[0]
log_filename_dir = os.path.join(os.getcwd(),script_name_clean, f"{script_name_clean}")
if not os.path.exists(log_filename_dir) : os.makedirs(log_filename_dir, True)
log_filename = os.path.join(log_filename_dir, f"{script_name_clean}-{dt.strftime('%Y-%m-%d-%H%M%S')}.log")
print(f"{script_name_clean} : Please find log file in {log_filename}")
LOGGING_FORMAT="%(message)s"
LOGGING_FORMAT="%(asctime)s %(levelname)s %(thread)s --- [%(threadName)s] %(message)s"
LOGGING_FORMAT = "%(asctime)s %(levelname)9s %(thread)6d --- [%(threadName)s] [%(name)s] %(message)s"
# LOGGING_FORMAT = "%(asctime)s %(levelname)s %07d --- [%(threadName)s] [%(name)s] %(message)s"

logging_level=logging.INFO
logging.basicConfig(
    filename=log_filename,
    level=logging_level,
    format=LOGGING_FORMAT
)
logging.Formatter(
    fmt='%(asctime)s.%(msecs)03d',
    datefmt='%Y-%m-%d,%H:%M:%S'
)
logger = logging.getLogger(__name__)
other_logging_level=logging.INFO
logging.getLogger('job_scrap.spiders.job_spider').setLevel(other_logging_level)
logging.getLogger('scrapy.Spider').setLevel(other_logging_level)
logging.getLogger('scrapy').setLevel(other_logging_level)
logging.getLogger('pymysql').setLevel(other_logging_level)
logging.getLogger('mysql.connector').setLevel(other_logging_level)
# scrapy.core.scraper
logging.getLogger('scrapy.core.scraper').setLevel(other_logging_level)
# logging.getLogger('scrapy.core.scraper').setLevel(logging.DEBUG)
# logging.getLogger('scrapy.core.scraper').setLevel(logging.INFO)


console_handler = logging.StreamHandler()
console_handler.setLevel(logging_level)
formatter = logging.Formatter(LOGGING_FORMAT)
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)