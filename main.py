#!/usr/local/bin/python3

import logging
import timeit
from api_crawlers import api_template
from bs4_crawlers import bs4_template
from rss_crawlers import rss_template
from selenium_crawlers import selenium_template
from datetime import date, timedelta
from utils.handy import LoggingMasterCrawler
import os
from dotenv import load_dotenv

#SET UP LOGGING
LoggingMasterCrawler()

def main(pipeline):
    # start master timer
    master_start_time = timeit.default_timer()

    #Start calling each crawler
    logging.info('ALL CRAWLERS DEPLOYED!')

    bs4_template(pipeline) #1st argument MAIN OR TEST

    api_template(pipeline) #1st: PROD_API or TEST_API
    
    rss_template(pipeline) #1st argument MAIN OR TEST

    selenium_template(pipeline) # 'MAIN', TEST or FREELANCE

    #print the time
    elapsed_time = timeit.default_timer() - master_start_time
    logging.info(f'ALL CRAWLERS FINISHED. ALL IN {elapsed_time:.2f} SECONDS!')

if __name__ == "__main__":
    main("TEST")

