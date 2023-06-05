#!/usr/local/bin/python3

import logging
import timeit
from api_crawlers import api_template
from rss_crawlers import rss_template
from selenium_crawlers import selenium_template
from datetime import date, timedelta
from utils.handy import LoggingMasterCrawler
import os
from dotenv import load_dotenv

""" LOAD ALL THE ENV VARIABLES"""
#TODO: fix .env
# API
load_dotenv('.env')
PROD_API = os.getenv('PATH_PROD_API', 'DEFAULT')
TEST_API = os.getenv('PATH_TEST_API', 'DEFAULT')

#SET UP LOGGING
LoggingMasterCrawler()

def main():
    # start master timer
    master_start_time = timeit.default_timer()

    #Start calling each crawler
    logging.info('ALL CRAWLERS DEPLOYED!')
    api_template(PROD_API) #1st: PROD_API or TEST_API
    
    #Move onto the next one
    rss_template("MAIN") #1st argument MAIN OR TEST

    #Move onto the next one
    selenium_template("MAIN") # 'MAIN', TEST or FREELANCE

    #print the time
    elapsed_time = timeit.default_timer() - master_start_time
    logging.info(f'ALL CRAWLERS FINISHED. ALL IN {elapsed_time:.2f} SECONDS!')

if __name__ == "__main__":
    main()

