#!/usr/local/bin/python3

import logging
import timeit
from api_crawlers import api_crawlers
from RSS import rss_abdy, rss_ymd
from selenium_template import selenium_crawlers
from datetime import date, timedelta
from utils.handy import LoggingMasterCrawler
import os
from dotenv import load_dotenv

""" LOAD ALL THE ENV VARIABLES"""

# API
load_dotenv('.env')
PROD_API = os.getenv('PATH_PROD_API', 'DEFAULT')
TEST_API = os.getenv('PATH_TEST_API', 'DEFAULT')

#SET UP LOGGING
LoggingMasterCrawler()

def MASTER():

    CUT_OFF_YDAY = date.today() - timedelta(days=2) #FROM YDAY ONWARDS
    # start master timer
    master_start_time = timeit.default_timer()

    #Start calling each crawler
    logging.info('CRAWLER MASTER IS STARTING!')
    api_crawlers(PROD_API, "MAIN") #1st: JSON, 2nd:POSTGRE(MAIN OR TEST)
    
    #Move onto the next one
    rss_abdy(CUT_OFF_YDAY) #1st argument is the cut-off date
    
    #Move onto the next one
    rss_ymd(CUT_OFF_YDAY) #1st argument is the cut-off date
    
    #Move onto the next one
    selenium_crawlers('MAIN') #Either 'MAIN' or FREELANCE

    #print the time
    elapsed_time = timeit.default_timer() - master_start_time
    logging.info(f'CRAWLER MASTER FINISHED. ALL IN {elapsed_time:.2f} SECONDS!')

if __name__ == "__main__":
    MASTER()

