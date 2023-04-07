#!/usr/local/bin/python3

import logging
import timeit
from api_crawlers import api_crawlers
from rss import rss_abdy, rss_ymd
from selenium_template import selenium_crawlers

#You need to use YOUR full path
logging.basicConfig(filename='/Users/juanreyesgarcia/Library/CloudStorage/OneDrive-FundacionUniversidaddelasAmericasPuebla/DEVELOPER/PROJECTS/CRAWLER_ALL/logs/master_crawler.log', level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')

def MASTER():
    CUT_OFF = '2023-03-29'
    # start master timer
    master_start_time = timeit.default_timer()

    #Start calling each crawler
    logging.info('Starting API crawlers...')
    api_crawlers(CUT_OFF) #1st argument is the cut-off date
    
    #Move onto the next one
    logging.info('API crawlers finished. Moving on to RSS crawlers...')
    rss_abdy(CUT_OFF) #1st argument is the cut-off date
    
    #Move onto the next one
    logging.info('RSS crawlers finished. Moving on to YMD crawler...')
    rss_ymd(CUT_OFF) #1st argument is the cut-off date
    
    #Move onto the next one
    logging.info('YMD crawler finished. Moving on to selenium crawler...')
    selenium_crawlers('MAIN') #Either 'MAIN' or FREELANCE

    #print the time
    elapsed_time = timeit.default_timer() - master_start_time
    logging.info(f'All crawlers finished in {elapsed_time:.2f} seconds')

if __name__ == "__main__":
    MASTER()