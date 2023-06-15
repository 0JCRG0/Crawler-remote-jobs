#!/usr/local/bin/python3

import logging
import timeit
from rss_reader import read_rss
from other.indeed import indeed
from api_crawlers import api_template
from bs4_crawlers import bs4_template
from selenium_crawlers import selenium_template
from utils.handy import LoggingMasterCrawler

#SET UP LOGGING
LoggingMasterCrawler()

def main(pipeline):
    # start master timer
    master_start_time = timeit.default_timer()

    #Start calling each crawler
    logging.info('ALL CRAWLERS DEPLOYED!')
    print("\n", "ALL CRAWLERS DEPLOYED!")

    api_template(pipeline)
    
    read_rss(pipeline)
    
    bs4_template(pipeline)

    selenium_template(pipeline) 

    indeed(SCHEME="main_mx", KEYWORD="")

    #print the time
    elapsed_time = timeit.default_timer() - master_start_time
    logging.info(f'ALL CRAWLERS FINISHED. ALL IN {elapsed_time:.2f} SECONDS!')

if __name__ == "__main__":
    main("MAIN") #MAIN OR TEST

