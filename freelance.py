#!/usr/local/bin/python3
from RSS import rss_freelance
from selenium_template import selenium_crawlers
import timeit
import logging
from datetime import date, timedelta
from utils.handy import LoggingFreelanceCrawler

#SET UP LOGGING
LoggingFreelanceCrawler()

def freelance_all():
    
    CUT_OFF_YDAY = date.today() - timedelta(days=2) #FROM YDAY ONWARDS
    # start master timer
    master_start_time = timeit.default_timer()

    #Start calling each crawler
    logging.info('FREELANCE CRAWLER IS STARTING!')
    rss_freelance(CUT_OFF_YDAY) #1st argument is the cut-off date
    
    #Move onto the next one
    selenium_crawlers('FREELANCE') #Either 'MAIN' or FREELANCE

    #print the time
    elapsed_time = timeit.default_timer() - master_start_time
    logging.info(f'FREELANCE CRAWLER FINISHED. ALL IN {elapsed_time:.2f} SECONDS!')

if __name__ == "__main__":
    freelance_all()