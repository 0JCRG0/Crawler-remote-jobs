from api_fetchers import w_nomads
from rss import rss_abdy, rss_ymd
from selenium_template import selenium_crawlers
import timeit

def MASTER():
    CUT_OFF = '2023-03-29'
    # start master timer
    master_start_time = timeit.default_timer()

    #Start calling each crawler
    w_nomads(CUT_OFF) #1st argument is the cut-off date
    
    #Move onto the next one
    print("\n", "MOVING ON...","\n")

    rss_abdy(CUT_OFF) #1st argument is the cut-off date
    
    #Move onto the next one
    print("\n", "MOVING ON...","\n")

    rss_ymd(CUT_OFF) #1st argument is the cut-off date
    
    #Move onto the next one
    print("\n", "MOVING ON...","\n")

    selenium_crawlers('MAIN') #Either 'MAIN' or FREELANCE

    #print the time
    elapsed_time = timeit.default_timer() - master_start_time
    print("\n", f"DONE! all crawlers finished in: {elapsed_time:.2f} seconds, not bad", "\n")

if __name__ == "__main__":
    MASTER()