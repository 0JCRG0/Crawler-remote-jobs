from APIs import w_nomads
from rss import rss_abdy, rss_ymd
from SELENIUM import himalayas
import timeit

def MASTER():
    # start master timer
    master_start_time = timeit.default_timer()

    #Start calling each crawler
    w_nomads()
    
    #Move onto the next one
    print("\n", "MOVING ON...","\n")

    rss_abdy()
    #Move onto the next one
    print("\n", "MOVING ON...","\n")

    rss_ymd()
    #Move onto the next one
    print("\n", "MOVING ON...","\n")

    himalayas(1) #1st argument is pages to scrap...

    #print the time
    elapsed_time = timeit.default_timer() - master_start_time
    print("\n", f"DONE! all crawlers finished in: {elapsed_time:.2f} seconds, not bad", "\n")

if __name__ == "__main__":
    MASTER()