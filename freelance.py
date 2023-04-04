from rss import rss_freelance
from selenium_template import selenium_crawlers
import timeit

def freelance_all():
    CUT_OFF = '2023-03-29'
    # start master timer
    master_start_time = timeit.default_timer()

    print("\n", "Crawlers getting Freelancer jobs","\n")


    rss_freelance(CUT_OFF) #1st argument is the cut-off date
    
    #Move onto the next one
    print("\n", "MOVING ON...","\n")

    selenium_crawlers('FREELANCE') #Either 'MAIN' or FREELANCE

    #print the time
    elapsed_time = timeit.default_timer() - master_start_time
    print("\n", f"DONE! all crawlers finished in: {elapsed_time:.2f} seconds, not bad", "\n")

if __name__ == "__main__":
    freelance_all()