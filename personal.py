from selen_crawlers import indeed
import timeit

def MASTER():
    KEYWORDS_LIST = [
        "DATA+ANALYST",
        "DATA",
        "MACHINE+LEARNING",
        "PYTHON",
        "BUSINESS+ANALYST",
        "ANALISTA+DE+DATOS",
        "INGENIERO+DE+DATOS",
        "DATA+ENGINEER",
        "SQL",
        "BUSINESS+INTELLIGENCE",
        "DATABASE+ADMINISTRATOR"
        ]
    PAGES = 5
    COUNTRY = "MX"
    
    # start master timer
    master_start_time = timeit.default_timer()

    #loop to get the jobs 
    for KEYWORD in KEYWORDS_LIST:
        indeed(PAGES, COUNTRY, KEYWORD)
        print("\n", "MOVING ON...","\n")
    

    #print the time
    elapsed_time = timeit.default_timer() - master_start_time / 60
    print("\n", f"DONE! mx.indeed's crawler finished in: {elapsed_time:.1f} minutes, not bad", "\n")

if __name__ == "__main__":
    MASTER()