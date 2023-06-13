import feedparser
import timeit
import os
import json
import pretty_errors
import sys
from datetime import date, datetime
from dotenv import load_dotenv

""" LOAD THE ENVIRONMENT VARIABLES """

load_dotenv()

PROD = os.environ.get('JSON_PROD_RSS_READER')
TEST = os.environ.get('JSON_TEST_RSS_READER')
SAVE_PATH = os.environ.get('SAVE_PATH_RSS_READER')
UTILS_PATH = os.environ.get('SYS_PATH_APPEND', "")

""" APPEND UTILS' PATH """
sys.path.append(UTILS_PATH)
from handy import *

def read_rss(pipeline):
    start_time = timeit.default_timer()
    
    """ TEST or PROD"""

    if pipeline == 'MAIN':
        if PROD:
            JSON = PROD
        POSTGRESQL = to_postgre
        print("\n", f"Pipeline is set to 'MAIN'. Jobs will be sent to PostgreSQL's main_jobs table", "\n")
        # configure the logger
        LoggingMasterCrawler()
    elif pipeline == 'TEST':
        if TEST:
            JSON = TEST
        POSTGRESQL = test_postgre
        print("\n", f"Pipeline is set to 'TEST'. Jobs will be sent to PostgreSQL's test table", "\n")
        # configure the logger
        LoggingMasterCrawler()
    else:
        print("\n", "Incorrect argument! Use 'MAIN' or 'TEST' to run this script.", "\n")

    #print("\n", f"Reading {file}... ", "\n")
    print("\n", "Crawler launched on RSS Feeds.")

    def soups_elements():
        total_pubdates = []
        total_titles = []
        total_links = []
        total_locations = []
        total_descriptions = []
        total_timestamps=[]
        rows = {"title": total_titles,
                "link": total_links,
                "description": total_descriptions,
                "pubdate": total_pubdates,
                "location": total_locations,
                "timestamp": total_timestamps}

        with open(JSON) as f:
            #load the json
            data = json.load(f)
            urls = data[0]["urls"]
            for url_obj in urls:
                url = url_obj['url']
                loc_tag = url_obj['location_tag']
                #ADD MORE VARIABLES IF REQUIRED
                try:
                    feed = feedparser.parse(url)
                    for entry in feed.entries:
                        # create a new dictionary to store the data for the current job
                        job_data = {}

                        job_data["title"] = entry.title if 'title' in entry else "NaN"
                        #print(job_data["title"])

                        job_data["link"] = entry.link if 'link' in entry else "NaN"

                        today = date.today()
                        job_data["pubdate"] = today

                        job_data["location"] = entry.location if 'location' in entry else "NaN"

                        job_data["description"]= entry.description if 'description' in entry else "NaN"

                        timestamp = datetime.now()
                        job_data["timestamp"] = timestamp
                        

                        # add the data for the current job to the rows list
                        total_links.append(job_data["link"])
                        total_titles.append(job_data["title"])
                        total_pubdates.append(job_data["pubdate"])
                        total_locations.append(job_data["location"])
                        total_timestamps.append(job_data["timestamp"])
                        total_descriptions.append(job_data["description"])
                        

                except Exception as e:
                    print(f"EXCEPTION: {e}")
                    continue
        return rows
    data = soups_elements()
    #-> DF
    df = pd.DataFrame(data) # type: ignore
    
    #Save it in local machine
    df.to_csv(SAVE_PATH, index=False)

        #Log it 
    logging.info('Finished RSS Reader. Results below ⬇︎')
    
    def processing():
        # SEND IT TO TO PostgreSQL    
        POSTGRESQL(df)
    processing()

    elapsed_time = timeit.default_timer() - start_time
    print("\n", f"RSS Reader is done! all in: {elapsed_time:.2f} seconds", "\n")


read_rss('TEST')
