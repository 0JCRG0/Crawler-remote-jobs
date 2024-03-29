import feedparser
import timeit
import os
import json
import pretty_errors
from selenium import webdriver
from selenium.webdriver.common.by import By
import sys
import bs4
import requests
from datetime import date, datetime
from dotenv import load_dotenv
from sql.clean_loc import clean_location_rows
from utils.handy import *
import asyncio
import aiohttp

""" LOAD THE ENVIRONMENT VARIABLES """

#TODO: ADD CLEANING FUNCTION TO DESC

load_dotenv()

PROD = os.environ.get('JSON_PROD_RSS_READER')
TEST = os.environ.get('JSON_TEST_RSS_READER')
SAVE_PATH = os.environ.get('SAVE_PATH_RSS_READER')


def read_rss(pipeline):
    start_time = timeit.default_timer()

    #START SEL
    options = webdriver.FirefoxOptions()
    options.add_argument('-headless')
                                    
    # Start the session
    driver = webdriver.Firefox(options=options)
    
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
                inner_link_tag = url_obj['inner_link_tag']
                #ADD MORE VARIABLES IF REQUIRED
                try:
                    feed = feedparser.parse(url)
                    if feed.status == 200:
                        for entry in feed.entries:
                            # create a new dictionary to store the data for the current job
                            job_data = {}

                            job_data["title"] = entry.title if 'title' in entry else "NaN"
                            #print(job_data["title"])

                            job_data["link"] = entry.link if 'link' in entry else "NaN"
                            

                            """ Access each scraped link to get the description """
                            link_res = requests.get(job_data["link"])
                            if link_res.status_code == 200:
                                print(f"""CONNECTION ESTABLISHED ON {job_data["link"]}""", "\n")
                                link_soup = bs4.BeautifulSoup(link_res.text, 'html.parser')
                                description_tag = link_soup.select_one(inner_link_tag)
                                if description_tag:
                                    job_data["description"] = description_tag.text
                                else:
                                    job_data["description"] = 'NaN'
                            elif link_res.status_code == 403:
                                print(f"""CONNECTION PROHIBITED WITH BS4 ON {job_data["link"]}. STATUS CODE: "{link_res.status_code}". TRYING WITH SELENIUM""", "\n")
                                
                                driver.get(job_data["link"])
                                description_tag = driver.find_element(By.CSS_SELECTOR, inner_link_tag)
                                if description_tag:
                                    job_data["description"] = description_tag.get_attribute("innerHTML") 
                                else:
                                    job_data["description"] = 'NaN'
                            else:
                                print(f"""CONNECTION FAILED ON {job_data["link"]}. STATUS CODE: "{link_res.status_code}". Getting the description from RSS FEED.""", "\n")
                                # Get the descriptions & append it to its list
                                job_data["description"]= entry.description if 'description' in entry else "NaN"
                            
                            today = date.today()
                            job_data["pubdate"] = today

                            job_data["location"] = getattr(entry, loc_tag) if hasattr(entry, loc_tag) else "NaN"

                            #job_data["description"]= entry.description if 'description' in entry else "NaN"

                            timestamp = datetime.now()
                            job_data["timestamp"] = timestamp
                            

                            # add the data for the current job to the rows list
                            total_links.append(job_data["link"])
                            total_titles.append(job_data["title"])
                            total_pubdates.append(job_data["pubdate"])
                            total_locations.append(job_data["location"])
                            total_timestamps.append(job_data["timestamp"])
                            total_descriptions.append(job_data["description"])
                    else:
                        print(f"""PARSING FAILED ON {feed}. SKIPPING...""", "\n")
                        continue
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
        #Cleaning columns
        for col in df.columns:
            if col == 'location':
                df[col] = df[col].apply(clean_location_rows)
            elif col == 'description':
                df[col] = df[col].apply(clean_other_rss)
        # SEND IT TO TO PostgreSQL    
        POSTGRESQL(df)
    processing()

    elapsed_time = timeit.default_timer() - start_time
    print("\n", f"RSS Reader is done! all in: {elapsed_time:.2f} seconds", "\n")


if __name__ == "__main__":
    read_rss("TEST")
