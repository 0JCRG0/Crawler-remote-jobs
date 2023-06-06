import bs4
import pandas as pd
import re
import pretty_errors
from urllib.error import HTTPError
import timeit
from dateutil.parser import parse
from datetime import date, datetime
import json
import logging
import requests
import os
from dotenv import load_dotenv
from utils.handy import *


""" LOAD THE ENVIRONMENT VARIABLES """

load_dotenv()

PROD = os.environ.get('JSON_PROD_BS4')
TEST = os.environ.get('JSON_TEST_BS4')
SAVE_PATH = os.environ.get('SAVE_PATH_BS4')

def bs4_template(pipeline):
    print("\n", "Crawler is using bs4 strategy.")

    #start timer
    start_time = timeit.default_timer()

    """ DETERMINING WHICH JSON TO LOAD & WHICH POSTGRE TABLE WILL BE USED """

    if pipeline == 'MAIN':
        if PROD:
            JSON = PROD
        POSTGRESQL = to_postgre
        print("\n", f"Pipeline is set to 'MAIN'. Jobs will be sent to PostgreSQL's main_jobs table", "\n")
        # configure the logger
        LoggingMasterCrawler()
    elif pipeline == 'FREELANCE':
        #TODO: FIX - ADD PÁTH
        JSON = '/selenium_resources/freelance.json'
        POSTGRESQL = freelance_postgre
        # configure the logger
        LoggingFreelanceCrawler()
        #print("\n", f"Reading {JSON}. Jobs will be sent to PostgreSQL's freelance table", "\n")
    elif pipeline == 'TEST':
        if TEST:
            JSON = TEST
        POSTGRESQL = test_postgre
        print("\n", f"Pipeline is set to 'TEST'. Jobs will be sent to PostgreSQL's test table", "\n")
        # configure the logger
        LoggingMasterCrawler()
    else:
        print("\n", "Incorrect argument! Use 'MAIN', 'TEST' or 'FREELANCE' to run this script.", "\n")


    def elements():
        total_links = []
        total_titles = []
        total_pubdates = []
        total_locations = [] 
        total_descriptions = []
        total_timestamps = []
        rows = {"title": total_titles,
                "link": total_links,
                "description": total_descriptions,
                "pubdate": total_pubdates,
                "location": total_locations,
                "timestamp": total_timestamps}

        #load the json
        with open(JSON) as f:
            data = json.load(f)
            # Access the 'urls' list in the first dictionary of the 'data' list and assign it to the variable 'urls'
            urls = data[0]["urls"]
            for url_obj in urls:
                #Extract the name of the crawler
                name = url_obj['name']
                print("\n", f"{name} has started", "\n")
                # Extract the 'url' key from the current dictionary and assign it to the variable 'url_prefix'
                url_prefix = url_obj['url']
                # Extract the first dictionary from the 'elements_path' list in the current dictionary and assign it to the variable 'elements_path'
                elements_path = url_obj['elements_path'][0]
                #Each site is different to a json file can give us the flexibility we need
                pages = url_obj['pages_to_crawl']
                #Extract the number in which the range is going to start from
                start_point = url_obj['start_point']
                # You need to +1 because range is exclusive
                for i in range(start_point, pages + 1):
                    #The url from json is incomplete, we need to get the number of the page we are scrapping
                    ##We do that in the following line
                    url = url_prefix + str(i)
                    # get the url
                    try:
                        res = requests.get(url)
                        if res.status_code != 200:
                            print(f"Received non-200 response ({res.status_code}) for URL {url}. Skipping...")
                            continue
                        res.raise_for_status()
                        soup = bs4.BeautifulSoup(res.text, 'lxml')
                        #print(soup.prettify())
                        print(f"Crawling... {url}")
                        # establish waiting strategy
                        # GETTING THE PARENT...
                        jobs = soup.select(elements_path["jobs_path"])
                        for job in jobs:
                            # create a new dictionary to store the data for the current job
                            job_data = {}

                            title_element = job.select_one(elements_path["title_path"])
                            job_data["title"] = title_element.text if title_element else "NaN"

                            link_element = job.select_one(elements_path["link_path"])
                            job_data["link"] = name + link_element["href"] if link_element else "NaN"

                            today = date.today()
                            job_data["pubdate"] = today

                            location_element = job.select_one(elements_path["location_path"])
                            job_data["location"] = location_element.text if location_element else "NaN"

                            description_element = job.select_one(elements_path["description_path"])
                            job_data["description"]= description_element.text if description_element else "NaN"

                            timestamp = datetime.now()
                            job_data["timestamp"] = timestamp
                                
                            # add the data for the current job to the rows list
                            total_links.append(job_data["link"])
                            total_titles.append(job_data["title"])
                            total_pubdates.append(job_data["pubdate"])
                            total_locations.append(job_data["location"])
                            total_timestamps.append(job_data["timestamp"])
                            total_descriptions.append(job_data["description"])
                    except HTTPError as e:
                        if e.code == 403:
                            print(f"An error occurred: {e}. Skipping URL {url}")
                            pass
                        else:
                            print(f"Unexpected error. Skipping URL {url}. CHECK!")
                            pass
        return rows
    data = elements()

    #-> DF
    df = pd.DataFrame(data)

    # count the number of duplicate rows
    num_duplicates = df.duplicated().sum()

            # print the number of duplicate rows
    print("Number of duplicate rows:", num_duplicates)

            # remove duplicate rows based on all columns
    df = df.drop_duplicates()
        
    def clean_postgre_bs4(df):
        
        #CLEANING AVOIDING DEPRECATION WARNING
        for col in df.columns:
            if col == 'title' or col == 'description' or col == 'location':
                i = df.columns.get_loc(col)
                newvals = df.loc[:, col].astype(str).apply(cleansing_selenium_crawlers)
                df[df.columns[i]] = newvals
            
        #Save it in local machine
        df.to_csv(SAVE_PATH, index=False)

        #Log it 
        logging.info('Finished Selenium_Crawlers. Results below ⬇︎')
        
        # SEND IT TO TO PostgreSQL    
        POSTGRESQL(df)

        #print the time
        elapsed_time = timeit.default_timer() - start_time
        print("\n")
        print(f"BS4 crawlers finished! all in: {elapsed_time:.2f} seconds.", "\n")
    clean_postgre_bs4(df)

if __name__ == "__main__":
    bs4_template('MAIN') 