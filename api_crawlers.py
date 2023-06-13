#!/usr/local/bin/python3

import requests
import json
import pretty_errors
import pandas as pd
import timeit
import os
import logging
from datetime import date
from datetime import datetime
from dotenv import load_dotenv
from utils.handy import *
from requests.exceptions import RequestException

""" LOAD THE ENVIRONMENT VARIABLES """

load_dotenv()

PROD = os.environ.get('JSON_PROD_API')
TEST = os.environ.get('JSON_TEST_API')
SAVE_PATH = os.environ.get('SAVE_PATH_API')


def api_template(pipeline):
    print("\n", "CRAWLER LAUNCHED ON APIs.")

    #Start the timer
    start_time = timeit.default_timer()

    """ DETERMINING WHICH JSON TO LOAD & WHICH POSTGRE TABLE WILL BE USED """

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
        print("\n", "Incorrect argument! Use either 'MAIN' or 'TEST' to run this script.", "\n")

    def api_fetcher():
        total_titles = []
        total_links = []
        total_descriptions = []
        total_pubdates = []
        total_locations = []
        total_ids=[]
        total_timestamps=[]
        rows = []

        with open(JSON) as f:
            #load the local json
            data = json.load(f)
            # Access the 'apis' list in the first dictionary of the 'data' list and assign it to the variable 'apis'
            apis = data[0]["apis"]
            for api_obj in apis:
                #Extract the name of the site
                name = api_obj['name']
                # Extract the 'api' key from the current dictionary and assign it to the variable 'api'
                api = api_obj['api']
                # Extract the first dictionary from the 'elements_path' list in the current dictionary and assign it to the variable 'elements_path'
                elements_path = api_obj['elements_path'][0]
                #Extract the class of the JSON
                class_json = api_obj['class_json']
                #Each site is different to a json file can give us the flexibility we need  
                headers = {"User-Agent": "my-app"}
                try:
                    print("\n", f"Requesting {name}...")
                    response = requests.get(api, headers=headers)
                    if response.status_code == 200:
                        data = json.loads(response.text)
                        print(f"Successful request on {api}", "\n")
                        
                        """Call the function depending on the JSON's class
                        If the data is inside another dict then we access it """
                        jobs = class_json_strategy(data, elements_path, class_json)

                        #Start loop if not None
                        if jobs is not None:
                            for job in jobs:
                                #IDs
                                #id = id_generator()
                                #total_ids.append(id)
                                #Titles
                                if elements_path["title_tag"] in job:
                                    title = elements_path["title_tag"]
                                    total_titles.append(job[title])
                                else:
                                    total_titles.append("NaN")
                                #Links
                                if elements_path["link_tag"] in job:
                                    link = elements_path["link_tag"]
                                    total_links.append(job[link])
                                else:
                                    total_links.append("NaN")
                                #Descriptions
                                if elements_path["description_tag"] in job:
                                    description = elements_path["description_tag"]
                                    total_descriptions.append(job[description])
                                else:
                                    total_descriptions.append("NaN")
                                #DATES
                                today = date.today()
                                total_pubdates.append(today)
                                #locations
                                if elements_path["location_tag"] in job:
                                    location = elements_path["location_tag"]
                                    total_locations.append(job[location])
                                else: 
                                    total_locations.append("NaN")
                                #TIMESTAMP
                                timestamp = datetime.now()
                                total_timestamps.append(timestamp)
                                #Put it all together...
                                rows = {'title': total_titles, 'link':total_links, 'description': total_descriptions, 'pubdate': total_pubdates, 'location': total_locations,'timestamp': total_timestamps}
                except RequestException as e:
                    print(f"Encountered a request error: {e}. Moving to the next API...")
                    logging.error(f"Encountered a request error: {e}. Moving to the next API...")
                    continue  # continue the execution
                except:
                    print(f"Encountered an unexpected error with {api}. CHECK.")
                    logging.error(f"Encountered an unexpected error with {api}. CHECK.")
                    continue
        return rows

    data = api_fetcher()
    
    #to df
    df = pd.DataFrame(data)

    def clean_postgre_api(df):

        """ Clean the rows accordingly """
        for col in df.columns:
            if col == 'location':
                #df[col] = df[col].astype(str).str.replace(r'{}', '', regex=True)
                df[col] = df[col].astype(str).apply(clean_rows)
            if col == 'description':
                df[col] = df[col].astype(str).apply(clean_rows).apply(cleansing_selenium_crawlers)


        df.to_csv(SAVE_PATH, index=False)

        #Log
        logging.info('Finished API crawlers. Results below ⬇︎')

        ## PostgreSQL
        POSTGRESQL(df)

        elapsed_time = timeit.default_timer() - start_time
        print("\n", f"Api crawlers have finished! all in: {elapsed_time:.2f} seconds", "\n") 
    clean_postgre_api(df)
    
if __name__ == "__main__":
    api_template("TEST")
