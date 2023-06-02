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

# Load the .env file & access variables
load_dotenv('.env')
PROD_API = os.getenv('PATH_PROD_API', 'DEFAULT')
TEST_API = os.getenv('PATH_TEST_API', 'DEFAULT')
OUTPUT = os.getenv('OUTPUT_API', 'DEFAULT')

#Import logging
LoggingMasterCrawler()

def api_crawlers(prod_or_test, postgre):

    #Start the timer
    start_time = timeit.default_timer()

    def api_fetcher():
        total_titles = []
        total_links = []
        total_descriptions = []
        total_pubdates = []
        total_locations = []
        total_ids=[]
        total_timestamps=[]
        rows = []

        with open(prod_or_test) as f:
            #load the json
            data = json.load(f)
            # Access the 'apis' list in the first dictionary of the 'data' list and assign it to the variable 'apis'
            apis = data[0]["apis"]
            for api_obj in apis:
                #Extract the name of the site
                name = api_obj['name']
                print("\n", f"CRAWLING {name}...", "\n")
                # Extract the 'api' key from the current dictionary and assign it to the variable 'api'
                api = api_obj['api']
                # Extract the first dictionary from the 'elements_path' list in the current dictionary and assign it to the variable 'elements_path'
                elements_path = api_obj['elements_path'][0]
                #Extract the class of the JSON
                class_json = api_obj['class_json']
                #Each site is different to a json file can give us the flexibility we need  
                #Request the api...
                try:
                    response = requests.get(api)
                    if response.status_code == 200:
                        data = json.loads(response.text)
                        #print just to test
                        #pretty_json = json.dumps(data, indent=4)
                        #print(pretty_json, type(data))
                        
                        print("\n", f"Fetching...{api}", "\n")
                        
                        """Call the function depending on the JSON's class
                        If the data is inside another dict then we access it """
                        jobs = class_json_strategy(data, elements_path, class_json)

                        #Start loop if not None
                        if jobs is not None:
                            for job in jobs:
                                #IDs
                                id = id_generator(5)
                                total_ids.append(id)
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
                                #MODIFYING DATES
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
                                rows = {'id': total_ids, 'title': total_titles, 'link':total_links, 'description': total_descriptions, 'pubdate': total_pubdates, 'location': total_locations,'timestamp': total_timestamps}
                except RequestException as e:
                    print(f"Encountered a request error: {e}. Moving to the next API...")
                    pass  # continue the execution
                except:
                    print("Unexpected error")
                    pass
        return rows

    data = api_fetcher()
    
    #to df
    df = pd.DataFrame(data)

    """ Clean the rows accordingly """
    for col in df.columns:
        if col == 'location':
            #df[col] = df[col].astype(str).str.replace(r'{}', '', regex=True)
            df[col] = df[col].astype(str).apply(clean_rows)
        if col == 'description':
            df[col] = df[col].astype(str).apply(clean_rows).apply(cleansing_selenium_crawlers)


    df.to_csv(OUTPUT, index=False)

    #Slice desdriptions
    df['description'] = df['description'].str.slice(0, 2000)

    #Log
    logging.info('Finished API crawlers. Results below ⬇︎')

    ## PostgreSQL
    if postgre == "MAIN":
        to_postgre(df)
    elif postgre == "TEST":
        test_postgre(df)

    elapsed_time = timeit.default_timer() - start_time
    print("\n", f"Api crawlers have finished! all in: {elapsed_time:.2f} seconds", "\n") 
    
if __name__ == "__main__":
    api_crawlers(PROD_API, "TEST")
