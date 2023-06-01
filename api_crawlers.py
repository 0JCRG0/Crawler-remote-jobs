#!/usr/local/bin/python3

import requests
import json
import pretty_errors
import pandas as pd
import timeit
import os
import logging
from dotenv import load_dotenv
from utils.handy import test_postgre, api_pubdate, class_json_strategy, to_postgre, clean_rows, cleansing_selenium_crawlers, LoggingMasterCrawler

#TODO: SORT OUT THE DATES. IF THIS SCRIPT RUNS DAILY THERE IS NO NEED TO GET ALL THE JOBS

# Load the .env file & access variables
load_dotenv('.env')
PROD = os.getenv('PATH_PROD_API', 'DEFAULT')
TEST = os.getenv('PATH_TEST_API', 'DEFAULT')
OUTPUT = os.getenv('OUTPUT_API', 'DEFAULT')

#Import logging
LoggingMasterCrawler()

def api_crawlers(cut_off):
    print(type(cut_off))

    #Start the timer
    start_time = timeit.default_timer()

    def api_fetcher():
        total_titles = []
        total_links = []
        total_descriptions = []
        total_pubdates = []
        total_locations = []
        rows = []

        with open(PROD) as f:
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
                response = requests.get(api)
                if response.status_code == 200:
                    data = json.loads(response.text)
                    #print just to test
                    #pretty_json = json.dumps(data, indent=4)
                    #print(pretty_json, type(data))
                    
                    print("\n", f"Fetching...{api}", "\n")
                    
                    #Call the function depending on the JSON's class
                    ##If the data is inside another dict then we access it 
                    jobs = class_json_strategy(data, elements_path, class_json)

                    #Start loop if not None
                    if jobs is not None:
                        for job in jobs:
                            if elements_path["title_tag"] in job:
                                title = elements_path["title_tag"]
                                total_titles.append(job[title])
                            else:
                                total_titles.append("NaN")
                            if elements_path["link_tag"] in job:
                                link = elements_path["link_tag"]
                                total_links.append(job[link])
                            else:
                                total_links.append("NaN")
                            if elements_path["description_tag"] in job:
                                description = elements_path["description_tag"]
                                total_descriptions.append(job[description])
                            else:
                                total_descriptions.append("NaN")
                            if elements_path["pubdate_tag"] in job:
                                pubdate = elements_path["pubdate_tag"]
                                total_pubdates.append(job[pubdate])
                            else: 
                                total_pubdates.append("NaN")
                            if elements_path["location_tag"] in job:
                                location = elements_path["location_tag"]
                                total_locations.append(job[location])
                            else: 
                                total_locations.append("NaN")
                            #Put it all together...
                            rows = {'title': total_titles, 'link':total_links, 'description': total_descriptions, 'pubdate': total_pubdates, 'location': total_locations}
        return rows

    data = api_fetcher()
    
    #to df
    df = pd.DataFrame(data)

    #Converting pubdate into datetime object
    for col in df.columns:
        if col == 'pubdate':
            df[col] = df[col].apply(api_pubdate)
            df[col] = pd.to_datetime(df[col], errors="coerce", infer_datetime_format=True, exact=False)
        if col == 'location':
            #df[col] = df[col].astype(str).str.replace(r'{}', '', regex=True)
            df[col] = df[col].astype(str).apply(clean_rows)
        if col == 'description':
            df[col] = df[col].astype(str).apply(clean_rows).apply(cleansing_selenium_crawlers)


    df.to_csv(OUTPUT, index=False)
            
    #Filter rows by a date range (this reduces the number of rows... duh)
    start_date = pd.to_datetime('2016-01-01')
    end_date = pd.to_datetime(cut_off)
    date_range_filter = (df['pubdate'] >= start_date) & (df['pubdate'] <= end_date)
    df = df.loc[~date_range_filter]

    #Slice desdriptions
    df['description'] = df['description'].str.slice(0, 2000)

    #Log
    logging.info('Finished API crawlers. Results below ⬇︎')

    #Get rid of USA jobs
    #new_df = bye_usa(df)

    ## PostgreSQL
    to_postgre(df)

    elapsed_time = timeit.default_timer() - start_time
    print("\n", f"Api crawlers have finished! all in: {elapsed_time:.2f} seconds", "\n") 

if __name__ == "__main__":
    api_crawlers('2023-03-29') #1st argument is cut-off date
