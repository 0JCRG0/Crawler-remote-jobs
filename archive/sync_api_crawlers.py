#!/usr/local/bin/python3

import requests
import json
import pretty_errors
import pandas as pd
import timeit
import os
import logging
from selenium import webdriver
from selenium.webdriver.common.by import By
import bs4
from datetime import date
from datetime import datetime
from dotenv import load_dotenv
from utils.handy import *
from sql.clean_loc import clean_location_rows
from requests.exceptions import RequestException

#TODO: REFACTOR - MIMICKING BS4 or RSS READ

""" LOAD THE ENVIRONMENT VARIABLES """

load_dotenv()

PROD = os.environ.get('JSON_PROD_API')
TEST = os.environ.get('JSON_TEST_API')
SAVE_PATH = os.environ.get('SAVE_PATH_API')


def api_template(pipeline):
    print("\n", "REQUEST TO APIs HAS STARTED.")

    #Start the timer
    start_time = timeit.default_timer()

    #START SEL IF REQUIRED
    options = webdriver.FirefoxOptions()
    options.add_argument('-headless')
                                                                    
    # Start the session
    driver = webdriver.Firefox(options=options)
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
                #Whether to follow link
                follow_link = api_obj['follow_link']
                #Extract inner link if follow link
                inner_link_tag = api_obj['inner_link_tag']
                #Each site is different to a json file can give us the flexibility we need  
                headers = {"User-Agent": "my-app"}
                try:
                    print("\n", f"Requesting {name}...")
                    response = requests.get(api, headers=headers)
                    if response.status_code != 200:
                        print(f"Received non-200 response ({response.status_code}) for API: {api}. Skipping...")
                        logging.error(f"Received non-200 response ({response.status_code}) for API: {api}. Skipping...")
                        continue
                    elif response.status_code == 200:
                        data = json.loads(response.text)
                        print(f"Successful request on {api}", "\n")
                        
                        """Call the function depending on the JSON's class
                        If the data is inside another dict then we access it """
                        jobs = class_json_strategy(data, elements_path, class_json)

                        #Start loop if not None
                        if jobs:
                            for job in jobs:
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
                                
                                """ IF IT NEEDS TO FOLLOW LINK OR NOT """
                                if follow_link == "yes":
                                    """ Access each scraped link to get the description """
                                    link_res = requests.get(job[link])
                                    if link_res.status_code == 200:
                                        print(f"""CONNECTION ESTABLISHED ON {job[link]}""", "\n")
                                        link_soup = bs4.BeautifulSoup(link_res.text, 'html.parser')
                                        description_tag = link_soup.select_one(inner_link_tag)
                                        if description_tag:
                                            description = description_tag.text
                                            total_descriptions.append(description)
                                        else:
                                            description = 'NaN'
                                            total_descriptions.append(description)

                                    elif link_res.status_code == 403:
                                        print(f"""CONNECTION PROHIBITED WITH BS4 ON {job[link]}. STATUS CODE: "{link_res.status_code}". TRYING WITH SELENIUM""", "\n")
                                        driver.get(job[link])
                                        description_tag = driver.find_element(By.CSS_SELECTOR, inner_link_tag)
                                        if description_tag:
                                            description = description_tag.get_attribute("innerHTML")
                                            total_descriptions.append(description)
                                        else:
                                            description = 'NaN'
                                            total_descriptions.append(description)
                                    else:
                                        print(f"""CONNECTION FAILED ON {job[link]}. STATUS CODE: "{link_res.status_code}". Getting the description from API.""", "\n")
                                        # Get the descriptions & append it to its list
                                        if elements_path["description_tag"] in job:
                                            description = elements_path["description_tag"]
                                            total_descriptions.append(job[description])
                                        else:
                                            total_descriptions.append("NaN")
                                else:
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
                df[col] = df[col].apply(clean_location_rows)
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
