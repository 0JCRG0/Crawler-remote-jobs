#!/usr/local/bin/python3

import csv
import requests
import pandas as pd
import bs4 
from urllib.error import HTTPError
import lxml
import re
import psycopg2
import numpy as np
import pretty_errors
from datetime import date, datetime
import timeit
import os
import json
import logging
from datetime import datetime
from dotenv import load_dotenv
from utils.handy import *

""" LOAD THE ENVIRONMENT VARIABLES """

load_dotenv()

PROD = os.environ.get('JSON_PROD_RSS')
TEST = os.environ.get('JSON_TEST_RSS')
SAVE_PATH = os.environ.get('SAVE_PATH_RSS')


def rss_template(pipeline):

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

    #print("\n", f"Reading {file}... ", "\n")
    print("\n", "Crawler launched on RSS Feeds.")

    def soups():
        soup_list = []
        with open(JSON) as f:
                #load the json
            urls = json.load(f)
                # Access the 'apis' list in the first dictionary of the 'data' list and assign it to the variable 'apis'
            for i in urls:
                url = i["url"]
                print(url)
                try:
                    res = requests.get(url)
                    if res.status_code != 200:
                        print(f"Received non-200 response ({res.status_code}) for URL {url}. Skipping...")
                        continue
                    res.raise_for_status()
                    soup = bs4.BeautifulSoup(res.text, 'lxml-xml')
                    soup_list.append(soup)
                except HTTPError as e:
                    if e.code == 403:
                        print(f"An error occurred: {e}. Skipping URL {url}")
                        rss = None
                    else:
                        raise
        return soup_list
    all_soups = soups()

    print("\n", f"Soup made from {len(all_soups)} established connections.")


    def all_elements():
        rows = []
        total_pubdates = []
        total_titles = []
        total_links = []
        total_locations = []
        total_descriptions = []
        total_timestamps=[]
        for soup in all_soups:
            for item in soup.find_all('item'):
                #IDs
                #id = id_generator()
                #total_ids.append(id)
                # Get titles & append it to the list
                title_tag = item.find('title')
                if title_tag is not None:
                    title = title_tag.get_text(strip=True)
                    total_titles.append(title)
                else:
                    total_titles.append('NaN')
                # Get links & append it to the list
                link_tag = item.find('link')
                if link_tag is not None:
                    link = link_tag.get_text(strip=True)
                    total_links.append(link)
                else:
                    total_links.append('NaN')
                # Get the pubdate of different tags
                today = date.today()
                total_pubdates.append(today)
                # Get the locations & append it to its list
                location_tag = item.find('location')
                if location_tag is not None:
                    location = location_tag.get_text(strip=True)
                    total_locations.append(location)
                else:
                    total_locations.append('NaN')
                # Get the descriptions & append it to its list
                description_tag = item.find('description')
                if description_tag is not None:
                    description = str(item.description.get_text(strip=True))
                    total_descriptions.append(description)
                else:
                    total_descriptions.append('NaN')
                #timestamps
                timestamp = datetime.now()
                total_timestamps.append(timestamp)
                rows = {'title':total_titles, 'link':total_links, 'description': total_descriptions, 'pubdate': total_pubdates, 'location': total_locations, 'timestamp': total_timestamps}
        return rows
    data = all_elements()

    #Convert data to a pandas df for further analysis
    data_dic = dict(data)
    df = pd.DataFrame.from_dict(data_dic, orient='index')
    df = df.transpose()

    def clean_postgre_rss(df):
        
        #Cleaning columns
        for col in df.columns:
            if col == 'link':
                df[col] = df[col].apply(clean_link_rss)
            else:
                df[col] = df[col].apply(clean_other_rss)

        df.fillna("NaN", inplace=True)
        
        df.to_csv(SAVE_PATH, index=False)

        logging.info('Finished RSS CRAWLERS. Results below ⬇︎')
        ## PostgreSQL
        POSTGRESQL(df)

        #print the time
        elapsed_time = timeit.default_timer() - start_time
        print("\n", f"rss_crawlers are done! all in: {elapsed_time:.2f} seconds", "\n")
    clean_postgre_rss(df)


if __name__ == "__main__":
    rss_template("TEST")


