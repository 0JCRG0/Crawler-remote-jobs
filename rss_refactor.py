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
from utils.handy import *

#EXPORT THE PATH - YOU NEED TO EXPORT YOUR OWN PATH & SAVE IT AS 'CRAWLER_ALL'
PATH = '/Users/juanreyesgarcia/Library/CloudStorage/OneDrive-FundacionUniversidaddelasAmericasPuebla/DEVELOPER/PROJECTS/CRAWLER_ALL'

#TODO: crawl coin w another crawler ffs

def rss_refactor(postgre):

    start_time = timeit.default_timer()

    JSON = '/Users/juanreyesgarcia/Library/CloudStorage/OneDrive-FundacionUniversidaddelasAmericasPuebla/DEVELOPER/PROJECTS/CRAWLER_ALL/rss_resources/main.json'
    #print("\n", f"Reading {file}... ", "\n")
    print("\n", "RSS_YMD starting now.")

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

    print("\n", f"Making soup from {len(all_soups)} established connections.")


    def all_elements():
        rows = []
        total_pubdates = []
        total_titles = []
        total_links = []
        total_locations = []
        total_descriptions = []
        total_ids = []
        total_timestamps=[]
        for soup in all_soups:
            for item in soup.find_all('item'):
                #IDs
                id = id_generator()
                total_ids.append(id)
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
                rows = {'id': total_ids, 'title':total_titles, 'link':total_links, 'description': total_descriptions, 'pubdate': total_pubdates, 'location': total_locations, 'timestamp': total_timestamps}
        return rows
    data = all_elements()

    #Convert data to a pandas df for further analysis
    data_dic = dict(data)
    df = pd.DataFrame.from_dict(data_dic, orient='index')
    df = df.transpose()

    #Cleaning columns
    for col in df.columns:
        if col == 'link':
            df[col] = df[col].apply(clean_link_rss)
        else:
            df[col] = df[col].apply(clean_other_rss)

    df.to_csv(PATH + '/OUTPUTS/rss_refactor.csv', index=False)

    def pipeline(df):
        df.fillna("NaN", inplace=True)
        #Slice desdriptions
        df['description'] = df['description'].str.slice(0, 2000)
        #Logging
        logging.info('Finished RSS_YMD. Results below ⬇︎')
        ## PostgreSQL
        if postgre == "MAIN":
            to_postgre(df)
        elif postgre == "TEST":
            test_postgre(df)
        
        #print the time
        elapsed_time = timeit.default_timer() - start_time
        print("\n", f"RSS_YMD is done! all in: {elapsed_time:.2f} seconds", "\n")
    pipeline(df)

rss_refactor("MAIN")


