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
import datetime
import timeit
import os
import logging
from datetime import datetime
from utils.handy import *
import os
from dotenv import load_dotenv

""" LOAD ALL THE ENV VARIABLES"""

# API
#load_dotenv('.env')
#PATH = os.getenv('PATH', 'DEFAULT')

"""
rss_abdy crawls 31 sites whereas rss_ymd only crawls 3 sites. The difference is the
pubdate format, which I cannot find a way to unify :(

cut_off = one day prior the oldest job 

E.g., If cut_off = '2023-03-20' then the oldest job will be from 2023-03-21
"""


#EXPORT THE PATH - YOU NEED TO EXPORT YOUR OWN PATH & SAVE IT AS 'CRAWLER_ALL'
PATH = '/Users/juanreyesgarcia/Library/CloudStorage/OneDrive-FundacionUniversidaddelasAmericasPuebla/DEVELOPER/PROJECTS/CRAWLER_ALL'


def rss_ymd(cut_off, postgre):
    #Import Logging
    LoggingMasterCrawler()
    #start timer

    start_time = timeit.default_timer()

    file = PATH + '/rss_resources/working-resources-YMD.csv'
    #file = PATH + '/rss_resources/remote-working-resources.csv'
    #print("\n", f"Reading {file}... ", "\n")
    print("\n", "RSS_YMD starting now.")

    def soups():
        soup_list = []
        with open(file, newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                if 'Feed URL' in row and row['Feed URL']:
                    rss = row['Feed URL']
                    try:
                        res = requests.get(rss)
                        if res.status_code != 200:
                            print(f"Received non-200 response ({res.status_code}) for URL {rss}. Skipping...")
                            continue
                        res.raise_for_status()
                        soup = bs4.BeautifulSoup(res.text, 'lxml-xml')
                        soup_list.append(soup)
                    except HTTPError as e:
                        if e.code == 403:
                            print(f"An error occurred: {e}. Skipping URL {rss}")
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
                id = id_generator(5)
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
                pubDate_tag = item.find('pubDate') or item.find('dc:date')
                if pubDate_tag is not None:
                    pubDate_text = pubDate_tag.get_text(strip=True)
                    total_pubdates.append(pubDate_text)
                else:
                    total_pubdates.append('NaN')
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

    df.to_csv(PATH + '/OUTPUTS/RSS-YMD.csv', index=False)

    for col in df.columns:
        if col == 'pubdate':
            df[col] = df[col].apply(YMD_pubdate)

    df.to_csv(PATH + '/OUTPUTS/test-RSS-YMD.csv', index=False)

    def pipeline(df):
        df.fillna("NaN", inplace=True)
        # convert the pubdate column to a datetime object
        for i in range(len(df.columns)):
            if df.columns[i] == 'pubdate':
                df[df.columns[i]] = pd.to_datetime(df[df.columns[i]], errors="coerce", infer_datetime_format=True, exact=False)

        #Filter rows by a date range (this reduces the number of rows... duh)
        start_date = pd.to_datetime('2016-01-01')
        end_date = pd.to_datetime(cut_off)
        date_range_filter = (df['pubdate'] >= start_date) & (df['pubdate'] <= end_date)
        df = df.loc[~date_range_filter]

        #print("\n", f"Jobs filtered from {str(start_date)} to {str(end_date)}", "\n")

        #sort the values
        df = df.sort_values(by='pubdate')
        # Reduce the lenght of description... 
        df['description'] = df['description'].str.slice(0, 2000)

        # replace NaT values in the DataFrame with None -> if not postgre raises an error
        df = df.replace({np.nan: None, pd.NaT: None})
        
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


"""




RSS_ABDY below





"""


def rss_abdy(cut_off, postgre):
    #Import Logging
    LoggingMasterCrawler()
    #start timer
    start_time = timeit.default_timer()

    file = PATH + '/rss_resources/remote-working-resources.csv'
    
    print("\n", "RSS_ABDY starting now.")

    def soups():
        soup_list = []
        with open(file, newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                if 'Feed URL' in row and row['Feed URL']:
                    rss = row['Feed URL']
                    try:
                        res = requests.get(rss)
                        if res.status_code != 200:
                            print(f"Received non-200 response ({res.status_code}) for URL {rss}. Skipping...")
                            continue
                        res.raise_for_status()
                        soup = bs4.BeautifulSoup(res.text, 'lxml-xml')
                        soup_list.append(soup)
                    except HTTPError as e:
                        if e.code == 403:
                            print(f"An error occurred: {e}. Skipping URL {rss}")
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
        total_ids=[]
        total_timestamps=[]
        for soup in all_soups:
            for item in soup.find_all('item'):
                #IDs
                id = id_generator(5)
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
                pubDate_tag = item.find('pubDate')
                if pubDate_tag is not None:
                    pubDate_text = pubDate_tag.get_text(strip=True)
                    total_pubdates.append(pubDate_text)
                else:
                    total_pubdates.append('NaN')
                # Get the locations & append it to its list
                location_tag = item.find('location') or item.find('region')
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

    print("Done crawling")
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

    df.to_csv(PATH + '/OUTPUTS/yummy_soup_rss.csv', index=False)

    for col in df.columns:
        if col == 'pubdate':
            df[col] = df[col].apply(adby_pubdate)

    df.to_csv(PATH + '/OUTPUTS/test.csv', index=False)

    print("pipeline started")
    def pipeline(df):

        #df = pd.read_csv('./OUTPUTS/yummy_soup_rss.csv')
        # Fill missing values with "NaN"
        df.fillna("NaN", inplace=True)
        # convert the pubdate column to a datetime object
        for i in range(len(df.columns)):
            if df.columns[i] == 'pubdate':
                df[df.columns[i]] = pd.to_datetime(df[df.columns[i]], errors="coerce", infer_datetime_format=True, exact=False)
                #format="%a %d %b %Y"

        #Filter rows by a date range (this reduces the number of rows... duh)

        start_date = pd.to_datetime('2016-01-01')
        end_date = pd.to_datetime(cut_off)
        date_range_filter = (df['pubdate'] >= start_date) & (df['pubdate'] <= end_date)
        df = df.loc[~date_range_filter]

        #print("\n", f"Jobs filtered from {str(start_date)} to {str(end_date)}", "\n")

        #sort the values
        df = df.sort_values(by='pubdate')
        # Reduce the lenght of description... 
        df['description'] = df['description'].str.slice(0, 2000)

        # replace NaT values in the DataFrame with None -> if not postgre raises an error
        df = df.replace({np.nan: None, pd.NaT: None})

        #Log it
        logging.info('Finished RSS_ABDY. Results below ⬇︎')

        print("postgre")
        ## PostgreSQL
        if postgre == "MAIN":
            debug_postgre(df)
        elif postgre == "TEST":
            test_postgre(df)
        
        #print the time
        elapsed_time = timeit.default_timer() - start_time
        print("\n", f"RSS_ABDY is done! all in: {elapsed_time:.2f} seconds", "\n")
    pipeline(df)

"""





FREELANCER CRAWLER






"""

def rss_freelance(cut_off):
    #SET UP LOGGING
    LoggingFreelanceCrawler()
    
    #start timer
    start_time = timeit.default_timer()

    file = PATH + '/rss_resources/freelance.csv'
    
    #print("\n", f"Reading {file}... ", "\n")
    print("\n", "RSS_FREELANCE starting now.")

    def soups():
        soup_list = []
        with open(file, newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                if 'Feed URL' in row and row['Feed URL']:
                    rss = row['Feed URL']
                    try:
                        res = requests.get(rss)
                        if res.status_code != 200:
                            print(f"Received non-200 response ({res.status_code}) for URL {rss}. Skipping...")
                            continue
                        res.raise_for_status()
                        soup = bs4.BeautifulSoup(res.text, 'lxml-xml')
                        soup_list.append(soup)
                    except HTTPError as e:
                        if e.code == 403:
                            print(f"An error occurred: {e}. Skipping URL {rss}")
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
        for soup in all_soups:
            for item in soup.find_all('item'):
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
                pubDate_tag = item.find('pubDate')
                if pubDate_tag is not None:
                    pubDate_text = pubDate_tag.get_text(strip=True)
                    total_pubdates.append(pubDate_text)
                else:
                    total_pubdates.append('NaN')
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
                rows = {'title':total_titles, 'link':total_links, 'pubdate': total_pubdates, 'location': total_locations, 'description': total_descriptions}
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

    df.to_csv(PATH + '/OUTPUTS/FreelanceRSS.csv', index=False)

    for col in df.columns:
        if col == 'pubdate':
            df[col] = df[col].apply(adby_pubdate)

    df.to_csv(PATH + '/OUTPUTS/test-RSS-YMD.csv', index=False)

    def pipeline(df):
        df.fillna("NaN", inplace=True)
        # convert the pubdate column to a datetime object
        for i in range(len(df.columns)):
            if df.columns[i] == 'pubdate':
                df[df.columns[i]] = pd.to_datetime(df[df.columns[i]], errors="coerce", infer_datetime_format=True, exact=False)

        #Filter rows by a date range (this reduces the number of rows... duh)
        start_date = pd.to_datetime('2016-01-01')
        end_date = pd.to_datetime(cut_off)
        date_range_filter = (df['pubdate'] >= start_date) & (df['pubdate'] <= end_date)
        df = df.loc[~date_range_filter]

        #print("\n", f"Jobs filtered from {str(start_date)} to {str(end_date)}", "\n")

        #sort the values
        df = df.sort_values(by='pubdate')
        # Reduce the lenght of description... 
        df['description'] = df['description'].str.slice(0, 2000)

        # replace NaT values in the DataFrame with None -> if not postgre raises an error
        df = df.replace({np.nan: None, pd.NaT: None})

        #Log it
        logging.info('Finished RSS_FREELANCE. Results below ⬇︎')
        
        ## PostgreSQL
        freelance_postgre(df)
        
        #print the time
        elapsed_time = timeit.default_timer() - start_time
        print("\n", f"RSS_FREELANCE is done! all in: {elapsed_time:.2f} seconds", "\n")
    pipeline(df)

if __name__ == "__main__":
    rss_ymd('2023-04-02', "MAIN") #cut_off -> this means that the oldest job will be the day after
    rss_abdy('2023-03-29', "MAIN") #same as above
    #rss_freelance('2023-03-20')