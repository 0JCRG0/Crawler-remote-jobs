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
import json
from RSS_CRAWLERS.utils.handy import clean_link_rss, clean_other_rss, YMD_pubdate, to_postgre, adby_pubdate


def master_crawler():
    # start master timer
    master_start_time = timeit.default_timer()

    #Start rss_abdy
    def rss_abdy():
        #start timer
        start_time = timeit.default_timer()

        file = './RSS_CRAWLERS/remote-working-resources.csv'
        
        print("\n", "RSS_ABDY has started...", "\n")

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

        directory = "./OUTPUTS/"
        df.to_csv(f'{directory}yummy_soup_rss.csv', index=False)

        for col in df.columns:
            if col == 'pubdate':
                df[col] = df[col].apply(adby_pubdate)

        directory = "./OUTPUTS/"
        df.to_csv(f'{directory}test.csv', index=False)

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
            end_date = pd.to_datetime('2023-02-15')
            date_range_filter = (df['pubdate'] >= start_date) & (df['pubdate'] <= end_date)
            df = df.loc[~date_range_filter]

            #sort the values
            df = df.sort_values(by='pubdate')
            # Reduce the lenght of description... 
            df['description'] = df['description'].str.slice(0, 2000)

            # replace NaT values in the DataFrame with None -> if not postgre raises an error
            df = df.replace({np.nan: None, pd.NaT: None})

            ## PostgreSQL

            to_postgre(df)
            
            #print the time
            elapsed_time = timeit.default_timer() - start_time
            print("\n", f"RSS_ABDY is done! all in: {elapsed_time:.2f} seconds", "\n")
        pipeline(df)
    rss_abdy()

    #Start rss_ymd
    print("\n", "MOVING ON...","\n")

    def rss_YMD():
        #start timer
        start_time = timeit.default_timer()

        file = './RSS_CRAWLERS/working-resources-YMD.csv'
        
        print("\n", "RSS_YMD has started...","\n")

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

        directory = "./OUTPUTS/"
        df.to_csv(f'{directory}RSS-YMD.csv', index=False)

        for col in df.columns:
            if col == 'pubdate':
                df[col] = df[col].apply(YMD_pubdate)

        directory = "./OUTPUTS/"
        df.to_csv(f'{directory}test-RSS-YMD.csv', index=False)

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
            end_date = pd.to_datetime('2023-02-15')
            date_range_filter = (df['pubdate'] >= start_date) & (df['pubdate'] <= end_date)
            df = df.loc[~date_range_filter]

            #sort the values
            df = df.sort_values(by='pubdate')
            # Reduce the lenght of description... 
            df['description'] = df['description'].str.slice(0, 2000)

            # replace NaT values in the DataFrame with None -> if not postgre raises an error
            df = df.replace({np.nan: None, pd.NaT: None})

            ## PostgreSQL

            to_postgre(df)
            
            #print the time
            elapsed_time = timeit.default_timer() - start_time
            print("\n", f"RSS_YMD finished! all in: {elapsed_time:.2f} seconds", "\n")
        pipeline(df)
    rss_YMD()

    #Start W_Nomads
    print("\n", "MOVING ON...","\n")

    def W_NOMADS():
        #Start the timer
        start_time = timeit.default_timer()

        def API_FETCHER():

            api = "https://www.workingnomads.com/api/exposed_jobs/"
            print("\n", f"Fetching...{api}", "\n")
            response = requests.get(api)
            if response.status_code == 200:
                data = json.loads(response.text)
                #pretty_json = json.dumps(data, indent=4)
                #print(pretty_json, type(data))
                
                #Loop through the list of dict            
                all_jobs = []
                titles = []
                links = []
                pubdates = []
                locations = []
                descriptions = []
                for job in data:
                    titles.append(job["title"])
                    links.append(job["url"])
                    pubdates.append(job["pub_date"])
                    locations.append(job["location"])
                    descriptions.append(job["tags"])
                    #Put it all together...
                    all_jobs = {'title': titles, 'link':links, 'pubdate': pubdates, 'location': locations, 'description': descriptions}
                return all_jobs
            else:
                print("Error connecting to API:", response.status_code)

        data = API_FETCHER()

        #to df
        df = pd.DataFrame(data)

        directory = "./OUTPUTS/"
        df.to_csv(f"{directory}W_NOMADS.csv", index=False)

        ## PostgreSQL

        to_postgre(df)

        elapsed_time = timeit.default_timer() - start_time
        print("\n", f"W_NOMADS is done! all in: {elapsed_time:.2f} seconds", "\n")
    W_NOMADS()

    #print the time
    elapsed_time = timeit.default_timer() - master_start_time
    print("\n", f"Master Crawler is done! It finished in: {elapsed_time:.2f} seconds, not bad", "\n")
if __name__ == "__main__":
    master_crawler()