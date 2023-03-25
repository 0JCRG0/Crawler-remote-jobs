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
import timeit
from utils.handy import clean_link_rss, clean_other_rss, send_postgre

def all_rss():
    #start timer
    start_time = timeit.default_timer()

    file = './RSS_CRAWLERS/remote-working-resources.csv'
    
    print("\n", f"Reading {file}... ", "\n")

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

    print("\n", f"Making soup from {len(all_soups)} established connections.", "\n")

    print("\n", f"Soup is ready.", "\n")

    print("\n", "CRAWLER IS LOOKING FOR DESIRED ELEMENTS IN THE SOUP", "\n")


    def all_elements():
        rows = []
        for soup in all_soups:
            for item in soup.find_all('item'):
                title = str(item.title.get_text(strip=True))
                link = str(item.link.get_text(strip=True))
                pubDate = item.find('pubDate')
                if pubDate is not None:
                    pubDate = str(item.pubDate.get_text(strip=True))
                location = item.find('location')
                if location is not None:
                    location = str(item.location.get_text(strip=True))
                description = item.find('description')
                if description is not None:
                    description = str(item.description.get_text(strip=True))
                row = {'title':title, 'link':link, 'pubdate': pubDate, 'location': location, 'description': description}
                rows.append(row)
        return rows
    jobs = all_elements()

    print("\n", f"Crawler successfully found {len(jobs)} jobs...", "\n")

    print("\n", "Preprocessing the obtained jobs...", "\n")

    def clean_df():
        df = pd.DataFrame()
        curated_rows = []
        for dic in jobs:
            row = {}
            for key,val in dic.items():
                if key == 'link':
                    row[key] = clean_link_rss(val)
                elif val is None:
                    continue
                else:
                    row[key] = clean_other_rss(val)
            curated_rows.append(row)
            df = pd.concat([pd.DataFrame(curated_rows)])
            directory = "./OUTPUTS/"
            df.to_csv(f'{directory}yummy_soup_rss.csv', index=False)
        #return df
    clean_df()

    print("\n", "Jobs have been saved into local machine as a CSV.", "\n")

    def pipeline():
        print("\n", "Jobs going through last pipeline...", "\n")

        df = pd.read_csv('./OUTPUTS/yummy_soup_rss.csv')
        # Fill missing values with "NaN"
        df.fillna("NaN", inplace=True)
        # convert the pubdate column to a datetime object
        for i in range(len(df.columns)):
            if df.columns[i] == 'pubdate':
                df[df.columns[i]] = pd.to_datetime(df[df.columns[i]], errors="coerce", format="%a %d %b %Y", exact=False)
        
        print("\n", "Jobs' pubdate converted into a datetime object", "\n")

        #Filter rows by a date range (this reduces the number of rows... duh)
        start_date = pd.to_datetime('2016-01-01')
        end_date = pd.to_datetime('2023-02-15')
        date_range_filter = (df['pubdate'] >= start_date) & (df['pubdate'] <= end_date)
        df = df.loc[~date_range_filter]

        print("\n", f"Jobs filtered from {str(start_date)} to {str(end_date)}", "\n")

        #sort the values
        df = df.sort_values(by='pubdate')
        # Reduce the lenght of description... 
        df['description'] = df['description'].str.slice(0, 1000)

        # replace NaT values in the DataFrame with None
        df = df.replace({np.nan: None, pd.NaT: None})

        ## PostgreSQL

        print("\n", f"Parsing {len(df)} filtered & preprocessed jobs to PostgreSQL...", "\n")

        send_postgre(df)
                
        #print the time
        elapsed_time = timeit.default_timer() - start_time
        print("\n", f"Jobs were found, preprocessed, cleaned and sent to PostgreSQL in: {elapsed_time:.2f} seconds", "\n")
    pipeline()

if __name__ == "__main__":
    all_rss()

