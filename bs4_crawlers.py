import csv
import requests
import pandas as pd
import bs4 
from urllib.error import HTTPError
import html
import re
import psycopg2
import numpy as np
import pretty_errors
import datetime
import timeit
import random
import time

def bs4_html():
    start_time = timeit.default_timer()

    file = './bs4_html_resources/bs4_html.csv'
    
    #print("\n", f"Reading {file}... ", "\n")
    print("\n", "bs4 html crawler starting")

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
                        soup = bs4.BeautifulSoup(res.text, 'html.parser')
                        soup_list.append(soup)
                    except HTTPError as e:
                        if e.code == 403:
                            print(f"An error occurred: {e}. Skipping URL {rss}")
                            rss = None
                        else:
                            raise
        return soup_list
    all_soups = soups()

    def elements():
        rows = []
        total_pubdates = []
        total_titles = []
        total_links = []
        total_locations = []
        total_descriptions = []
        for soup in all_soups:
            delay = random.uniform(0, 2)
            time.sleep(delay)
            #all_jobs = soup.find_all(class_= "FxQpvm yKsady")
            #raw_titles = soup.find_all(class_="FxQpvm yKsady", class_= "jkit__ojhiH jkit__a66IR hyperlink_appearance_undefined jkit__osRmE _2dWEc6")
            raw_titles = soup.find_all(class_=["infinite-scroll-component _3_Ra0G _serpContentBlock", "_2xhmyP", "FxQpvm yKsady", "_3sAr4j", "_15V35X", "jkit__ojhiH jkit__a66IR hyperlink_appearance_undefined jkit__osRmE _2dWEc6"])
            #"FxQpvm yKsady", "_3sAr4j", "_15V35X", "jkit__ojhiH jkit__a66IR hyperlink_appearance_undefined jkit__osRmE _2dWEc6", "(text)"])
            print(len(raw_titles))
            #class_="FxQpvm yKsady"
            #print(raw_titles)
            for i in raw_titles:
                if i is not None:
                    #title_text = i.text
                    
                    title_text = i.string

                    total_titles.append(title_text)
                else:
                    total_titles.append("NaN")
        return total_titles
    data = elements()
    t = len(data)

    print(t)
    print(data)

    with open('example.txt', 'w') as f:
        # Write some text to the file
        #f.write(str(all_soups))
        f.write(str(data))

if __name__ == '__main__':
    bs4_html()