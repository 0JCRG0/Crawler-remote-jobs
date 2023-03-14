#! python3
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
import pandas as pd
import re
import csv
import pretty_errors
import psycopg2
import numpy as np
from dateutil.relativedelta import relativedelta
from dateutil.parser import parse
from datetime import datetime, timedelta


# 1 TODO: FIX DATETIME (see below)
# 2 TODO: FINISH GETTING THE ELEMENTS AND THEIR ATTRIBUTES
# 3 TODO: FIX PUBDATE - it only gets "posted"

"https://mx.indeed.com/jobs?q=PYTHON&sc=0kf%3Aattr%28DSQF7%29%3B&sort=date&fromage=7&filter=0&start=80&pp=gQB4AAABhth9DdoAAAAB-_3ALQAJAQA_oyaWY9L6AAA&vjk=a7a2ee2d2741c607"
"https://mx.indeed.com/jobs?q=PYTHON&sc=0kf%3Aattr%28DSQF7%29%3B&sort=date&fromage=7&filter=0&start=70&pp=gQBpAAABhth9DdoAAAAB-_3ALQAYAQACCYx8rKQbcGPqdw-3E24IbS_nb3zaAAA&vjk=3954cde2ef9483b9"
'https://indeed.com/jobs?q=DATA+ANALYST&sc=0kf%3Aattr%28DSQF7%29%3B&sort=date&fromage=7&filter=0&start=0'


def indeed():
    # Start the session
    driver = webdriver.Firefox()

    # set the number of pages you want to scrape
    num_pages = 1

    # cleansing function
    def bye_regex(s):
        # Remove leading/trailing white space
        s = s.strip()
        # Replace multiple spaces with a single space
        s = re.sub(r'\s+', ' ', s)
        # Remove newline characters
        s = re.sub(r'\n', '', s)
        # Replace regex for í
        s = re.sub(r'√≠', 'í', s)
        # Replace word
        s = re.sub(r'Posted', '', s)

        return s
        
    # START CRAWLING
    def crawling(keyword):
        print("\n", f"Crawler deployed... ", "\n")
        total_urls = []
        total_titles = []
        total_pubdates = []
        total_locations = [] 
        total_descriptions = []
        rows = []
        for i in range(0, num_pages * 10, 10):
            url = f"https://mx.indeed.com/jobs?q={keyword}&sc=0kf%3Aattr%28DSQF7%29%3B&sort=date&fromage=7&filter=0&start={i}"
            #Make the request
            driver.get(url)
            print(f"Crawling... {url}")
            #Set waiting strategy
            driver.implicitly_wait(1.5)
            #Get the elements for all the jobs
            titles = driver.find_elements(By.CSS_SELECTOR, '[id^="jobTitle"]')
            links = driver.find_elements(By.CSS_SELECTOR, '[id^="job_"]') #THIS FINDS THE PATTERN
            #pubdates = driver.find_elements(By.CSS_SELECTOR, '.date .visually-hidden')
            pubdates = driver.find_elements(By.XPATH, "//*[contains(text(), 'Publicado hace')]")
            locations = driver.find_elements(By.CSS_SELECTOR, '.companyLocation')
            #descriptions = driver.find_elements(By.CSS_SELECTOR, '.job-snippet ul li')
            #Get the attributes
            for t in titles:
                title = t.get_attribute("innerHTML")
                total_titles.append(title)
            for li in links:
                href = li.get_attribute("href")
                total_urls.append(href)
            for p in pubdates:
                dirty_pubdate = p.text
                pubdate = bye_regex(dirty_pubdate)
                total_pubdates.append(pubdate)
            for l in locations:
                location = l.text
                total_locations.append(location)
            #for d in descriptions:
                #description = d.text
                #total_descriptions.append(description)
            rows = {'titles': total_titles, 'links': total_urls, 'pubdate': total_pubdates, 'location': total_locations, 'description': ""}
        return rows
    
    data = crawling("DATA+ANALYST") #THIS IS YOUR SEARCH BOX - use "+" if more than 1 word

    
    driver.close()

    print("\n", "Converting data to a pandas df...", "\n")
    data_dic = dict(data)
    df = pd.DataFrame.from_dict(data_dic, orient='index')
    df = df.transpose()
    pd.set_option('display.max_colwidth', 150)
    pd.set_option("display.max_rows", None)
    print(df)
    print("\n", f"Crawler successfully found {len(df)} jobs...", "\n")

    directory = "./OUTPUTS/"
    df.to_csv(f'{directory}INDEED_MX.csv', index=False)

if __name__ == "__main__":
    indeed()
    #print(f"Found {len(urls)} job listings.")


# 1 TODO: FIX DATETIME

"""
# get today's datetime
today = datetime.now()

# example string
str_date = "Publicado hace 1 días"

# extract the number of days from the string
num_days = int(str_date.split()[2])

# calculate yesterday's datetime
yesterday = today - timedelta(days=num_days)

# example if statement
if str_date == "Publicado hace 1 días":
    print(f"Yesterday was {yesterday}")
"""