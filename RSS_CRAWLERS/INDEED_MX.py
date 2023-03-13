#! python3
from selenium import webdriver
from selenium.webdriver.common.by import By
import pandas as pd
import re
import csv
import pretty_errors
import psycopg2
import numpy as np
from dateutil.relativedelta import relativedelta
from dateutil.parser import parse


# 1 TODO: FIX DATETIME (see below)
# 2 TODO: FINISH GETTING THE ELEMENTS AND THEIR ATTRIBUTES
# 3 TODO: FIX PUBDATE - it only gets "posted"

def indeed():
    # Start the session
    driver = webdriver.Firefox()

    # set the number of pages you want to scrape
    num_pages = 5
        
    # START CRAWLING
    def crawling():
        print("\n", f"Crawler deployed... ", "\n")
        total_urls = []
        total_titles = []
        total_pubdates = []
        total_locations = [] 
        total_categories = []
        rows = []
        for i in range(0, num_pages * 10, 10):
            url = f"https://mx.indeed.com/jobs?q=PYTHON&sc=0kf%3Aattr%28DSQF7%29%3B&sort=date&fromage=7&start={i}"
            #Make the request
            driver.get(url)
            print(f"Crawling... {url}")
            #Set waiting strategy
            driver.implicitly_wait(.5)
            #Get the elements for all the jobs
            titles = driver.find_elements(By.CSS_SELECTOR, '[id^="jobTitle"]')
            links = driver.find_elements(By.CSS_SELECTOR, '[id^="job_"]') #THIS FINDS THE PATTERN
            pubdates = driver.find_elements(By.CSS_SELECTOR, '.date .visually-hidden')
            #Get the attributes
            for t in titles:
                title = t.get_attribute("innerHTML")
                total_titles.append(title)
            for l in links:
                href = l.get_attribute("href")
                total_urls.append(href)
            for p in pubdates:
                pubdate = p.get_attribute("innerHTML")
                total_pubdates.append(pubdate)
            rows = {'titles': total_titles, 'links': total_urls, 'pubdate': total_pubdates, 'location': '', 'description': ''}
        return rows
    
    urls = crawling()
    print(urls)

    driver.close()
    return urls


if __name__ == "__main__":
    urls = indeed()
    print(f"Found {len(urls)} job listings.")


# 1 TODO: FIX DATETIME
"""
from datetime import datetime, timedelta

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
