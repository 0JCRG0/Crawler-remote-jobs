from selenium import webdriver
from selenium.webdriver.common.by import By
import pandas as pd
import re
import csv
import pretty_errors
import numpy as np

def WORKING_NOMADS():
    
    # Start the session
    driver = webdriver.Firefox()

    # set the number of pages you want to scrape
    num_pages = 101

    #For later (CLEAN TITLES)
    def TEXT_WASH(s):
        s = " ".join(s.split())
        s = re.sub(r'n/', '', s)
        return s
    
    # START CRAWLING
    def CRAWLING():
        total_urls = []
        total_titles = []
        total_pubDates = []
        total_locations = [] 
        total_categories = []
        rows = []
        for i in range(1, num_pages + 1):
            url = f"https://himalayas.app/jobs?page={i}"
            driver.get(url)
            # Establish Waiting Strategy
            driver.implicitly_wait(1)
            #GETTING THE PARENT...
            jobs = driver.find_elements(By.CSS_SELECTOR, '#card-group [name = "card"]')
            for job in jobs:
                #GETTING THE ELEMENTS FROM THE CHILDREN... remember to use css_selectors instead of tags
                all_urls = job.find_elements(By.CSS_SELECTOR, '.mt-4.md\:mt-0.ml-0.md\:ml-5.w-full .flex.flex-row.items-start.justify-between.mb-2.w-full .flex.flex-row.items-center a')
                all_titles = job.find_elements(By.CSS_SELECTOR, '.mt-4.md\:mt-0.ml-0.md\:ml-5.w-full .flex.flex-row.items-start.justify-between.mb-2.w-full .flex.flex-row.items-center a .text-xl.font-medium.text-gray-900')
                all_pubDates = job.find_elements(By.CSS_SELECTOR, '.mt-4.md\:mt-0.ml-0.md\:ml-5.w-full .flex.flex-row.items-start.justify-between.mb-2.w-full .hidden.md\:block.md\:whitespace-nowrap.md\:ml-2.relative div .text-base.text-gray-600')
                all_location = job.find_elements(By.CSS_SELECTOR, '.mt-4.md\:mt-0.ml-0.md\:ml-5.w-full .flex.flex-row.items-center.justify-between .flex.flex-row.items-center.flex-wrap.overflow-hidden .hidden.md\:flex.md\:flex-row.md\:items-center .badge.badge-gray.no-hover.mr-2 .flex.flex-row.items-center .badge-text.whitespace-nowrap.no-hover.text-center')
                all_category = job.find_elements(By.CSS_SELECTOR, '.mt-4.md\:mt-0.ml-0.md\:ml-5.w-full .flex.flex-row.items-center.justify-between .flex.flex-row.items-center.flex-wrap.overflow-hidden .flex div .flex.flex-row.items-center .badge-text.text-center.whitespace-nowrap')
                for url in all_urls:
                    href = url.get_attribute("href")
                    total_urls.append(href)
                for titl in all_titles:
                    title = titl.get_attribute("innerHTML")
                    curated_title = [TEXT_WASH(title)]
                    total_titles.append(curated_title)
                for dates in all_pubDates:
                    pubDate = dates.get_attribute("innerHTML") 
                    total_pubDates.append(pubDate)
                for loc in all_location:
                    location = loc.get_attribute("innerHTML") 
                    total_locations.append(location)
                for categ in all_category:
                    category = categ.get_attribute("innerHTML") 
                    total_categories.append(category)
                rows = {"title": total_titles, "link": total_urls, "pubDate": total_pubDates, "location": total_locations, "description": total_categories}
        return rows

    #Quit the driver & save the data
    data = CRAWLING()
    driver.quit()
    
    #It is easier to deal with missing data like this tbh
    data_dic = dict(data)
    df = pd.DataFrame.from_dict(data_dic, orient='index')
    df = df.transpose()

    df.to_csv("himalaya.csv", index=False)
WORKING_NOMADS()