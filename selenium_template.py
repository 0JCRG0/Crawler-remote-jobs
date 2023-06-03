#!/usr/local/bin/python3

from selenium import webdriver
from selenium.webdriver.common.by import By
import pandas as pd
import re
import pretty_errors
import timeit
from dateutil.parser import parse
from datetime import date, datetime
import json
import logging
import os
from dotenv import load_dotenv
from utils.handy import *

#TODO: replace path with .env

#IMPORT THE PATH - YOU NEED TO EXPORT YOUR OWN PATH TO zsh/bash & SAVE IT AS 'CRAWLER_ALL'
PATH = '/Users/juanreyesgarcia/Library/CloudStorage/OneDrive-FundacionUniversidaddelasAmericasPuebla/DEVELOPER/PROJECTS/CRAWLER_ALL'

def selenium_crawlers(TYPE):
    #start timer
    start_time = timeit.default_timer()

    #Modify the options so it is headless - to disable just comment the next 2 lines and use the commented driver
    options = webdriver.FirefoxOptions()
    options.add_argument('-headless')
        
    # Start the session
    driver = webdriver.Firefox(options=options)

    """
    The following is specifying which JSON to load & to which table it will be sent
    """

    if TYPE == 'MAIN':
        JSON = PATH + '/selenium_resources/main_sel_crawlers.json'
        POSTGRESQL = to_postgre
        print("\n", f"Sent to PROD. Jobs will be sent to PostgreSQL's master_jobs table", "\n")
        # configure the logger
        LoggingMasterCrawler()
    elif TYPE == 'FREELANCE':
        JSON = PATH + '/selenium_resources/freelance.json'
        POSTGRESQL = freelance_postgre
        # configure the logger
        LoggingFreelanceCrawler()
        #print("\n", f"Reading {JSON}. Jobs will be sent to PostgreSQL's freelance table", "\n")
    elif TYPE == 'TEST':
        JSON = PATH + '/selenium_resources/test_selenium.json'
        POSTGRESQL = test_postgre
        print("\n", f"TESTING. Jobs will be sent to PostgreSQL's test table", "\n")
        # configure the logger
        LoggingMasterCrawler()
    else:
        print("\n", "Incorrect argument! Use 'MAIN', 'TEST' or 'FREELANCE' to run this script.", "\n")


    def elements():
        total_ids = []
        total_links = []
        total_titles = []
        total_pubdates = []
        total_locations = [] 
        total_descriptions = []
        total_timestamps = []
        rows = {"id": total_ids,
                "title": total_titles,
                "link": total_links,
                "description": total_descriptions,
                "pubdate": total_pubdates,
                "location": total_locations,
                "timestamp": total_timestamps}

        #load the json
        with open(JSON) as f:
            data = json.load(f)
            # Access the 'urls' list in the first dictionary of the 'data' list and assign it to the variable 'urls'
            urls = data[0]["urls"]
            for url_obj in urls:
                #Extract the name of the crawler
                name = url_obj['name']
                print("\n", f"{name} has started", "\n")
                # Extract the 'url' key from the current dictionary and assign it to the variable 'url_prefix'
                url_prefix = url_obj['url']
                # Extract the first dictionary from the 'elements_path' list in the current dictionary and assign it to the variable 'elements_path'
                elements_path = url_obj['elements_path'][0]
                #Each site is different to a json file can give us the flexibility we need
                ## Extract the "pages_to_crawl"
                pages = url_obj['pages_to_crawl']
                #Extract the number in which the range is going to start from
                start_point = url_obj['start_point']
                # You need to +1 because range is exclusive
                for i in range(start_point, pages + 1):
                    #The url from json is incomplete, we need to get the number of the page we are scrapping
                    ##We do that in the following line
                    url = url_prefix + str(i)
                    # get the url
                    driver.get(url)
                    print(f"Crawling... {url}")
                    # establish waiting strategy
                    driver.implicitly_wait(1)
                    # GETTING THE PARENT...
                    jobs = driver.find_elements(By.CSS_SELECTOR, elements_path["jobs_path"])
                    for job in jobs:
                        # create a new dictionary to store the data for the current job
                        job_data = {}

                        #IDs
                        id = id_generator(5)
                        job_data["id"]= id

                        #TITLES
                        title_element = job.find_element(By.CSS_SELECTOR, elements_path["title_path"])
                        if title_element is not None:
                            title = title_element.get_attribute("innerHTML")
                            job_data["title"]= title
                        else:
                            job_data["title"]= "NaN"
                            
                        #LINKS
                        link_element = job.find_element(By.CSS_SELECTOR, elements_path["link_path"])
                        if link_element is not None:
                            href = link_element.get_attribute("href")
                            job_data["link"]= href
                        else:
                            job_data["link"]= "NaN"

                        #PUBDATES - to simplify things & considering this snippet will be run daily datetime is the same day as the day this is running                       
                        today = date.today()
                        job_data["pubdate"] = today
                            
                        #LOCATIONS
                        location_element = job.find_element(By.CSS_SELECTOR, elements_path["location_path"])
                        if location_element is not None:
                            location = location_element.get_attribute("innerHTML") 
                            job_data["location"]= location
                        else:
                            job_data["location"]= "NaN"
                            
                        #Descriptions
                        description_element = job.find_element(By.CSS_SELECTOR, elements_path["description_path"])
                        if description_element is not None:
                            description = description_element.get_attribute("innerHTML") 
                            job_data["description"]= description
                        else:
                            job_data["description"]= "NaN"
                        
                        #Timestamps
                        timestamp = datetime.now()
                        job_data["timestamp"] = timestamp
                            
                        # add the data for the current job to the rows list
                        total_ids.append(job_data["id"])
                        total_links.append(job_data["link"])
                        total_titles.append(job_data["title"])
                        total_pubdates.append(job_data["pubdate"])
                        total_locations.append(job_data["location"])
                        total_timestamps.append(job_data["timestamp"])
                        total_descriptions.append(job_data["description"])
        return rows

                    
    data = elements()
    driver.quit()

    #-> DF
    df = pd.DataFrame(data)

    df.to_csv(PATH + "/OUTPUTS/pre-pipeline-Sel_All.csv", index=False)

    # count the number of duplicate rows
    num_duplicates = df.duplicated().sum()

            # print the number of duplicate rows
    print("Number of duplicate rows:", num_duplicates)

            # remove duplicate rows based on all columns
    df = df.drop_duplicates()
        
    def pipeline(df):
        
        """ CLEANING AVOIDING DEPRECATION WARNING """
        for col in df.columns:
            if col != 'pubdate':
                i = df.columns.get_loc(col)
                newvals = df.loc[:, col].astype(str).apply(cleansing_selenium_crawlers)
                df[df.columns[i]] = newvals
            
        #Save it in local machine
        df.to_csv(PATH + "/OUTPUTS/post-pipeline-Sel_All.csv", index=False)

        #Log it 
        logging.info('Finished Selenium_Crawlers. Results below ⬇︎')
        
        # SEND IT TO TO PostgreSQL    
        POSTGRESQL(df)

        #print the time
        elapsed_time = timeit.default_timer() - start_time
        print("\n")
        print(f"The selected selenium crawlers have finished! all in: {elapsed_time:.2f} seconds.", "\n")
    pipeline(df)

if __name__ == "__main__":
    selenium_crawlers('MAIN') 