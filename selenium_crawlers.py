#!/usr/local/bin/python3

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException
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
from sql.clean_loc import clean_location_rows


""" LOAD THE ENVIRONMENT VARIABLES """

load_dotenv()

PROD = os.environ.get('JSON_PROD_SEL')
TEST = os.environ.get('JSON_TEST_SEL')
SAVE_PATH = os.environ.get('SAVE_PATH_SEL')

def selenium_template(pipeline):
    print("\n", "Crawler launched on headless browser.")

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

    if pipeline == 'MAIN':
        if PROD:
            JSON = PROD
        POSTGRESQL = to_postgre
        print("\n", f"Pipeline is set to 'MAIN'. Jobs will be sent to PostgreSQL's main_jobs table", "\n")
        # configure the logger
        LoggingMasterCrawler()
    elif pipeline == 'FREELANCE':
        #TODO: Fix path
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


    def elements():
        total_links = []
        total_titles = []
        total_pubdates = []
        total_locations = [] 
        total_descriptions = []
        total_timestamps = []
        rows = {"title": total_titles,
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
                strategy = url_obj['strategy']

                for i in range(start_point, pages + 1):
                    #The url from json is incomplete, we need to get the number of the page we are scrapping
                    ##We do that in the following line
                    url = url_prefix + str(i)
                    # get the url
                    try:
                        driver.get(url)
                        print(f"Crawling {url} with {strategy} strategy")
                        # establish waiting strategy
                        driver.implicitly_wait(1)
                        """ IF LINKS ARE *NOT* IN THE SAME ELEMENT AS JOBS """
                        if strategy == "main":
                            jobs = driver.find_elements(By.CSS_SELECTOR, elements_path["jobs_path"])
                            for job in jobs:
                                # create a new dictionary to store the data for the current job
                                job_data = {}

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
                                total_links.append(job_data["link"])
                                total_titles.append(job_data["title"])
                                total_pubdates.append(job_data["pubdate"])
                                total_locations.append(job_data["location"])
                                total_timestamps.append(job_data["timestamp"])
                                total_descriptions.append(job_data["description"])
                        
                        elif strategy == "container":
                            """ IF LINKS *ARE* IN THE SAME ELEMENT AS JOBS """
                            
                            #Identify the container with all the jobs
                            container = driver.find_element(By.CSS_SELECTOR, elements_path["jobs_path"])
                            
                            #TITLES
                            title_elements = container.find_elements(By.CSS_SELECTOR, elements_path["title_path"])
                            for i in title_elements:
                                if i:
                                    title = i.get_attribute("innerHTML")
                                    total_titles.append(title)
                                else:
                                    total_titles.append("NaN")
                            
                            #LINKS
                            link_elements = container.find_elements(By.CSS_SELECTOR, elements_path["link_path"])
                            for i in link_elements:
                                if i:
                                    href = i.get_attribute("href")
                                    total_links.append(href)
                                else:
                                    total_links.append("NaN")

                            # PUBDATES
                            today = date.today()
                            total_pubdates.extend([today] * len(link_elements))
                                    
                            #LOCATIONS
                            location_elements = container.find_elements(By.CSS_SELECTOR, elements_path["location_path"])
                            for i in location_elements:
                                if i:
                                    location = i.get_attribute("innerHTML") 
                                    total_locations.append(location)
                                else:
                                    total_locations.append("NaN")
                                    
                            #Descriptions
                            description_elements = container.find_elements(By.CSS_SELECTOR, elements_path["description_path"])
                            for i in description_elements:
                                if i:
                                    description = i.get_attribute("innerHTML") 
                                    total_descriptions.append(description)
                                else:
                                    total_descriptions.append("NaN")
                                
                            #Timestamps
                            timestamp = datetime.now()
                            total_timestamps.extend([timestamp] * len(link_elements))
                            rows = {'title':total_titles, 'link':total_links, 'description': total_descriptions, 'pubdate': total_pubdates, 'location': total_locations, 'timestamp': total_timestamps}

                    except Exception as e:
                        # Handle any other exceptions
                        print("ELEMENT NOT FOUND:", str(e))
                        logging.error("ELEMENT NOT FOUND:", str(e))

        return rows 

                    
    data = elements()
    driver.quit()

    #Save it in local machine

    data_dic = dict(data) # type: ignore
    df = pd.DataFrame.from_dict(data_dic, orient='index')
    df = df.transpose()
    
    """
    if strategy == "main":
        df = pd.DataFrame(data)
    else:
        #Convert data to a pandas df for further analysis
        data_dic = dict(data) # type: ignore
        df = pd.DataFrame.from_dict(data_dic, orient='index')
        df = df.transpose()
    """

    # count the number of duplicate rows
    num_duplicates = df.duplicated().sum()

            # print the number of duplicate rows
    print("Number of duplicate rows:", num_duplicates)

            # remove duplicate rows based on all columns
    df = df.drop_duplicates()
        
    def clean_postgre(df):
        
        """ CLEANING AVOIDING DEPRECATION WARNING """
        for col in df.columns:
            if col != 'pubdate':
                i = df.columns.get_loc(col)
                newvals = df.loc[:, col].astype(str).apply(cleansing_selenium_crawlers)
                df[df.columns[i]] = newvals
            elif col == 'location':
                i = df.columns.get_loc(col)
                newvals = df.loc[:, col].astype(str).apply(clean_location_rows)
                df[df.columns[i]] = newvals
            
        #Save it in local machine
        df.to_csv(SAVE_PATH, index=False)

        #Log it 
        logging.info('Finished Selenium_Crawlers. Results below ⬇︎')
        
        # SEND IT TO TO PostgreSQL    
        POSTGRESQL(df)

        #print the time
        elapsed_time = timeit.default_timer() - start_time
        print("\n")
        print(f"The selected selenium crawlers have finished! all in: {elapsed_time:.2f} seconds.", "\n")
    clean_postgre(df)

if __name__ == "__main__":
    selenium_template('TEST') 