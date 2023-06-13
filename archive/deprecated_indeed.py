#! python3
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException
import pandas as pd
import re
import pretty_errors
import json
import timeit
from datetime import date, datetime
from dotenv import load_dotenv
import os
import sys

""" LOAD THE ENVIRONMENT VARIABLES """

load_dotenv()

RESOURCES = os.environ.get('JSON_COUNTRY_CODE', "")
SAVE_PATH = os.environ.get('SAVE_PATH_INDEED', "")
UTILS_PATH = os.environ.get('SYS_PATH_APPEND', "")

""" APPEND UTILS' PATH """
sys.path.append(UTILS_PATH)
from handy import *



def indeed(pages, country, keyword):
    #welcome message
    #start timer
    start_time = timeit.default_timer()

    #Modify the options so it is headless - to disable just comment the next 2 lines and use the commented driver
    options = webdriver.FirefoxOptions()
    options.add_argument('-headless')
        
    # Start the session
    #driver = webdriver.Firefox()
    driver = webdriver.Firefox(options=options)
    
    # START CRAWLING
    def crawling():
        total_titles = []
        total_links = []
        total_pubdates = []
        total_locations = [] 
        total_descriptions = []
        total_timestamps = []
        rows = {'title': total_titles,
                'link': total_links,
                'description': total_descriptions,
                'pubdate': total_pubdates,
                'location': total_locations,
                "timestamp": total_timestamps}


        def country_url(page):
            url = ""
            with open(RESOURCES) as f:
                    data = json.load(f)
                    # Search for a specific code in the JSON document
                    for item in data:
                        if item['code'] == country:  
                            url= item['url_1'] + keyword + item["url_2"] + str(page)
            return url

        for i in range(0, pages * 10, 10):
            page = round(i/10)
            print("\n", f"Crawler deployed on Indeed {country}. Looking for \"{keyword}\" jobs in page number {page}")
            url = country_url(page)
            #Make the request
            try:
                driver.get(url)
                print(url)
                #Set waiting strategy
                driver.implicitly_wait(1.5)
                jobs = driver.find_elements(By.CSS_SELECTOR, '.slider_item.css-kyg8or.eu4oa1w0')    
                for job in jobs:
                    ##Get the attributes...

                    job_data = {}
                    
                    title_raw = job.find_element(By.CSS_SELECTOR, '[id^="jobTitle"]')
                    job_data["title"] = title_raw.get_attribute("innerHTML") if title_raw else "NaN"
                    
                    #LINKS
                    link_raw = job.find_element(By.CSS_SELECTOR, '.jcs-JobTitle.css-jspxzf.eu4oa1w0')
                    job_data["link"] = link_raw.get_attribute("href") if link_raw else "NaN"

                    #DATES
                    today = date.today()
                    job_data["pubdate"] = today
                    #LOCATION
                    
                    location_raw = job.find_element(By.CSS_SELECTOR, '.companyLocation')
                    job_data["location"] = location_raw.text if location_raw else "NaN"

                    #DESCRIPTIONS
                    description_raw = job.find_element(By.CSS_SELECTOR, '.jobCardShelfContainer.big6_visualChanges .heading6.tapItem-gutter.result-footer')
                    job_data["description"] = description_raw.get_attribute("innerHTML") if description_raw else "NaN"

                    #TIMESTAMP
                    timestamp = datetime.now()
                    job_data["timestamp"] = timestamp

                    #Put it all together...
                    total_titles.append(job_data["title"])
                    total_links.append(job_data["link"])
                    total_pubdates.append(job_data["pubdate"])
                    total_locations.append(job_data["location"])
                    total_descriptions.append(job_data["description"])
                    total_timestamps.append(job_data["timestamp"])
            except NoSuchElementException as e:
                # Handle the specific exception
                print("Element not found:", e)
                pass
            except Exception as e:
                print("\n", f"WARNING! ELEMENTS NOT FOUND: {e}", "\n")
                pass
        return rows
    
    data = crawling()
    driver.quit()

    #Convert data to a pandas df for further analysis
    df = pd.DataFrame(data)

    df.to_csv(SAVE_PATH, index=False)

    #Do some cleaning
    for col in df.columns:
        if col == 'description':
            df[col] = df[col].apply(cleansing_selenium_crawlers)
            #df[col] = ["{MX}".format(i) for i in df[col]]
        elif col == 'location':
            df[col] = df[col] + " MX"


    #Save locally
    

    # SEND IT TO TO PostgreSQL
    test_postgre(df)

    #stop the timer
    elapsed_time = timeit.default_timer() - start_time
    print("\n", f"Crawler FINISHED finding \"{keyword}\" jobs. All in {elapsed_time:.5f} seconds!!!", "\n")


if __name__ == "__main__":
    indeed(1, "MX", "python") 
    """
    1st = number of pages
    2nd = country code from json
    3rd = keyword(s) -if more than 1 use +
    
    """