#! python3
from selenium import webdriver
from selenium.webdriver.common.by import By
import pandas as pd
import re
import pretty_errors
import json
import timeit
from datetime import date
from utils.handy import *


#IMPORT THE PATH - YOU NEED TO EXPORT YOUR OWN PATH TO zsh/bash & SAVE IT AS 'CRAWLER_ALL'
PATH = '/Users/juanreyesgarcia/Library/CloudStorage/OneDrive-FundacionUniversidaddelasAmericasPuebla/DEVELOPER/PROJECTS/CRAWLER_ALL/'


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
        total_ids = []
        total_titles = []
        total_links = []
        total_pubdates = []
        total_locations = [] 
        total_descriptions = []
        rows = {'id': total_ids,
                'title': total_titles,
                'link': total_links,
                'description': total_descriptions,
                'pubdate': total_pubdates,
                'location': total_locations}


        def country_url(i):
            url = ""
            with open('./selenium_resources/indeed_country.json') as f:
                    data = json.load(f)
                    # Search for a specific code in the JSON document
                    for item in data:
                        if item['code'] == country:  
                            url= item['url_1'] + keyword + item["url_2"] + str(i)
            return url

        for i in range(0, pages * 10, 10):
            page = round(i/10)
            print("\n", f"Crawler deployed on Indeed {country}. Looking for \"{keyword}\" jobs in page number {page}")
            url = country_url(page)
            #Make the request
            driver.get(url)
            #Set waiting strategy
            driver.implicitly_wait(1.5)
            jobs = driver.find_elements(By.CSS_SELECTOR, '.slider_item.css-kyg8or.eu4oa1w0')    
            for job in jobs:
                ##Get the attributes...

                job_data = {}
                
                #ID
                id = id_generator(8)
                job_data["id"] = id
                #TITLES
                title_raw = job.find_element(By.CSS_SELECTOR, '[id^="jobTitle"]')
                if title_raw is not None:
                    title = title_raw.get_attribute("innerHTML")
                    job_data["title"]= title
                else:
                    job_data["title"]= "NaN"
                #LINKS
                link_raw = job.find_element(By.CSS_SELECTOR, '.jcs-JobTitle.css-jspxzf.eu4oa1w0') #THIS FINDS THE PATTERN
                if link_raw is not None:
                    link = link_raw.get_attribute("href")
                    job_data["link"]= link
                else:
                    job_data["link"]= "NaN"
                #DATES
                today = date.today()
                job_data["pubdate"] = today
                #LOCATION
                location_raw = job.find_element(By.CSS_SELECTOR, '.companyLocation')
                if location_raw is not None:
                    location = location_raw.text
                    job_data["location"]= location
                else:
                    job_data["location"]= "NaN"
                #DESCRIPTIONS
                description_raw = job.find_element(By.CSS_SELECTOR, '.jobCardShelfContainer.big6_visualChanges .heading6.tapItem-gutter.result-footer')
                if description_raw is not None:
                    description = description_raw.get_attribute("innerHTML")
                    job_data["description"]= description
                else:
                    job_data["description"]= "NaN"
            #Put it all together...
                total_ids.append(job_data["id"])
                total_titles.append(job_data["title"])
                total_links.append(job_data["link"])
                total_pubdates.append(job_data["pubdate"])
                total_locations.append(job_data["location"])
                total_descriptions.append(job_data["description"])
        return rows
    
    data = crawling()
    driver.quit()

    #Convert data to a pandas df for further analysis
    df = pd.DataFrame(data)

    #Do some cleaning
    for col in df.columns:
        if col == 'description':
            df[col] = df[col].apply(indeed_regex)

    #Save locally
    df.to_csv(PATH + '/OUTPUTS/INDEED_MX.csv', index=False)

    # SEND IT TO TO PostgreSQL
    personal_postgre(df)

    #stop the timer
    elapsed_time = timeit.default_timer() - start_time
    print("\n", f"Crawler FINISHED finding \"{keyword}\" jobs. All in {elapsed_time:.5f} seconds!!!", "\n")


if __name__ == "__main__":
    indeed(2, "MX", "data+analyst") 
    """
    1st = number of pages
    2nd = country code from json
    3rd = keyword(s) -if more than 1 use +
    
    """