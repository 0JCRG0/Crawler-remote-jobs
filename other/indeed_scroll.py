#! python3
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.actions.wheel_input import ScrollOrigin
import random
import time
import pandas as pd
import re
import pretty_errors
import csv
import psycopg2
import numpy as np
from dateutil.relativedelta import relativedelta
from dateutil.parser import parse
from datetime import datetime, timedelta, date
import json
import timeit
from utils.handy import bye_regex, indeed_regex


def indeed():
    #start timer
    start_time = timeit.default_timer()

    # Start the session
    driver = webdriver.Firefox()

    # set the number of pages you want to scrape
    num_pages = 1
    
    # START CRAWLING
    def crawling(country, keyword):
        print("\n", f"Crawler deployed... ", "\n")
        total_urls = []
        total_titles = []
        total_pubdates = []
        total_locations = [] 
        total_descriptions = []
        rows = []

        def country_url():
            url = ""
            with open('./selenium_resources/indeed_country.json') as f:
                    data = json.load(f)
                    # Search for a specific code in the JSON document
                    for item in data:
                        if item['code'] == country:  
                            url= item['url_1'] + keyword + item["url_2"] + str(i)
                            print(url)
            return url

        for i in range(0, num_pages * 10, 10):
            #url = f"https://mx.indeed.com/jobs?q={keyword}&sc=0kf%3Aattr%28DSQF7%29%3B&sort=date&fromage=7&filter=0&start={i}"
            url = country_url()
            #Make the request
            driver.get(url)
            print(f"Crawling... {url}")
            #Set waiting strategy
            driver.implicitly_wait(1.5)
            
            #Find all the job boxes to click on
            jobs = driver.find_elements(By.CSS_SELECTOR, '#mosaic-jobResults .jobsearch-ResultsList.css-0 [class*="cardOutline"][class*="tapItem"][class*="fs-unmask"][class*="result"][class*="job_"][class*="resultWithShelf"][class*="sponTapItem"][class*="desktop"][class*="css-kyg8or"][class*="eu4oa1w0"]')
            #Lil loop through the elements
            for job in jobs:
                """
                Indeed's layout is quite tricky. The complete descriptions of a given job is
                found on a right panel that is displayed when the respective job box is clicked
                
                So...
                
                1. This loop clicks on each job box and then performs the scraping on its respective panel
                2. Given that the job needs to be in the viewport to be scraped we scroll down *from* 
                the previous job so it pivots to the next job.
                3. We use the random module to avoid CAPTCHA

                For obvious reasons it is a bit slow tho

                THIS WORKS AS A LIL HACK BUT IT HAS SO MANY LIMITATIONS THAT COULD BE SOLVED BUT I SIMPLY DO 
                NOT HAVE THE TIME.

                """
                #get the height of every job to scroll depending on its size...
                height_ = job.size['height']
                height = int(height_ * 2.15)
                #Scroll to 1st job
                scroll_origin = ScrollOrigin.from_element(job)
                #Scroll from 1st job to next one... and to next one... (pause is extremely important)
                ActionChains(driver).scroll_from_origin(scroll_origin, 0, height).pause(.5).click(job).perform()
                #Wait for the description on the right panel
                description_element = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, '#jobsearch-ViewjobPaneWrapper #jobDescriptionText')))
                #Get the text
                dirty_description = description_element.get_attribute("innerHTML")
                description = indeed_regex(dirty_description)
                total_descriptions.append(description)
                # Pause for a random amount of time between 1 and 3 seconds to avoid CAPTCHA :P
                delay = random.uniform(0, 3)
                time.sleep(delay)

            #NOW THAT THE DIFFICULT PART IS OVER WE CAN EASILY GET THE REST OF THE ELEMENTS...

            #These two lines find a pattern of a given id
            titles = driver.find_elements(By.CSS_SELECTOR, '[id^="jobTitle"]')
            links = driver.find_elements(By.CSS_SELECTOR, '[id^="job_"]') #THIS FINDS THE PATTERN
            #I had to find this pattern bcos if not it would only yield the word "Posted"
            #pubdates = driver.find_elements(By.XPATH, "//*[contains(text(), 'Publicado hace')]")
            locations = driver.find_elements(By.CSS_SELECTOR, '.companyLocation')
            #Get the attributes
            for i in titles:
                title = i.get_attribute("innerHTML")
                total_titles.append(title)
            for i in links:
                href = i.get_attribute("href")
                total_urls.append(href)
            for i in range(len(titles)):
                today = date.today()
                total_pubdates.append(today)
            for i in locations:
                location = i.text
                total_locations.append(location)
            #Put it all together...
            rows = {'titles': total_titles, 'links': total_urls, 'pubdate': total_pubdates, 'location': total_locations, 'description': total_descriptions}
        return rows
    
    data = crawling("MX", "DATA+ANALYST") #THIS IS YOUR SEARCH BOX - use "+" if more than 1 word

    
    driver.close()

    #Convert data to a pandas df for further analysis
    print("\n", "Converting data to a pandas df...", "\n")
    data_dic = dict(data)
    df = pd.DataFrame.from_dict(data_dic, orient='index')
    df = df.transpose()
    pd.set_option('display.max_colwidth', 150)
    pd.set_option("display.max_rows", None)
    print(df)

    print("\n", "Saving jobs in local machine...", "\n")
    directory = "./OUTPUTS/"
    df.to_csv(f'{directory}indeed_scroll.csv', index=False)

#stop the timer
    elapsed_time = timeit.default_timer() - start_time
    print("\n", f"Crawler successfully found {len(df)} jobs in {elapsed_time:.5f} seconds", "\n")


if __name__ == "__main__":
    indeed()

    