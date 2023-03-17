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
from datetime import datetime, timedelta
import json



"https://mx.indeed.com/jobs?q=PYTHON&sc=0kf%3Aattr%28DSQF7%29%3B&sort=date&fromage=7&filter=0&start=80&pp=gQB4AAABhth9DdoAAAAB-_3ALQAJAQA_oyaWY9L6AAA&vjk=a7a2ee2d2741c607"
'https://indeed.com/jobs?q=DATA+ANALYST&sc=0kf%3Aattr%28DSQF7%29%3B&sort=date&fromage=7&filter=0&start=0'
"https://indeed.com/jobs?q=PYTHON&sc=0kf%3Aattr%28DSQF7%29%3B&sort=date&fromage=7&filter=0&start=0"
"https://indeed.com/jobs?q=DATA+ANALYST&l=Remote&sc=0kf%3Aattr%28DSQF7%29%3B&&rbl=Remote&jlid=aaa2b906602aa8f5&sort=date&fromage=7&start=10"
"https://mx.indeed.com/jobs?q=PYTHON&sc=0kf%3Aattr%28DSQF7%29%3B&sort=date&fromage=7&filter=0&start=70&pp=gQBpAAABhth9DdoAAAAB-_3ALQAYAQACCYx8rKQbcGPqdw-3E24IbS_nb3zaAAA&vjk=3954cde2ef9483b9"

"https://www.indeed.com/jobs?q=DATA+ANALYST&sc=0kf%3Aattr%28DSQF7%29%3B&rbl=Remote&jlid=aaa2b906602aa8f5&fromage=7&start=10"
def indeed():
    # Start the session
    driver = webdriver.Firefox()

    # set the number of pages you want to scrape
    num_pages = 2

    # Handy cleansing function
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
        # Remove HTML tags
        s = re.sub(r'<.*?>', '', s)

        return s
    
    # START CRAWLING
    def crawling(country, keyword):
        print("\n", f"Crawler deployed... ", "\n")
        total_urls = []
        total_titles = []
        total_pubdates = []
        total_locations = [] 
        total_descriptions = []
        rows = []

        # Load the JSON document from a file
        #TODO: 
        """
            To solve the issue of the url. You should divide the url in two chunks.
            One before the keyword and the other after the keyword.
        """
        with open('/Users/juanreyesgarcia/Library/CloudStorage/OneDrive-FundacionUniversidaddelasAmericasPuebla/DEVELOPER/PROJECTS/CRAWLER_ALL/SELENIUM_CRAWLERS/INDEED_country.json') as f:
            data = json.load(f)

        # Search for a specific code in the JSON document
        for item in data:
            if item['code'] == country:  
                url = item['url']
                print(url)
        for i in range(0, num_pages * 10, 10):
            url = f"https://mx.indeed.com/jobs?q={keyword}&sc=0kf%3Aattr%28DSQF7%29%3B&sort=date&fromage=7&filter=0&start={i}"
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

                """
                scroll_origin = ScrollOrigin.from_element(job)
                # NOTE: You could even randomise the scroll origin -> random.uniform(770, 780) to avoid CAPTCHA
                # NOTE: it seems that as long as the job is within the viewport then it is fine, so 770 could be incremented
                # NOTE: Would displaying full screen on the driver solve delta_Y(hoe much it scrolls)? Overkill?
                ActionChains(driver).scroll_from_origin(scroll_origin, 0, 770).pause(.5).click(job).perform()
                #Wait for the element
                description_element = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, '#jobsearch-ViewjobPaneWrapper #jobDescriptionText')))
                #Get the text
                dirty_description = description_element.get_attribute("innerHTML")
                description = bye_regex(dirty_description)
                total_descriptions.append(description)
                # Pause for a random amount of time between 1 and 3 seconds to avoid CAPTCHA :P
                delay = random.uniform(1, 3)
                time.sleep(delay)

            #NOW THAT THE DIFFICULT PART IS OVER WE CAN EASILY GET THE REST OF THE ELEMENTS...

            #These two lines find a pattern of a given id
            titles = driver.find_elements(By.CSS_SELECTOR, '[id^="jobTitle"]')
            links = driver.find_elements(By.CSS_SELECTOR, '[id^="job_"]') #THIS FINDS THE PATTERN
            #I had to find this pattern bcos if not it would only yield the word "Posted"
            pubdates = driver.find_elements(By.XPATH, "//*[contains(text(), 'Publicado hace')]")
            locations = driver.find_elements(By.CSS_SELECTOR, '.companyLocation')
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
            #Put it all together...
            rows = {'titles': total_titles, 'links': total_urls, 'pubdate': total_pubdates, 'location': total_locations, 'description': total_descriptions}
        return rows
    
    data = crawling("MX", "PYTHON") #THIS IS YOUR SEARCH BOX - use "+" if more than 1 word

    
    driver.close()

    #Convert data to a pandas df for further analysis
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


#TODO: (FIX DATETIME see below)

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
    print(f"Yesterday was {yesterday}")"""
