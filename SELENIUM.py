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
import csv
import pretty_errors
import psycopg2
import timeit
from dateutil.parser import parse
from datetime import datetime, timedelta
import json
from dateutil.relativedelta import relativedelta
from utils.handy import clean_rows, initial_clean, to_postgre, bye_regex


def himalayas(pages):
    print("\n", "HIMALAYAS starting now.", "\n")

    #start timer
    start_time = timeit.default_timer()

    def crawler():
        
        # Start the session
        driver = webdriver.Firefox()

        # START CRAWLING
        def elements():
            total_urls = []
            total_titles = []
            total_pubDates = []
            total_locations = [] 
            total_categories = []
            rows = []
            for i in range(1, pages + 1):
                url = f"https://himalayas.app/jobs?page={i}"
                driver.get(url)
                print(f"Crawling... {url}")
                # Establish Waiting Strategy
                driver.implicitly_wait(1)
                #GETTING THE PARENT...
                jobs = driver.find_elements(By.CSS_SELECTOR, '#card-group [name = "card"]')
                for job in jobs:
                    #GETTING THE ELEMENTS FROM THE CHILDREN... remember to use css_selectors instead of tags
                    all_urls = job.find_elements(By.CSS_SELECTOR, ' .mt-4.md\:mt-0.ml-0.md\:ml-5.w-full .flex.flex-row.items-start.justify-between.mb-2.w-full .flex.flex-row.items-center a')
                    all_titles = job.find_elements(By.CSS_SELECTOR, '.mt-4.md\:mt-0.ml-0.md\:ml-5.w-full .flex.flex-row.items-start.justify-between.mb-2.w-full .flex.flex-row.items-center a .text-xl.font-medium.text-gray-900')
                    all_pubDates = job.find_elements(By.CSS_SELECTOR, '.mt-4.md\:mt-0.ml-0.md\:ml-5.w-full .flex.flex-row.items-start.justify-between.mb-2.w-full .hidden.md\:block.md\:whitespace-nowrap.md\:ml-2.relative div .text-base.text-gray-600')
                    all_location = job.find_elements(By.CSS_SELECTOR, '.mt-4.md\:mt-0.ml-0.md\:ml-5.w-full .flex.flex-row.items-center.justify-between .flex.flex-row.items-center.flex-wrap.overflow-hidden .hidden.md\:flex.md\:flex-row.md\:items-center .badge.badge-gray.no-hover.mr-2 .flex.flex-row.items-center .badge-text.whitespace-nowrap.no-hover.text-center')
                    all_category = job.find_elements(By.CSS_SELECTOR, '.mt-4.md\:mt-0.ml-0.md\:ml-5.w-full .flex.flex-row.items-center.justify-between .flex.flex-row.items-center.flex-wrap.overflow-hidden .flex div .flex.flex-row.items-center .badge-text.text-center.whitespace-nowrap')
                    for url in all_urls:
                        href = url.get_attribute("href")
                        total_urls.append(href)
                    for titl in all_titles:
                        title = titl.get_attribute("innerHTML")
                        curated_title = [initial_clean(title)] #We clean the title with the function in handy
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
                    #Save it all in a single list of dictionaries
                    rows = {"title": total_titles, "link": total_urls, "pubdate": total_pubDates, "location": total_locations, "description": total_categories}
            return rows
        #Quit the driver
        data = elements()
        driver.quit()
        
        #The data had missing values so we have to do this to convert it into a pandas df
        data_dic = dict(data)
        df = pd.DataFrame.from_dict(data_dic, orient='index')
        df = df.transpose()

        #Save it in local machine
        directory = "./OUTPUTS/"
        df.to_csv(f"{directory}himalaya.csv", index=False)
        return df

    def pipeline():
        df = crawler()

        # Fill missing values with "NaN"
        df.fillna("NaN", inplace=True)

        # Mask to convert minutes|hour% to 1 day ago so it can be parse to date time (cba to say it was posted today, maybe later should be fixed)
        mask = df['pubdate'].str.contains('minutes|hour%')
        df.loc[mask, 'pubdate'] = "1 day ago"

        # From relative date strings to date time...
        def convert_date(x):
            if isinstance(x, str) and 'ago' in x:
                return pd.Timestamp.today() - relativedelta(days=int(x.split()[0]))
            return x

        # Convert str to datetime & clean titles
        for col in df.columns:
            if col == 'pubdate':
                df[col] = df[col].apply(convert_date).astype('datetime64[ns]')
            if col == 'title':
                df[col] = df[col].astype(str)
                df[col] = df[col].apply(clean_rows)

        ##reindex
        df = df.reindex(columns=['title', 'link', 'description', 'pubdate', 'location'])

        # Filter rows by a date range (this reduces the number of rows... duh)
        ## set the date range
        start_date = pd.to_datetime('2016-01-01')
        end_date = pd.to_datetime('2023-02-15')
        ## apply it
        date_range_filter = (df['pubdate'] >= start_date) & (df['pubdate'] <= end_date)
        df = df.loc[~date_range_filter]

        # SEND IT TO TO PostgreSQL
        to_postgre(df)
        
        #print the time
        elapsed_time = timeit.default_timer() - start_time
        print("\n")
        print(f"HIMALAYAS is done! all in: {elapsed_time:.2f} seconds.", "\n")
    pipeline()

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
                description = bye_regex(dirty_description)
                total_descriptions.append(description)
                # Pause for a random amount of time between 1 and 3 seconds to avoid CAPTCHA :P
                delay = random.uniform(0, 3)
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
    
    data = crawling("USA", "DATA ANALYST") #THIS IS YOUR SEARCH BOX - use "+" if more than 1 word

    
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
    df.to_csv(f'{directory}INDEED_MX.csv', index=False)

#stop the timer
    elapsed_time = timeit.default_timer() - start_time
    print("\n", f"Crawler successfully found {len(df)} jobs in {elapsed_time:.5f} seconds", "\n")


#TODO: (FIX DATETIME see below)

"""
        def fix_datetime(x):
                # get today's datetime
                today = datetime.now()

                # example string
                str_date = pubdate

                # extract the number of days from the string
                num_days = int(str_date.split()[2])

                # calculate yesterday's datetime
                pubdate_good = today - timedelta(days=num_days)

                # example if statement
                return pubdate_good
"""

if __name__ == "__main__":
    himalayas(1) #the 1st argument is the number of pages to scrap (needs to be int64)
    indeed()