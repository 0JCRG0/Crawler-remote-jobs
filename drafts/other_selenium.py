from selenium import webdriver
from selenium.webdriver.common.by import By
import pandas as pd
import re
import pretty_errors
import timeit
from dateutil.parser import parse
from datetime import datetime, timedelta, date
import json
from dateutil.relativedelta import relativedelta
from utils.handy import clean_rows, initial_clean, to_postgre, bye_regex, test_postgre, indeed_regex, personal_postgre, id_generator


#IMPORT THE PATH - YOU NEED TO EXPORT YOUR OWN PATH TO zsh/bash & SAVE IT AS 'CRAWLER_ALL'
PATH = '/Users/juanreyesgarcia/Library/CloudStorage/OneDrive-FundacionUniversidaddelasAmericasPuebla/DEVELOPER/PROJECTS/CRAWLER_ALL/'

def himalayas(pages, cut_off):
    print("\n", "HIMALAYAS starting now.", "\n")

    #start timer
    start_time = timeit.default_timer()

    def crawler():

        #Modify the options so it is headless - to disable just comment the next 2 lines and use the commented driver
        options = webdriver.FirefoxOptions()
        options.add_argument('-headless')
        
        # Start the session
        #driver = webdriver.Firefox()
        driver = webdriver.Firefox(options=options)

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
                    for i in all_urls:
                        href = i.get_attribute("href")
                        total_urls.append(href)
                    for i in all_titles:
                        title = i.get_attribute("innerHTML")
                        curated_title = [initial_clean(title)] #We clean the title with the function in handy
                        total_titles.append(curated_title)
                    for i in all_pubDates:
                        pubDate = i.get_attribute("innerHTML") 
                        total_pubDates.append(pubDate)
                    for i in all_location:
                        location = i.get_attribute("innerHTML") 
                        total_locations.append(location)
                    for i in all_category:
                        category = i.get_attribute("innerHTML") 
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
        end_date = pd.to_datetime(cut_off)
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

"""

INDEED IS BELOW...

"""

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
    #himalayas(1, '2023-03-20') 
    
    """
    1st = number of pages to scrap (needs to be int64)
    2nd = cut-off date
    """
    
    indeed(1, "MX", "DATA+ANALYST") 
    
    """
    1st = number of pages
    2nd = country code from json
    3rd = keyword(s) -if more than 1 use +
    
    """