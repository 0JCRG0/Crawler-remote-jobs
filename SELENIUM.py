from selenium import webdriver
from selenium.webdriver.common.by import By
import pandas as pd
import re
import csv
import pretty_errors
import psycopg2
import timeit
from dateutil.relativedelta import relativedelta
from utils.handy import clean_rows, initial_clean, to_postgre


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

if __name__ == "__main__":
    himalayas(1) #the 1st argument is the number of pages to scrap (needs to be int64)