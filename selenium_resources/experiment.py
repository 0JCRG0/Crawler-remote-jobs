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
from utils.handy import clean_rows, initial_clean, to_postgre, test_postgre



def selenium_crawlers(pages):
    print("\n", "HIMALAYAS starting now.", "\n")

    #start timer
    start_time = timeit.default_timer()

    #Modify the options so it is headless - to disable just comment the next 2 lines and use the commented driver
    options = webdriver.FirefoxOptions()
    options.add_argument('-headless')
        
    # Start the session
    #driver = webdriver.Firefox()
    driver = webdriver.Firefox(options=options)

    def elements():
        total_urls = []
        total_titles = []
        total_pubdates = []
        total_locations = [] 
        total_descriptions = []
        rows = []
        with open('./selenium_resources/main_elements.json') as f:
            #load the json
            data = json.load(f)
            # Access the 'urls' list in the first dictionary of the 'data' list and assign it to the variable 'urls'
            urls = data[0]["urls"]
            for url_obj in urls:
                # Extract the 'url' key from the current dictionary and assign it to the variable 'url_prefix'
                url_prefix = url_obj['url']
                # Extract the first dictionary from the 'elements_path' list in the current dictionary and assign it to the variable 'elements_path'
                elements_path = url_obj['elements_path'][0]
                # +1 is done due to the exclusive nature of range
                for i in range(1, pages + 1):
                    url = url_prefix + str(i)
                    # get the url
                    driver.get(url)
                    print(f"Crawling... {url}")
                    # establish waiting strategy
                    driver.implicitly_wait(1)
                    # GETTING THE PARENT...
                    jobs = driver.find_elements(By.CSS_SELECTOR, elements_path["jobs_path"])
                    for job in jobs:
                        # Getting the elements from the parent, making reference to the json
                        all_titles = job.find_elements(By.CSS_SELECTOR, elements_path["title_path"])
                        all_urls = job.find_elements(By.CSS_SELECTOR, elements_path["link_path"])
                        #all_pubDates = job.find_elements(By.CSS_SELECTOR, elements_path["pubdate_path"])
                        all_location = job.find_elements(By.CSS_SELECTOR, elements_path["location_path"])
                        all_category = job.find_elements(By.CSS_SELECTOR, elements_path["description_path"])
                        for i in all_titles:
                            title = i.get_attribute("innerHTML")
                            curated_title = [initial_clean(title)] #We clean the title with the function in handy
                            total_titles.append(curated_title)
                        for i in all_urls:
                            href = i.get_attribute("href")
                            total_urls.append(href)                        
                        for i in range(len(all_urls)):
                            today = date.today()
                            total_pubdates.append(today)
                        for i in all_location:
                            location = i.get_attribute("innerHTML") 
                            total_locations.append(location)
                            total_locations.append("NaN")
                        for i in all_category:
                            category = i.get_attribute("innerHTML") 
                            total_descriptions.append(category)
                        rows = {"title": total_titles, "link": total_urls, "description": total_descriptions, "pubdate": total_pubdates, "location": total_locations}
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
    
    def pipeline(df):

        # Fill missing values with "NaN"
        df.fillna("NaN", inplace=True)

        # Convert str to datetime & clean titles
        for col in df.columns:
            if col == 'title':
                df[col] = df[col].astype(str)
                df[col] = df[col].apply(clean_rows)
                df[col] = df[col].str.slice(0, 255)
            else:
                df[col] = df[col].str.slice(0, 255)


        # SEND IT TO TO PostgreSQL
        test_postgre(df)
        
        #print the time
        elapsed_time = timeit.default_timer() - start_time
        print("\n")
        print(f"HIMALAYAS is done! all in: {elapsed_time:.2f} seconds.", "\n")
    pipeline(df)

if __name__ == "__main__":
    selenium_crawlers(4) 