from selenium import webdriver
from selenium.webdriver.common.by import By
import pandas as pd
import re
import pretty_errors
import timeit
from dateutil.parser import parse
from datetime import date
import json
import logging
from utils.handy import cleansing_selenium_crawlers, to_postgre, test_postgre, freelance_postgre



def selenium_crawlers(TYPE):
    # configure the logger
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)

    # create a file handler
    handler = logging.FileHandler('selenium_crawlers.log')
    handler.setLevel(logging.DEBUG)

    # create a logging format
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)

    # add the handlers to the logger
    logger.addHandler(handler)

    try:

        #start timer
        start_time = timeit.default_timer()

        #Modify the options so it is headless - to disable just comment the next 2 lines and use the commented driver
        options = webdriver.FirefoxOptions()
        options.add_argument('-headless')
            
        # Start the session
        #driver = webdriver.Firefox()
        driver = webdriver.Firefox(options=options)

        """
        The following is specifying which JSON to load & to which table it will be sent
        """

        if TYPE == 'MAIN':
            JSON = './selenium_resources/main_sel_crawlers.json'
            POSTGRESQL = to_postgre
            print("\n", f"Reading {JSON}. Jobs will be sent to PostgreSQL's master_jobs table", "\n")
        elif TYPE == 'FREELANCE':
            JSON = './selenium_resources/freelance.json'
            POSTGRESQL = freelance_postgre
            print("\n", f"Reading {JSON}. Jobs will be sent to PostgreSQL's freelance table", "\n")
        else:
            print("\n", "Incorrect argument! Use either 'MAIN' or 'FREELANCE' to run this script.", "\n")


        def elements():
            total_links = []
            total_titles = []
            total_pubdates = []
            total_locations = [] 
            total_descriptions = []
            rows = []

            with open(JSON) as f:
                #load the json
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
                            
                            #TITLES
                            title_element = job.find_element(By.CSS_SELECTOR, elements_path["title_path"])
                            if title_element is not None:
                                title = title_element.get_attribute("innerHTML")
                                total_titles.append(title)
                            else:
                                total_titles.append('NaN')
                            
                            #LINKS
                            link_element = job.find_element(By.CSS_SELECTOR, elements_path["link_path"])
                            if link_element is not None:
                                href = link_element.get_attribute("href")
                                total_links.append(href)
                            else:
                                total_links.append("NaN")

                            #PUBDATES - to simplify things & considering this snippet will be run daily datetime is the same day as the day this is running                       
                            for i in range(len(total_links)):
                                today = date.today()
                                total_pubdates.append(today)
                            
                            #LOCATIONS
                            location_element = job.find_element(By.CSS_SELECTOR, elements_path["location_path"])
                            if location_element is not None:
                                location = location_element.get_attribute("innerHTML") 
                                total_locations.append(location)
                            else:
                                total_locations.append("NaN")
                            
                            #Descriptions
                            description_element = job.find_element(By.CSS_SELECTOR, elements_path["description_path"])
                            if description_element is not None:
                                description = description_element.get_attribute("innerHTML") 
                                total_descriptions.append(description)
                            else:
                                total_descriptions.append("NaN")
                            
                            #Add them all
                            rows = {"title": total_titles, "link": total_links, "description": total_descriptions, "pubdate": total_pubdates, "location": total_locations}
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
        df.to_csv(f"{directory}pre-pipeline-Sel_All.csv", index=False)

        # count the number of duplicate rows
        num_duplicates = df.duplicated().sum()

            # print the number of duplicate rows
        print("Number of duplicate rows:", num_duplicates)

            # remove duplicate rows based on all columns
        df = df.drop_duplicates()
        
        def pipeline(df):

            # Convert str to datetime & clean titles
            #TODO: Solve SettingWithCopyWarning
            
            for col in df.columns:
                if col != 'pubdate':
                    df.loc[:, col] = df.loc[:, col].astype(str).apply(cleansing_selenium_crawlers)
            
            #df.loc[:, df.columns != 'pubdate'] = df.loc[:, df.columns != 'pubdate'].astype(str).apply(cleansing_selenium_crawlers)

            #Save it in local machine
            directory = "./OUTPUTS/"
            df.to_csv(f"{directory}post-pipeline-Sel_All.csv", index=False)


            # SEND IT TO TO PostgreSQL 
            
            POSTGRESQL(df)

            #print the time
            elapsed_time = timeit.default_timer() - start_time
            print("\n")
            print(f"The selected selenium crawlers have finished! all in: {elapsed_time:.2f} seconds.", "\n")
        pipeline(df)
    
    except Exception as e:
        # log the error
        logger.error(f'Error occurred: {str(e)}')
        # handle the error

if __name__ == "__main__":
    selenium_crawlers('FREELANCE') 