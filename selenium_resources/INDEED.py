#! python3
from selenium import webdriver
from selenium.webdriver.common.by import By
import pandas as pd
import re
import pretty_errors
import json
import timeit
from datetime import date


def indeed(pages, country, keyword):
    #start timer
    start_time = timeit.default_timer()

    # Start the session
    driver = webdriver.Firefox()

    # set the number of pages you want to scrape
    
    # START CRAWLING
    def crawling():
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

        for i in range(0, pages * 10, 10):
            #url = f"https://mx.indeed.com/jobs?q={keyword}&sc=0kf%3Aattr%28DSQF7%29%3B&sort=date&fromage=7&filter=0&start={i}"
            url = country_url()
            #Make the request
            driver.get(url)
            print(f"Crawling... {url}")
            #Set waiting strategy
            driver.implicitly_wait(1.5)

            #These two lines find a pattern of a given id
            titles = driver.find_elements(By.CSS_SELECTOR, '[id^="jobTitle"]')
            links = driver.find_elements(By.CSS_SELECTOR, '[id^="job_"]') #THIS FINDS THE PATTERN
            locations = driver.find_elements(By.CSS_SELECTOR, '.companyLocation')
            descriptions = driver.find_elements(By.CSS_SELECTOR, '#mosaic-provider-jobcards .jobCardShelfContainer.big6_visualChanges .heading6.tapItem-gutter.result-footer')
            #Get the attributes
            for i in titles:
                title = i.get_attribute("innerHTML")
                total_titles.append(title)
            for i in links:
                href = i.get_attribute("href")
                total_urls.append(href)
            for i in range(len(titles)):
                today = date.today()
                #dirty_pubdate = i.text
                #pubdate = bye_regex(dirty_pubdate)
                total_pubdates.append(today)
            for i in locations:
                location = i.text
                total_locations.append(location)
            for i in descriptions:
                description = i.get_attribute("innerHTML")
                #description = bye_regex(dirty_description)
                total_descriptions.append(description)
            #Put it all together...
            rows = {'titles': total_titles, 'links': total_urls, 'pubdate': total_pubdates, 'location': total_locations, 'description': total_descriptions}
        return rows
    
    data = crawling()

    
    driver.close()

    #Convert data to a pandas df for further analysis
    print("\n", "Converting data to a pandas df...", "\n")
    data_dic = dict(data)
    df = pd.DataFrame.from_dict(data_dic, orient='index')
    df = df.transpose()
    pd.set_option('display.max_colwidth', 150)
    pd.set_option("display.max_rows", None)

    def indeed_regex(s):
        pattern = r'<li>(.*?)<\/li>'
        matches = re.findall(pattern, s, re.DOTALL)
        if len(matches) > 0:
            text = matches[0]
            text = re.sub(r'<b>|<\/b>', '', text)
            return text
        else:
            return ''

    for col in df.columns:
        if col == 'description':
            df[col] = df[col].apply(indeed_regex)

    print(df)

    print("\n", "Saving jobs in local machine...", "\n")
    directory = "./OUTPUTS/"
    df.to_csv(f'{directory}INDEED_MX.csv', index=False)

#stop the timer
    elapsed_time = timeit.default_timer() - start_time
    print("\n", f"Crawler successfully found {len(df)} jobs in {elapsed_time:.5f} seconds", "\n")


if __name__ == "__main__":
    indeed(2, "MX", "data+analyst") 
    """
    1st = number of pages
    2nd = country code from json
    3rd = keyword(s) -if more than 1 use +
    
    """