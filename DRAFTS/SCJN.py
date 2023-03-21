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
import bs4
import json
import requests
import sys
import timeit

"https://bj.scjn.gob.mx/busqueda?q=*&indice=tesis&subFiltros=tipo.keyword:Tesis%20de%20Jurisprudencia,materia.keyword:Laboral"

def scjn():

    #Driver...
    driver = webdriver.Firefox()

    # set the number of pages you want to scrape
    num_pages = 1
    
    start_time = timeit.default_timer()

    def url_araña():
        print("\n", f"Iniciando araña... 0%", "\n")
        url_parsed = []
        urls = []

        """def country_url():
            #TODO: ADD THIS CODE TO THE URL
            url = ""
            with open('./SELENIUM_CRAWLERS/INDEED_country.json') as f:
                    data = json.load(f)
                    # Search for a specific code in the JSON document
                    for item in data:
                        if item['code'] == materia:  
                            url= item['url_1'] + materia + item["url_2"] + str(i)
                            print(url)
            return url
            """
        for i in range(0, num_pages):
            url = "https://bj.scjn.gob.mx/busqueda?q=*&indice=tesis&subFiltros=tipo.keyword:Tesis%20de%20Jurisprudencia,materia.keyword:Laboral&size=25"
            #Make the request
            #Make the soup...
            print("\n", 'Tejiendo telaraña en...' + url, "\n")
            #Make the request
            driver.get(url)
            #Set waiting strategy
            driver.implicitly_wait(1.5)
            #Find all the job boxes to click on
            tesis = driver.find_elements(By.CSS_SELECTOR, '.pub-results__list .card.my-3.ng-star-inserted.find a')
            for i in tesis:
                url = i.get_attribute("href")
                url_parsed.append(url)
            urls = {"link": url_parsed}
        return urls
    
    data_ = url_araña()

    #close the driver
    driver.close()

    data = dict(data_)
    df = pd.DataFrame.from_dict(data, orient='index')
    df = df.transpose()
    print(df)

    print("\n", "Saving jobs in local machine...", "\n")
    directory = "./OUTPUTS/"
    df.to_csv(f'{directory}SCJN_LABORAL_JURISPRUDENCIA.csv', index=False)

    def make_soup(url):
            try:
                res = requests.get(url)
                res.raise_for_status()
            except Exception as e:
                print(f'An error occurred: {e}')
                sys.exit(1)
            return res

    def text_araña():
        file = "./OUTPUTS/SCJN_LABORAL_JURISPRUDENCIA.csv"
        text_all = []
        with open(file, newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                url = row['link']
                soup = bs4.BeautifulSoup(make_soup(url).text, 'html.parser')
                print(url)
                titulo = soup.select('.int-doc-tesis__metadata.row.p-2.m-0')
                for i in titulo:
                    text = i.get_text()
                    text_all.append(text)
        return text_all
    text = text_araña()
    print(text)


#print the time
    elapsed_time = timeit.default_timer() - start_time
    print("\n", f"Terminó la araña en: {elapsed_time:.5f} segundos", "\n")


if __name__ == "__main__":
    scjn()
    

"""driver = webdriver.Firefox()

            driver.get(url)
            print(f"Crawling... {url}")
            #Set waiting strategy
            driver.implicitly_wait(1.5)
            
            #Find all the job boxes to click on
            tesis = driver.find_elements(By.CSS_SELECTOR, '.pub-results__list')
            #Lil loop through the elements
            for job in tesis:
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


"""