from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd
import re
import random

def ROTTEN_TOMATOES():
    #0. URL
    
    #url = python_latam_anywhere = "https://www.workingnomads.com/jobs?location=anywhere,latin-america&tag=python"
    url = sales = "https://www.rottentomatoes.com/browse/movies_at_home/audience:upright~critics:certified_fresh,fresh~sort:popular?page=5"
    
    # 1. Start the session
    driver = webdriver.Firefox()

    # 2. Take action on browser 
    driver.get(url)

    #4. Establish Waiting Strategy - 
    driver.implicitly_wait(0.5)

    #5 to use later... (to clean the text [blank soaces & n/])
    def TEXT_WASH(s):
        s = " ".join(s.split())
        s = re.sub(r'n/', '', s)
        return s

    #6. GETTING TITLES & JOBS -- GETTING RID OF EXPIRED POSITIONS...
    def GET_TITLES_ALL_JOBS():
        all_titles = []
        all_links = []
        all_pubDates = []
        all_critic_percentages = []
        all_audience_percentages = []
        
        #wait...
        #wait = WebDriverWait(driver, 30)
        
        #get all the elements link
        titles = driver.find_elements(By.CSS_SELECTOR, ".js-tile-link tile-dynamic div .p--small")
        links = driver.find_elements(By.CSS_SELECTOR, ".js-tile-link")
        pubDates = driver.find_elements(By.CSS_SELECTOR, ".js-tile-link tile-dynamic div .smaller")
        audience_percentages = driver.find_elements(By.CSS_SELECTOR, "a.js-tile-link tile-dynamic div score-pairs #shadow-root div.wrap score-icon-critic #shadow-root div.wrap span.percentage")
        #audience_percentages = driver.find_elements(By.CSS_SELECTOR, ".js-tile-link tile-dynamic div score-pairs #shadow-root .wrap score-icon-audience  #shadow-root .wrap .percentage")
        #audience_percentages = wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, '#main-page-content > div > div.discovery-grids-container.sticky-header > div > div > a:nth-child(40) > tile-dynamic > div > score-pairs > div > score-icon-audience > div > span.percentage')))
        #audience_percentages = driver.find_elements(By.CSS_SELECTOR, "#main-page-content > div > div.discovery-grids-container.sticky-header > div > div > a:nth-child(40) > tile-dynamic > div > score-pairs >>> div > score-icon-audience >>> div > span.percentage")

        """
        #get those attributes
        for title in titles: 
            title_x = title.get_attribute("text")
            all_titles.append(title_x)
        for link in links: 
            link_x = link.get_attribute("href")
            all_links.append(link_x)
        for pubDate in pubDates:
            pubDate_x = pubDate.get_attribute("text")
            all_pubDates.append(pubDate_x)
        for critic_percentage in critic_percentages:
            critic_percentage_x = critic_percentage.get_attribute("text")
            all_critic_percentages.append(critic_percentage_x)
        for audience_percentage in audience_percentages:
            audience_percentage_x = audience_percentage.text
            all_audience_percentages.append(audience_percentage_x)
        """
        
        #Transform it into a df to improve readability
        #all_x = {"MOVIE TITLE": all_links, "LINKS": all_links, "DATE": all_pubDates, "CERTIFIED FRESH":all_critic_percentages, "AUDIENCE SCORE": all_audience_percentages}
        # Set the maximum column width to 1000 -> to avoid pd to truncate the URL
        #pd.set_option('display.max_colwidth', 150)
        #random_1 = random.choice(all_links)
        
        
        return print(links)
    GET_TITLES_ALL_JOBS()

    driver.quit()

ROTTEN_TOMATOES()