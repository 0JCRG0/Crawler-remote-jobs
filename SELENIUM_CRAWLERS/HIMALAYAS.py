from selenium import webdriver
from selenium.webdriver.common.by import By
import pandas as pd
import re

def WORKING_NOMADS():
    
    # 0. URL
    url = python_all = "https://himalayas.app/jobs/python"
    
    # 1. Start the session
    driver = webdriver.Firefox()

    # 2. Take action on browser 
    driver.get(url)

    # 4. Establish Waiting Strategy - 
    driver.implicitly_wait(0.5)

    #TODO: ADD PAGE BROWSING

    '.w-full'

    # 5. to use later... (to clean the text from regex)
    def TEXT_WASH(s):
        s = " ".join(s.split())
        s = re.sub(r'n/', '', s)
        return s
    
    #6. 
    def CRAWLING():
        total_urls = []
        total_titles = []
        total_pubDates = []
        total_locations = []
        total_categories = []
        #GETTING THE PARENT...
        jobs = driver.find_elements(By.CSS_SELECTOR, '#card-group [name = "card"]')
        for job in jobs:
            #GETTING THE ELEMENTS FROM THE CHILDREN... remember to use css_selectors instead of tags
            all_urls = job.find_elements(By.CSS_SELECTOR, '.mt-4.md\:mt-0.ml-0.md\:ml-5.w-full .flex.flex-row.items-start.justify-between.mb-2.w-full .flex.flex-row.items-center a')
            all_titles = job.find_elements(By.CSS_SELECTOR, '.mt-4.md\:mt-0.ml-0.md\:ml-5.w-full .flex.flex-row.items-start.justify-between.mb-2.w-full .flex.flex-row.items-center a .text-xl.font-medium.text-gray-900')
            all_pubDates = job.find_elements(By.CSS_SELECTOR, '.mt-4.md\:mt-0.ml-0.md\:ml-5.w-full .flex.flex-row.items-start.justify-between.mb-2.w-full .hidden.md\:block.md\:whitespace-nowrap.md\:ml-2.relative div .text-base.text-gray-600')
            all_location = job.find_elements(By.CSS_SELECTOR, '.mt-4.md\:mt-0.ml-0.md\:ml-5.w-full .flex.flex-row.items-center.justify-between .flex.flex-row.items-center.flex-wrap.overflow-hidden .hidden.md\:flex.md\:flex-row.md\:items-center .badge.badge-gray.no-hover.mr-2 .flex.flex-row.items-center .badge-text.whitespace-nowrap.no-hover.text-center')
            all_category = job.find_elements(By.CSS_SELECTOR, '.mt-4.md\:mt-0.ml-0.md\:ml-5.w-full .flex.flex-row.items-center.justify-between .flex.flex-row.items-center.flex-wrap.overflow-hidden .flex .badge.badge-blue.w-full .flex.flex-row.items-center .badge-text.text-center.whitespace-nowrap')
            for url in all_urls:
                href = url.get_attribute("href")
                total_urls.append(href)
            for x in all_titles:
                title = x.get_attribute("innerHTML")
                total_titles.append(title)
            for dates in all_pubDates:
                pubDate = dates.get_attribute("innerHTML") 
                total_pubDates.append(pubDate)
            for loc in all_location:
                location = loc.get_attribute("innerHTML") 
                total_locations.append(location)
            for y in all_category:
                category = y.get_attribute("innerHTML") 
                total_categories.append(category)

            
        return total_titles, total_urls, total_pubDates, total_locations, total_categories
    elements = CRAWLING()


    print(elements, len(elements))
            
            
            
    #TODO -- FIX THIS. PROBABLY USE LOCATORS (LOCATE WITH IS NOT FOR / WHY IS IT TAKING SO LONG?)

    """
    #6. GETTING TITLES & JOBS -- GETTING RID OF EXPIRED POSITIONS...
    def GET_TITLES_ALL_JOBS():
        all_titles = []
        all_urls = []
        all_expired_urls = []
        #get all the jobs
        jobs_card = driver.find_element(By.ID, "card-group")
        jobs = jobs_card.find_elements(By.TAG_NAME, "div")
        #get the expired jobs
        #expired_jobs = driver.find_elements(By.CSS_SELECTOR, "#jobs .job.expired .media-body h4 .open-button.ng-binding")
        for job in all_jobs: #add dirty titles & urls to their respective lists
            title = job.get_attribute("text")
            href = job.get_attribute("href")
            all_titles.append(title)
            all_urls.append(href)
        for job_expired in expired_jobs: #get expired links & add them to their list
            href_expired = job_expired.get_attribute("href")
            all_expired_urls.append(href_expired)
        #get rid of the expired links 
        curated_urls = [url for url in all_urls if url not in all_expired_urls] 
        #slice the list of titles to only get the ones that are not expired
        correct_but_dirty_titles = len(curated_urls)
        dirty_titles = all_titles[:correct_but_dirty_titles]
        #clean those dirty titles
        curated_titles = [TEXT_WASH(s) for s in dirty_titles]
        #Transform it into a df to improve readability
        df = pd.DataFrame({"JOB TITLES": curated_titles, "URLs": curated_urls})
        # Set the maximum column width to 1000 -> to avoid pd to truncate the URL
        pd.set_option('display.max_colwidth', 1000)
        return print(df)
    GET_TITLES_ALL_JOBS()
    """

    driver.quit()

WORKING_NOMADS()
