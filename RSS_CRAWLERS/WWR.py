#! python3

import requests
import bs4
import sys
import pandas as pd
import re
import psycopg2
import pretty_errors
import timeit
from utils.handy import test_postgre, send_postgre

def WWR():
    #start timer
    start_time = timeit.default_timer()
    
    print("\n", "Crawler deployed... ", "\n")
    
    #Select how many pages you want to scrap
    num_pages = 3

    jobs = []
    links_dirty = [] #links not curated
    links_all = []
    titles_all = []
    locations_all = []
    pubdates_all = []
    rubish = ['https://weworkremotely.com/remote-jobs/search', 'https://weworkremotely.com/remote-jobs/new', 'https://weworkremotely.comhttps://weworkremotely.com/remote-jobs/search']
        
    #Iterate through all the pages
    for i in range(1, num_pages + 1):
        url = f"https://weworkremotely.com/remote-full-time-jobs?page={i}"   
            
        #Make the soup...
        print('Crawling...' + url)
        def make_soup(url):
            try:
                res = requests.get(url)
                res.raise_for_status()
            except Exception as e:
                print(f'An error occurred: {e}')
                sys.exit(1)
            return res
        soup = bs4.BeautifulSoup(make_soup(url).text, 'html.parser')
            
        #Get the elements...       
        titles_parsed = soup.select('.title') #getting the class
        locations_parsed = soup.select('.region.company') #getting the class
        pubdates_parsed = soup.find_all('time') #getting the tag
        links_parsed = soup.find_all(href=re.compile("/remote-jobs/|/listings/")) #We use | to include both href
        for t in titles_parsed:
            title = t.get_text()
            titles_all.append(title)
        for l in links_parsed:
            link = l.get('href')
            links_dirty.append("https://weworkremotely.com" + link)
        links_all = [url for url in links_dirty if url not in rubish]
        for l in locations_parsed:
            location = l.get_text()
            locations_all.append(location)
        for p in pubdates_parsed:
            pubdate = p.get('datetime') #getting attrs
            pubdates_all.append(pubdate)
        #this is just a ridiculous-unnecessary way of saving all the data
        jobs += [{"title": title, "link": link, "pubdate": pubdate, "location": location, "description": None} for title, link, pubdate, location in zip(titles_all, links_all, pubdates_all, locations_all)]
    #PARSE IT TO A PANDAS DF
    df = pd.DataFrame.from_records(jobs)
    
    #COUNT THE JOBS 
    print("\n", f"Crawler successfully found {len(df)} jobs...", "\n")
    
    #SAVE DATA...
    print("\n", "Saving jobs in local machine as a CSV file...", "\n")
    directory = "./OUTPUTS/"
    df.to_csv(f"{directory}WWR.csv", index=False)
    
    #SEND IT TO TO PostgreSQL
    print("\n", f"Fetching {len(df)} jobs to PostgreSQL...", "\n")

    """
    Less jobs are sent to postgre than the ones obtained bcos the upsert strtategy used in send_postgre()
    only admits unique values on the link column
    """ 
    
    #Send it to postgre
    send_postgre(df)

    #print the time
    elapsed_time = timeit.default_timer() - start_time
    print("\n", f"All done! {len(df)} jobs were found and sent to PostgreSQL in: {elapsed_time:.2f} seconds", "\n")
WWR()