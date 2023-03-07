#! python3
# searchpypi.py  - Opens several search results.

import requests
import bs4
import sys
import pandas as pd
import re
import pretty_errors

# CRITICAL!!! -> UNCOMMENT URL DEPENDING ON YOUR NEEDS...

#url = sales_url = 'https://weworkremotely.com/remote-jobs/search?term=&categories%5B%5D=9&region%5B%5D=0&region%5B%5D=4&job_listing_type%5B%5D=Contract&job_listing_type%5B%5D=Full-Time'
#url = software_eng_url = 'https://weworkremotely.com/remote-jobs/search?term=&categories%5B%5D=17&categories%5B%5D=18&categories%5B%5D=4&region%5B%5D=0&region%5B%5D=4&job_listing_type%5B%5D=Full-Time'
#url = python_url = 'https://weworkremotely.com/remote-jobs/search?term=Python&button=&categories%5B%5D=17&categories%5B%5D=18&categories%5B%5D=4&region%5B%5D=0&region%5B%5D=4&job_listing_type%5B%5D=Full-Time'
url = all_jobs = 'https://weworkremotely.com/remote-full-time-jobs'


print('Searching...')
def make_soup(url):
    try:
        res = requests.get(url)
        res.raise_for_status()
    except Exception as e:
        print(f'An error occurred: {e}')
        sys.exit(1)
    return res


soup = bs4.BeautifulSoup(make_soup(url).text, 'html.parser')

#Getting every URL

def URL():
    urls_list = []
    curated_url = []
    all_url = soup.find_all(href=re.compile("/remote-jobs/|/listings/")) #We use | to include both href
    rubish = ['https://weworkremotely.com/remote-jobs/search', 'https://weworkremotely.com/remote-jobs/new', 'https://weworkremotely.comhttps://weworkremotely.com/remote-jobs/search']
    for link in all_url:
        href = link.get('href')
        urls_list.append("https://weworkremotely.com" + href)
    curated_url = [url for url in urls_list if url not in rubish]
    return curated_url


def READY_JOBS():
    links_all = URL()
    titles_parsed = soup.select('.title') #getting the class
    locations_parsed = soup.select('.region.company') #getting the class
    pubdates_parsed = soup.find_all('time') #getting the tag
    titles_all = []
    locations_all = []
    pubdates_all = []
    jobs = []
    for t in titles_parsed:
        title = t.get_text()
        titles_all.append(title)
    for l in locations_parsed:
        location = l.get_text()
        locations_all.append(location)
    for p in pubdates_parsed:
        pubdate = p.get('datetime')
        pubdates_all.append(pubdate)
    jobs = {"title": titles_all, "link": links_all, "pubdate": pubdates_all, "location": locations_all, "description": " "}
    return jobs

data = READY_JOBS()
data_dic = dict(data)
df = pd.DataFrame.from_dict(data_dic, orient='index')
df = df.transpose()

print(df)
df.to_csv("WWR.csv", index=False)

#Download Dictionary in CSV or send it to Notion?

