#! python3
# searchpypi.py  - Opens several search results.

import requests, bs4, sys, pandas as pd, re

# CRITICAL!!! -> UNCOMMENT URL DEPENDING ON YOUR NEEDS...

url = sales_url = 'https://weworkremotely.com/remote-jobs/search?term=&categories%5B%5D=9&region%5B%5D=0&region%5B%5D=4&job_listing_type%5B%5D=Contract&job_listing_type%5B%5D=Full-Time'
#url = software_eng_url = 'https://weworkremotely.com/remote-jobs/search?term=&categories%5B%5D=17&categories%5B%5D=18&categories%5B%5D=4&region%5B%5D=0&region%5B%5D=4&job_listing_type%5B%5D=Full-Time'
#url = python_url = 'https://weworkremotely.com/remote-jobs/search?term=Python&button=&categories%5B%5D=17&categories%5B%5D=18&categories%5B%5D=4&region%5B%5D=0&region%5B%5D=4&job_listing_type%5B%5D=Full-Time'

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

def wwr_url():
    all_url = soup.find_all(href=re.compile("/remote-jobs/|/listings/")) #We use | to include both href
    rubish = ['https://weworkremotely.com/remote-jobs/search', 'https://weworkremotely.com/remote-jobs/new', 'https://weworkremotely.comhttps://weworkremotely.com/remote-jobs/search']
    urls_list = []
    for link in all_url:
        href = link.get('href')
        urls_list.append("https://weworkremotely.com" + href)
        curated_url = [url for url in urls_list if url not in rubish]
    return curated_url

#TITLES + URL = {TITLE:URL}
    
titles = soup.select('.title')
def Get_Title_Url():
    All_titles = []
    for title in titles:
        title_class = title.get_text()
        All_titles.append(title_class)
    #TITLE_URL = dict(zip(All_titles, wwr_url()))
    df = pd.DataFrame({"JOB TITLES": All_titles, "URLs": wwr_url()})
    # Set the maximum column width to 1000 -> to avoid pd to truncate the URL
    pd.set_option('display.max_colwidth', 1000)
    return print(df)

Get_Title_Url()

#Download Dictionary in CSV or send it to Notion?

