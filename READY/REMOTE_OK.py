#! python3
# searchpypi.py  - Opens several search results.

import requests
import sys
import pandas as pd
import bs4
import lxml
# CRITICAL!!! -> THIS IS ONLY FOR XML!!! NOT HTTP PARSERS...

rss = rss_remote_ok = 'https://remoteok.com/rss'
print('Searching...')

#Make the request sirr
def request_rss(rss):
    try:
        res = requests.get(rss)
        res.raise_for_status()
    except Exception as e:
        print(f'An error occurred: {e}')
        sys.exit(1)
    return res

#MAKE THE SOOOOOOUP...
def make_soup():
    soup = bs4.BeautifulSoup(request_rss(rss).text, 'lxml-xml')
    return soup

# Get those tags
def REMOTE_OK():
    items_new = []
    for item in make_soup().find_all('item'):
        # extract the values of the 5 different tags
        title = item.title.get_text(strip=True)
        tags = item.tags.get_text(strip=True)
        location = item.location.get_text(strip=True)
        pubDate = item.pubDate.get_text(strip=True)
        link = item.link.get_text(strip=True)
        #add them to a list of dic
        items_new.append({'TITLE': title, 'TAGS': tags, 'LOC': location, 'PUB_DATE': pubDate, 'LINK': link})
        # df...
        df = pd.DataFrame(items_new, index=range(1, len(items_new)+1))
        # Set the maximum column width to 1000 -> to avoid pd to truncate the URL
        pd.set_option('display.max_colwidth', 1000)
        # add the values to the list
        Worldwide_jobs_NOTSAVED = df[df['location'].str.contains('Worldwide', case=False)]
        Worldwide_LATAM_jobs_NOTSAVED = df[(df['location'].str.contains('Worldwide', case=False)) | (df['location'].str.contains('LATAM', case=False))]
        DF_SAVED = df.to_csv('REMOTE_OK.csv', index=False)
        Worldwide_jobs = Worldwide_jobs_NOTSAVED.to_csv('REMOTE_OK_worldwide.csv', index=False)
        Worldwide_LATAM = Worldwide_LATAM_jobs_NOTSAVED.to_csv('REMOTE_OK_Worldwide_LATAM.csv', index=False)
    return DF_SAVED, Worldwide_jobs, Worldwide_LATAM

print(REMOTE_OK())