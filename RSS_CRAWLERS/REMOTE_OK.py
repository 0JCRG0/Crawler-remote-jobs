#! python3
# searchpypi.py  - Opens several search results.

import requests
import sys
import pandas as pd
import bs4
import lxml
import pretty_errors
# CRITICAL!!! -> THIS IS ONLY FOR XML!!! NOT HTTP PARSERS...

rss = rss_remote_ok = 'https://remoteok.com/rss'
print(f'Crawling...{rss}')

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
    df = pd.DataFrame()
    for item in make_soup().find_all('item'):
        # extract the values of the 5 different tags
        title = item.title.get_text(strip=True)
        tags = item.tags.get_text(strip=True)
        location = item.location.get_text(strip=True)
        pubDate = item.pubDate.get_text(strip=True)
        link = item.link.get_text(strip=True)
        #add them to a list of dic
        items_new.append({'title': title, 'description': tags, 'location': location, 'pubdate': pubDate, 'link': link})
        # df...
        df = pd.DataFrame(items_new, index=range(1, len(items_new)+1))
        # Set the maximum column width to 1000 -> to avoid pd to truncate the URL
        pd.set_option('display.max_colwidth', 1000)
    return df

df = REMOTE_OK()

directory = "./OUTPUTS/"
df.to_csv(f'{directory}REMOTE_OK.csv', index=False)
