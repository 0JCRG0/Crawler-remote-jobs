import csv
import requests
import pandas as pd
import bs4 
from urllib.error import HTTPError
import lxml
import re
         
                
file = '/Users/juanreyesgarcia/Library/CloudStorage/OneDrive-FundacionUniversidaddelasAmericasPuebla/DEVELOPER/PROJECTS/CRAWLER_ALL/RSS_CRAWLERS/remote-working-resources.csv'
output = []

def COOK_SOUP():
    soup_list = []
    with open(file, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            if 'Feed URL' in row and row['Feed URL']:
                rss = row['Feed URL']
                try:
                    res = requests.get(rss)
                    if res.status_code != 200:
                        print(f"Received non-200 response ({res.status_code}) for URL {rss}. Skipping...")
                        continue
                    res.raise_for_status()
                    soup = bs4.BeautifulSoup(res.text, 'lxml-xml')
                    soup_list.append(soup)
                except HTTPError as e:
                    if e.code == 403:
                        print(f"An error occurred: {e}. Skipping URL {rss}")
                        rss = None
                    else:
                        raise
    return soup_list

soup_list = COOK_SOUP()
print(f"Number of soup objects: {len(soup_list)}")

def GET_ELEMENTS():
    rows = []
    for soup in soup_list:
        for item in soup.find_all('item'):
            title = str(item.title.get_text(strip=True))
            link = str(item.link.get_text(strip=True))
            pubDate = item.find('pubDate')
            if pubDate is not None:
                pubDate = str(item.pubDate.get_text(strip=True))
            location = item.find('location')
            if location is not None:
                location = str(item.location.get_text(strip=True))
            description = item.find('description')
            if description is not None:
                description = str(item.description.get_text(strip=True))
            row = {'title':title, 'link':link, 'pubdate': pubDate, 'location': location, 'description': description}
            rows.append(row)
    return rows


dirty_soup = GET_ELEMENTS()

num_rows = len(dirty_soup)
print(dirty_soup[30:51])
print(f"Number of rows: {num_rows}")


def CLEAN_LINK(s):
    # Remove leading/trailing white space
    s = s.strip()
    
    # Replace multiple spaces with a single space
    s = re.sub(r'\s+', ' ', s)
    
    # Remove newline characters
    s = re.sub(r'\n', '', s)
    
    return s


def CLEAN_OTHER(s):
    # Remove leading/trailing white space
    s = s.strip()
    
    # Replace multiple spaces with a single space
    s = re.sub(r'\s+', ' ', s)
    
    # Remove newline characters
    s = re.sub(r'\n', '', s)
    
    # Remove HTML tags
    s = re.sub(r'<.*?>', '', s)
    
    # Remove non-alphanumeric characters (except for spaces)
    s = re.sub(r'[^a-zA-Z0-9\s]+', '', s)
    
    # Remove symbols
    s = re.sub(r'[-–—•@Ôªø]+', '', s)
        
    return s


def CLEAN():
    df = pd.DataFrame()
    curated_rows = []
    for dic in dirty_soup:
        row = {}
        for key,val in dic.items():
            if key == 'link':
                row[key] = CLEAN_LINK(val)
            elif val is None:
                continue
            else:
                row[key] = CLEAN_OTHER(val)
        curated_rows.append(row)
        df = pd.concat([pd.DataFrame(curated_rows)])
        df.to_csv('yummy_soup_rss.csv', index=False)
    return df

CLEAN()

