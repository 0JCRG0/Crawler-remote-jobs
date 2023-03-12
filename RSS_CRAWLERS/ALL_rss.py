import csv
import requests
import pandas as pd
import bs4 
from urllib.error import HTTPError
import lxml
import re
import psycopg2
import numpy as np
import pretty_errors
                
file = '/Users/juanreyesgarcia/Library/CloudStorage/OneDrive-FundacionUniversidaddelasAmericasPuebla/DEVELOPER/PROJECTS/CRAWLER_ALL/RSS_CRAWLERS/remote-working-resources.csv'

print("\n", f"Crawler deployed... ", "\n")

#TODO: RUN THIS CODE TOMORROW AND TRY THE NEW TRIGGER // ADD THE DROP STATEMENT?
#TODO: MAKE THE SCRIPT MORE COHESIVE - ONE SINGLE FUNCTION.

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


print("\n", f"Established connection with {len(COOK_SOUP())} websites. Finding elements now...", "\n")

def GET_ELEMENTS():
    rows = []
    for soup in COOK_SOUP():
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


print("\n", f"Crawler successfully found {len(GET_ELEMENTS())} jobs...", "\n")


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

print("\n", "Preprocessing the obtained jobs...", "\n")

def CLEAN():
    df = pd.DataFrame()
    curated_rows = []
    for dic in GET_ELEMENTS():
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
        directory = "./OUTPUTS/"
        df.to_csv(f'{directory}yummy_soup_rss.csv', index=False)
    return df

print("\n", "Jobs have been saved into local machine as a CSV & converted into a df for further cleansing...", "\n")
print("\n", "Data cleansing with pandas has started...", "\n")

def PIPELINE():
    #df = pd.read_csv('/Users/juanreyesgarcia/Library/CloudStorage/OneDrive-FundacionUniversidaddelasAmericasPuebla/DEVELOPER/PROJECTS/CRAWLER_ALL/OUTPUTS/yummy_soup_rss.csv')
    df = CLEAN()
    # Play with the settings...

    pd.set_option('display.max_colwidth', 150)
    pd.set_option("display.max_rows", None)

    # Fill missing values with "NaN"
    df.fillna("NaN", inplace=True)


    #From string to date...
    #df.loc[:, 'pubdate']  = pd.to_datetime(df['pubdate'], errors="coerce", format="%a %d %b %Y", exact=False)

    # convert the pubdate column to a datetime object
    for i in range(len(df.columns)):
        if df.columns[i] == 'pubdate':
            df[df.columns[i]] = pd.to_datetime(df[df.columns[i]], errors="coerce", format="%a %d %b %Y", exact=False)
    #Filter rows by a date range (this reduces the number of rows... duh)

    start_date = pd.to_datetime('2016-01-01')
    end_date = pd.to_datetime('2023-02-15')
    date_range_filter = (df['pubdate'] >= start_date) & (df['pubdate'] <= end_date)
    df = df.loc[~date_range_filter]

    #sort the values
    df = df.sort_values(by='pubdate')


    # Reduce the lenght of description... 
    df['description'] = df['description'].str.slice(0, 1000)

    # replace NaT values in the DataFrame with None
    df = df.replace({np.nan: None, pd.NaT: None})

    ## PostgreSQL

    print("\n", f"Fetching {len(df)} cleaned jobs to PostgreSQL...", "\n")

    # This code creates a new table per iteration

    # create a connection to the PostgreSQL database
    cnx = psycopg2.connect(user='postgres', password='3312', host='localhost', port='5432', database='postgres')

    # create a cursor object
    cursor = cnx.cursor()

    # get the name of the next table to create
    get_table_name_query = '''
        SELECT COUNT(*) FROM information_schema.tables
        WHERE table_name LIKE 'rss_%'
    '''
    cursor.execute(get_table_name_query)
    # execute the query to get the count of existing tables

    # fetch the first row of the query results
    result = cursor.fetchone()

    # get the number of existing tables if there are any, otherwise set it to 0
    next_table_number = result[0] + 1 if result is not None else 0
    next_table_name = 'rss_{}'.format(next_table_number)

    # prepare the SQL query to create a new table
    create_table_query = '''
        CREATE TABLE {} (
            title VARCHAR(255),
            link VARCHAR(255),
            description VARCHAR(1000),
            pubdate TIMESTAMP,
            location VARCHAR(255)
        )
    '''.format(next_table_name)

    # execute the create table query
    cursor.execute(create_table_query)

    # insert the DataFrame into the PostgreSQL database as a new table
    for index, row in df.iterrows():
        insert_query = '''
            INSERT INTO {} (title, link, description, pubdate, location)
            VALUES (%s, %s, %s, %s, %s)
        '''.format(next_table_name)
        values = (row['title'], row['link'], row['description'], row['pubdate'], row['location'])
        cursor.execute(insert_query, values)

    # commit the changes to the database
    cnx.commit()

    # close the cursor and connection
    cursor.close()
    cnx.close()
PIPELINE()

print("\n", "JOBS ADDED INTO POSTGRESQL! :)", "\n")
