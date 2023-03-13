#! python3

import requests
import sys
import pandas as pd
import bs4
import lxml
import pretty_errors
import psycopg2
# CRITICAL!!! -> THIS IS ONLY FOR XML!!! NOT HTTP PARSERS...

rss = rss_remote_ok = 'https://remoteok.com/rss'
print("\n", f'Crawling...{rss}', "\n")

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
        items_new.append({'title': title, 'link': link, 'pubdate': pubDate, 'location': location, 'description': tags})
        # df...
        df = pd.DataFrame(items_new, index=range(1, len(items_new)+1))
        # Set the maximum column width to 1000 -> to avoid pd to truncate the URL
        pd.set_option('display.max_colwidth', 1000)
    return df

df = REMOTE_OK()

directory = "./OUTPUTS/"
df.to_csv(f'{directory}REMOTE_OK.csv', index=False)

print("\n", f"Fetching {len(df)} cleaned jobs to PostgreSQL...", "\n")

def PIPELINE():
    directory = "./OUTPUTS/"
    df = pd.read_csv(f'{directory}REMOTE_OK.csv')
    
    ## PostgreSQL

    # This code creates a new table per iteration

    # create a connection to the PostgreSQL database
    cnx = psycopg2.connect(user='postgres', password='3312', host='localhost', port='5432', database='postgres')

    # create a cursor object
    cursor = cnx.cursor()

    # get the name of the next table to create
    get_table_name_query = '''
        SELECT COUNT(*) FROM information_schema.tables
        WHERE table_name LIKE 'REMOTE_OK_%'
    '''
    cursor.execute(get_table_name_query)
    # execute the query to get the count of existing tables

    # fetch the first row of the query results
    result = cursor.fetchone()

    # get the number of existing tables if there are any, otherwise set it to 0
    next_table_number = result[0] + 1 if result is not None else 0
    next_table_name = 'REMOTE_OK_{}'.format(next_table_number)

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

    print("\n", "JOBS ADDED INTO POSTGRESQL! :)", "\n")

PIPELINE()