#! python3
# searchpypi.py  - Opens several search results.
import requests
import bs4
import sys
import pandas as pd
import re
import psycopg2
import pretty_errors

def WWR():
    print("\n", "Crawler deployed... ", "\n")
    num_pages = 6
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
        jobs += [{"title": title, "link": link, "pubdate": pubdate, "location": location, "description": " "} for title, link, pubdate, location in zip(titles_all, links_all, pubdates_all, locations_all)]
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

    # This code creates a new table per iteration
    ## Create a connection to the PostgreSQL database
    cnx = psycopg2.connect(user='postgres', password='3312', host='localhost', port='5432', database='postgres')

    # create a cursor object
    cursor = cnx.cursor()

    # Get the name of the next table to create
    get_table_name_query = '''
        SELECT COUNT(*) FROM information_schema.tables
        WHERE table_name LIKE 'WWR_%'
    '''
    cursor.execute(get_table_name_query)
    # execute the query to get the count of existing tables

    # fetch the first row of the query results
    result = cursor.fetchone()

    # get the number of existing tables if there are any, otherwise set it to 0
    next_table_number = result[0] + 1 if result is not None else 0
    next_table_name = 'WWR_{}'.format(next_table_number)

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
    print("\n", "ALL DONE! GO TO POSTGRESQL FOR FURTHER PROCESSING :)", "\n")
WWR()