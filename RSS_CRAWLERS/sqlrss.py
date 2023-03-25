import pandas as pd
import bs4 
from urllib.error import HTTPError
import lxml
import re
import psycopg2
import numpy as np
import pretty_errors

def pipeline():
    df = pd.read_csv('./OUTPUTS/yummy_soup_rss.csv')


    # Fill missing values with "NaN"
    df.fillna("NaN", inplace=True)


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

        ## PostgreSQL#df = pd.read_csv('./OUTPUTS/yummy_soup_rss.csv')


    print("\n", f"Fetching {len(df)} cleaned jobs to PostgreSQL...", "\n")

    # create a connection to the PostgreSQL database
    cnx = psycopg2.connect(user='postgres', password='3312', host='localhost', port='5432', database='postgres')

    # create a cursor object
    cursor = cnx.cursor()

    # prepare the SQL query to create a new table
    create_table_query = '''
        CREATE TABLE IF NOT EXISTS _rss (
            title VARCHAR(255),
            link VARCHAR(255),
            description VARCHAR(1000),
            pubdate TIMESTAMP,
            location VARCHAR(255),
            PRIMARY KEY (title, link)
        )
    '''

    # execute the create table query
    cursor.execute(create_table_query)

    # insert the DataFrame into the PostgreSQL database using an upsert strategy
    for index, row in df.iterrows():
        insert_query = '''
            INSERT INTO _rss (title, link, description, pubdate, location)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (title, link)
            DO UPDATE SET
                description = EXCLUDED.description,
                pubdate = EXCLUDED.pubdate,
                location = EXCLUDED.location
        '''
        values = (row['title'], row['link'], row['description'], row['pubdate'], row['location'])
        cursor.execute(insert_query, values)

    # commit the changes to the database
    cnx.commit()

    # close the cursor and connection
    cursor.close()
    cnx.close()
    print("\n", "JOBS ADDED INTO POSTGRESQL! :)", "\n")

if __name__ == "__main__":
    pipeline()
