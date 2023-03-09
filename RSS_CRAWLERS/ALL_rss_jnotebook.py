#!/usr/bin/env python
# coding: utf-8

# # ALL_RSS

# ## Given that RSS FEEDs update frequently we need to find a way to include the new jobs into the current DB without losing the "old" jobs.

# ### In brief...

# ### For every iteration, the df is inspected and then filtered by its pubDate (if there's one). Finally, it creates a new df that will be transformed into a new table in the local postgreSQL DB.

import pandas as pd
import matplotlib.pyplot as plt 
import psycopg2
import numpy as np
import pretty_errors

def PIPELINE():
    df = pd.read_csv('/Users/juanreyesgarcia/Library/CloudStorage/OneDrive-FundacionUniversidaddelasAmericasPuebla/DEVELOPER/PROJECTS/CRAWLER_ALL/OUTPUTS/yummy_soup_rss.csv')

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
    # ## Filter rows by a date range (this reduces the number of rows... duh)


    # filter rows by a date range
    start_date = pd.to_datetime('2016-01-01')
    end_date = pd.to_datetime('2023-02-15')

    #for df
    date_range_filter = (df['pubdate'] >= start_date) & (df['pubdate'] <= end_date)
    df = df.loc[~date_range_filter]

    df = df.sort_values(by='pubdate')


    # ##### Make a copy 

    df['description'] = df['description'].str.slice(0, 1000)

    # replace NaT values in the DataFrame with None
    df = df.replace({np.nan: None, pd.NaT: None})

    ## PostgreSQL

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
