import requests
import json
import pretty_errors
import pandas as pd
import psycopg2

def W_NOMADS_API():
    api = "https://www.workingnomads.com/api/exposed_jobs/"
    print("\n", f"Fetching...{api}", "\n")
    response = requests.get(api)
    if response.status_code == 200:
        data = json.loads(response.text)
        #pretty_json = json.dumps(data, indent=4)
        #print(pretty_json, type(data))
        
        #Loop through the list of dict            
        all_jobs = []
        titles = []
        links = []
        pubdates = []
        locations = []
        descriptions = []
        for job in data:
            titles.append(job["title"])
            links.append(job["url"])
            pubdates.append(job["pub_date"])
            locations.append(job["location"])
            descriptions.append(job["tags"])
            #Put it all together...
            all_jobs = {'title': titles, 'link':links, 'pubdate': pubdates, 'location': locations, 'description': descriptions}
        return all_jobs
    else:
        print("Error connecting to API:", response.status_code)

#to df
df = pd.DataFrame(W_NOMADS_API())
print("\n", f"Successfully fetched {len(df)} jobs", "\n")

print("\n", "Saving jobs into local machine as a CSV...", "\n")

directory = "./OUTPUTS/"
df.to_csv(f"{directory}W_NOMADS.csv", index=False)

print("\n", f"Fetching {len(df)} cleaned jobs to PostgreSQL...", "\n")

## PostgreSQL

# This code creates a new table per iteration

# create a connection to the PostgreSQL database
cnx = psycopg2.connect(user='postgres', password='3312', host='localhost', port='5432', database='postgres')

# create a cursor object
cursor = cnx.cursor()

# get the name of the next table to create
get_table_name_query = '''
    SELECT COUNT(*) FROM information_schema.tables
    WHERE table_name LIKE 'W_NOMADS_%'
'''
cursor.execute(get_table_name_query)
# execute the query to get the count of existing tables

# fetch the first row of the query results
result = cursor.fetchone()

# get the number of existing tables if there are any, otherwise set it to 0
next_table_number = result[0] + 1 if result is not None else 0
next_table_name = 'W_NOMADS_{}'.format(next_table_number)

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