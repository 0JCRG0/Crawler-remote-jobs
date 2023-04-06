import re
import psycopg2

#CLEANING FUNCTIONS FOR SELENIUM
def clean_rows(s):
    if not isinstance(s, str):
        return s
    s = re.sub(r'n/', '', s)
    s = re.sub(r'"', '', s)
    s = re.sub(r'{', '', s)
    s = re.sub(r'}', '', s)
    s = re.sub(r'[\[\]]', '', s)
    s = re.sub(r"'", '', s)
    return s

def cleansing_selenium_crawlers(s):
    if not isinstance(s, str):
        print(f"{s} is not a string! Returning unmodified")
        return s
    s = " ".join(s.split())
    s = re.sub(r'<[^>]+>', '', s)
    s = re.sub(r'n/', '', s)
    s = re.sub(r'"', '', s)
    s = re.sub(r'{', '', s)
    s = re.sub(r'}', '', s)
    s = re.sub(r'[\[\]]', '', s)
    s = re.sub(r"'", '', s)
    return s

def initial_clean(s):
    s = " ".join(s.split())
    s = re.sub(r'n/', '', s)
    return s

    # Handy cleansing function

def bye_regex(s):
    # Remove leading/trailing white space
    s = s.strip()
        # Replace multiple spaces with a single space
    s = re.sub(r'\s+', ' ', s)
        # Remove newline characters
    s = re.sub(r'\n', '', s)
        # Replace regex for í
    s = re.sub(r'√≠', 'í', s)
        # Replace word
    s = re.sub(r'Posted', '', s)
        # Remove HTML tags
    s = re.sub(r'<.*?>', '', s)

#CLEANING FUNCTIONS FOR RSS' crawlers
def clean_link_rss(s):
    # Remove leading/trailing white space
    s = s.strip()
        
    # Replace multiple spaces with a single space
    s = re.sub(r'\s+', ' ', s)
        
    # Remove newline characters
    s = re.sub(r'\n', '', s)
        
    return s

def clean_other_rss(s):
    if not isinstance(s, str):
        return s
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

#FUNCTIONS TO SEND A DF TO POSTGRESQL
def mx_postgre(df):
    # create a connection to the PostgreSQL database
    cnx = psycopg2.connect(user='postgres', password='3312', host='localhost', port='5432', database='postgres')

    # create a cursor object
    cursor = cnx.cursor()

    # prepare the SQL query to create a new table
    create_table_query = '''
        CREATE TABLE IF NOT EXISTS mx_jobs (
            title VARCHAR(1000),
            link VARCHAR(1000),
            description VARCHAR(2000),
            pubdate TIMESTAMP,
            location VARCHAR(1000),
            PRIMARY KEY (link),
            UNIQUE (title, link)
        )
    '''

    # execute the create table query
    cursor.execute(create_table_query)

    # execute the initial count query and retrieve the result
    initial_count_query = '''
        SELECT COUNT(*) FROM mx_jobs
    '''
    cursor.execute(initial_count_query)
    initial_count_result = cursor.fetchone()

    # insert the DataFrame into the PostgreSQL database using an upsert strategy
    jobs_added = []
    for index, row in df.iterrows():
        insert_query = '''
            INSERT INTO mx_jobs (title, link, description, pubdate, location)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (link, title) DO UPDATE SET
                title = excluded.title,
                description = excluded.description,
                pubdate = excluded.pubdate,
                location = excluded.location
            RETURNING *
        '''
        values = (row['title'], row['link'], row['description'], row['pubdate'], row['location'])
        cursor.execute(insert_query, values)
        if cursor.rowcount > 0:
            jobs_added.append(cursor.fetchone())

    final_count_query = '''
        SELECT COUNT(*) FROM mx_jobs
    '''
    # execute the count query and retrieve the result
    cursor.execute(final_count_query)
    final_count_result = cursor.fetchone()

    # calculate the number of unique jobs that were added
    if initial_count_result is not None:
        initial_count = initial_count_result[0]
    else:
        initial_count = 0
    jobs_added_count = len(jobs_added)
    if final_count_result is not None:
        final_count = final_count_result[0]
    else:
        final_count = 0
    unique_jobs = final_count - initial_count

    # check if the result set is not empty
    print("\n")
    print("FINAL REPORT:", "\n")
    print(f"Total count of jobs before crawling: {initial_count}")
    print(f"Total number of jobs obtained by crawling: {jobs_added_count}")
    print(f"Total number of unique jobs added: {unique_jobs}")
    print(f"Current total count of jobs in PostgreSQL: {final_count}")

    # commit the changes to the database
    cnx.commit()

    # close the cursor and connection
    cursor.close()
    cnx.close()

def test_postgre(df):
    # create a connection to the PostgreSQL database
    cnx = psycopg2.connect(user='postgres', password='3312', host='localhost', port='5432', database='postgres')

    # create a cursor object
    cursor = cnx.cursor()

    # prepare the SQL query to create a new table
    create_table_query = '''
        CREATE TABLE IF NOT EXISTS test (
            title VARCHAR(1000),
            link VARCHAR(1000) PRIMARY KEY,
            description VARCHAR(2000),
            pubdate TIMESTAMP,
            location VARCHAR(1000)
        )
    '''

    # execute the create table query
    cursor.execute(create_table_query)

    # execute the initial count query and retrieve the result
    initial_count_query = '''
        SELECT COUNT(*) FROM test
    '''
    cursor.execute(initial_count_query)
    initial_count_result = cursor.fetchone()

    # insert the DataFrame into the PostgreSQL database using an upsert strategy
    jobs_added = []
    for index, row in df.iterrows():
        insert_query = '''
            INSERT INTO test (title, link, description, pubdate, location)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (link) DO UPDATE SET
                title = excluded.title,
                description = excluded.description,
                pubdate = excluded.pubdate,
                location = excluded.location
            RETURNING *
        '''
        values = (row['title'], row['link'], row['description'], row['pubdate'], row['location'])
        cursor.execute(insert_query, values)
        if cursor.rowcount > 0:
            jobs_added.append(cursor.fetchone())

    final_count_query = '''
        SELECT COUNT(*) FROM test
    '''
    # execute the count query and retrieve the result
    cursor.execute(final_count_query)
    final_count_result = cursor.fetchone()

    # calculate the number of unique jobs that were added
    if initial_count_result is not None:
        initial_count = initial_count_result[0]
    else:
        initial_count = 0
    jobs_added_count = len(jobs_added)
    if final_count_result is not None:
        final_count = final_count_result[0]
    else:
        final_count = 0
    unique_jobs = final_count - initial_count

    # check if the result set is not empty
    print("\n")
    print("TEST RESULTS:", "\n")
    print(f"Total count of jobs before crawling: {initial_count}")
    print(f"Total number of jobs obtained by crawling: {jobs_added_count}")
    print(f"Total number of unique jobs added: {unique_jobs}")
    print(f"Current total count of jobs in PostgreSQL: {final_count}")

    # commit the changes to the database
    cnx.commit()

    # close the cursor and connection
    cursor.close()
    cnx.close()

def to_postgre(df):
    # create a connection to the PostgreSQL database
    cnx = psycopg2.connect(user='postgres', password='3312', host='localhost', port='5432', database='postgres')

    # create a cursor object
    cursor = cnx.cursor()

    # prepare the SQL query to create a new table
    create_table_query = '''
        CREATE TABLE IF NOT EXISTS master_jobs (
            title VARCHAR(1000),
            link VARCHAR(1000) PRIMARY KEY,
            description VARCHAR(2000),
            pubdate TIMESTAMP,
            location VARCHAR(1000)
        )
    '''

    # execute the create table query
    cursor.execute(create_table_query)

    # execute the initial count query and retrieve the result
    initial_count_query = '''
        SELECT COUNT(*) FROM master_jobs
    '''
    cursor.execute(initial_count_query)
    initial_count_result = cursor.fetchone()

    # insert the DataFrame into the PostgreSQL database using an upsert strategy
    jobs_added = []
    for index, row in df.iterrows():
        insert_query = '''
            INSERT INTO master_jobs (title, link, description, pubdate, location)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (link) DO UPDATE SET
                title = excluded.title,
                description = excluded.description,
                pubdate = excluded.pubdate,
                location = excluded.location
            RETURNING *
        '''
        values = (row['title'], row['link'], row['description'], row['pubdate'], row['location'])
        cursor.execute(insert_query, values)
        if cursor.rowcount > 0:
            jobs_added.append(cursor.fetchone())

    final_count_query = '''
        SELECT COUNT(*) FROM master_jobs
    '''
    # execute the count query and retrieve the result
    cursor.execute(final_count_query)
    final_count_result = cursor.fetchone()

    # calculate the number of unique jobs that were added
    if initial_count_result is not None:
        initial_count = initial_count_result[0]
    else:
        initial_count = 0
    jobs_added_count = len(jobs_added)
    if final_count_result is not None:
        final_count = final_count_result[0]
    else:
        final_count = 0
    unique_jobs = final_count - initial_count

    # check if the result set is not empty
    print("\n")
    print("FINAL REPORT:", "\n")
    print(f"Total count of jobs before crawling: {initial_count}")
    print(f"Total count of jobs found by crawling: {jobs_added_count}")
    print(f"Total count of unique jobs added: {unique_jobs}")
    print(f"Current total count of jobs in PostgreSQL: {final_count}")

    # commit the changes to the database
    cnx.commit()

    # close the cursor and connection
    cursor.close()
    cnx.close()

def freelance_postgre(df):
    # create a connection to the PostgreSQL database
    cnx = psycopg2.connect(user='postgres', password='3312', host='localhost', port='5432', database='postgres')

    # create a cursor object
    cursor = cnx.cursor()

    # prepare the SQL query to create a new table
    create_table_query = '''
        CREATE TABLE IF NOT EXISTS freelance (
            title VARCHAR(1000) PRIMARY KEY,
            link VARCHAR(1000),
            description VARCHAR(2000),
            pubdate TIMESTAMP,
            location VARCHAR(1000)
        )
    '''

    # execute the create table query
    cursor.execute(create_table_query)

    # execute the initial count query and retrieve the result
    initial_count_query = '''
        SELECT COUNT(*) FROM freelance
    '''
    cursor.execute(initial_count_query)
    initial_count_result = cursor.fetchone()

    # insert the DataFrame into the PostgreSQL database using an upsert strategy
    jobs_added = []
    for index, row in df.iterrows():
        insert_query = '''
            INSERT INTO freelance (title, link, description, pubdate, location)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (title) DO UPDATE SET
                link = excluded.link,
                description = excluded.description,
                pubdate = excluded.pubdate,
                location = excluded.location
            RETURNING *
        '''
        values = (row['title'], row['link'], row['description'], row['pubdate'], row['location'])
        cursor.execute(insert_query, values)
        if cursor.rowcount > 0:
            jobs_added.append(cursor.fetchone())

    final_count_query = '''
        SELECT COUNT(*) FROM freelance
    '''
    # execute the count query and retrieve the result
    cursor.execute(final_count_query)
    final_count_result = cursor.fetchone()

    # calculate the number of unique jobs that were added
    if initial_count_result is not None:
        initial_count = initial_count_result[0]
    else:
        initial_count = 0
    jobs_added_count = len(jobs_added)
    if final_count_result is not None:
        final_count = final_count_result[0]
    else:
        final_count = 0
    unique_jobs = final_count - initial_count

    # check if the result set is not empty
    print("\n")
    print("FREELANCE TABLE REPORT:", "\n")
    print(f"Total count of jobs before crawling: {initial_count}")
    print(f"Total number of jobs obtained by crawling: {jobs_added_count}")
    print(f"Total number of unique jobs added: {unique_jobs}")
    print(f"Current total count of jobs in PostgreSQL: {final_count}")

    # commit the changes to the database
    cnx.commit()

    # close the cursor and connection
    cursor.close()
    cnx.close()

#FUNCTION FOR APIs

## Function to choose class_json_strategy
def class_json_strategy(data, elements_path, class_json):
    """
    
    Given that some JSON requests are either
    dict or list we need to access the 1st dict if 
    needed
    
    """
    if class_json == "dict":                    
        # Access the key of the dictionary, which is a list of job postings
        jobs = data[elements_path["dict_tag"]]
        return jobs
    elif class_json == "list":
        jobs = data
        return jobs

## Function to clean api_pubdates
def api_pubdate(s):
    if s is not None:
        pubdate = s[0:10]
        return pubdate

#FUNCTIONS FOR FORMATTING PUBDATES IN RSS
def adby_pubdate(s):
    #slice the jobs that are formatted as %a %d %b %Y
    if s is not None:
        if not s.startswith("20"):
            s_sliced = s[0:15]
            return s_sliced

def YMD_pubdate(s):
    #slice the jobs that are formatted as %a %d %b %Y
    if s is not None:
        YMD_pub = s[0:8]
        return YMD_pub


#Cleaning function indeed
def indeed_regex(s):
        pattern = r'<li>(.*?)<\/li>'
        matches = re.findall(pattern, s, re.DOTALL)
        if len(matches) > 0:
            text = matches[0]
            text = re.sub(r'<b>|<\/b>', '', text)
            return text
        else:
            return ''