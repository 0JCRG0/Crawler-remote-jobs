import re
import psycopg2

def clean_rows(s):
    if not isinstance(s, str):
        return s
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

def send_postgre(df):
        # create a connection to the PostgreSQL database
        cnx = psycopg2.connect(user='postgres', password='3312', host='localhost', port='5432', database='postgres')

        # create a cursor object
        cursor = cnx.cursor()

        # prepare the SQL query to create a new table
        create_table_query = '''
            CREATE TABLE IF NOT EXISTS master_jobs (
                title VARCHAR(255),
                link VARCHAR(255) PRIMARY KEY,
                description VARCHAR(2000),
                pubdate TIMESTAMP,
                location VARCHAR(255)
            )
        '''

        # execute the create table query
        cursor.execute(create_table_query)

        print("\n", "Inserting jobs into PostgreSQL using an upsert strategy...", "\n")

        # insert the DataFrame into the PostgreSQL database using an upsert strategy
        for index, row in df.iterrows():
            insert_query = '''
                INSERT INTO master_jobs (title, link, description, pubdate, location)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (link) DO UPDATE SET
                    title = excluded.title,
                    description = excluded.description,
                    pubdate = excluded.pubdate,
                    location = excluded.location
            '''
            values = (row['title'], row['link'], row['description'], row['pubdate'], row['location'])
            cursor.execute(insert_query, values)

        count_query = '''
            SELECT COUNT(*) FROM master_jobs
        '''
        # execute the count query and retrieve the result
        cursor.execute(count_query)
        result = cursor.fetchone()

        # check if the result set is not empty
        if result is not None:
            count = result[0]
            print("\n", "DONE.", "\n", "\n", f"Current total count of jobs in PostgreSQL: {count}", "\n")
        
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
                title VARCHAR(255),
                link VARCHAR(255) PRIMARY KEY,
                description VARCHAR(2000),
                pubdate TIMESTAMP,
                location VARCHAR(255)
            )
        '''

        # execute the create table query
        cursor.execute(create_table_query)

        print("\n", "Inserting jobs into PostgreSQL using an upsert strategy...", "\n")

        # insert the DataFrame into the PostgreSQL database using an upsert strategy
        for index, row in df.iterrows():
            insert_query = '''
                INSERT INTO test (title, link, description, pubdate, location)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (link) DO UPDATE SET
                    title = excluded.title,
                    description = excluded.description,
                    pubdate = excluded.pubdate,
                    location = excluded.location
            '''
            values = (row['title'], row['link'], row['description'], row['pubdate'], row['location'])
            cursor.execute(insert_query, values)

        count_query = '''
            SELECT COUNT(*) FROM test
        '''
        # execute the count query and retrieve the result
        cursor.execute(count_query)
        result = cursor.fetchone()

        # check if the result set is not empty
        if result is not None:
            count = result[0]
            print("\n", "DONE.", "\n", "\n", f"Current total count of jobs in PostgreSQL: {count}", "\n")
        
        # commit the changes to the database
        cnx.commit()

        # close the cursor and connection
        cursor.close()
        cnx.close()
