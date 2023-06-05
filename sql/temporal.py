import psycopg2
from utils.handy import *

#print(unique_ids)


def update_ids_in_table(unique_ids):
    cnx = psycopg2.connect(user='postgres', password='3312', host='localhost', port='5432', database='postgres')
    cursor = cnx.cursor()

    cursor.execute("ALTER TABLE master_jobs_test ADD COLUMN IF NOT EXISTS id text;")

    # Create a temporary table and insert the unique_ids into it
    cursor.execute("CREATE TEMPORARY TABLE temp_ids (id text, row_num serial);")
    cursor.executemany("INSERT INTO temp_ids (id) VALUES (%s);", [(uid,) for uid in unique_ids])

    # Update the 'id' column in the original table using a join with the temporary table
    cursor.execute("""
        UPDATE master_jobs_test
        SET id = temp_ids.id
        FROM (
            SELECT link, row_number() OVER (ORDER BY link) AS row_num
            FROM master_jobs_test
        ) original
        JOIN temp_ids ON original.row_num = temp_ids.row_num
        WHERE master_jobs_test.link = original.link;
    """)

    cnx.commit()
    cursor.close()
    cnx.close()

unique_ids = [id_generator(5) for _ in range(7496)]
update_ids_in_table(unique_ids)