import csv
import psycopg2

# Connect to the PostgreSQL database
conn = psycopg2.connect(user='postgres', password='3312', host='localhost', port='5432', database='postgres')
    

# Create a cursor object to execute SQL queries
cursor = conn.cursor()

# Execute the query
query = """
SELECT DISTINCT location
FROM main_jobs
"""

cursor.execute(query)

# Fetch all rows from the result set
rows = cursor.fetchall()

# Define the path and filename for the CSV file


csv_filename = "/Users/juanreyesgarcia/Library/CloudStorage/OneDrive-FundacionUniversidaddelasAmericasPuebla/DEVELOPER/PROJECTS/CRAWLER_ALL/download/sql_distinct.csv"

# Write the data to the CSV file
with open(csv_filename, mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow([desc[0] for desc in cursor.description])  # Write the column headers
    writer.writerows(rows)  # Write the data rows

# Close the cursor and database connection
cursor.close()
conn.close()

print(f"Data exported to {csv_filename}")


