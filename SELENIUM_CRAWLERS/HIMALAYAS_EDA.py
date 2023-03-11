#!/usr/bin/env python
# coding: utf-8

# # HIMALAYAS

# ### In brief...

# ### For every iteration, the df is inspected and then filtered by its pubDate (if there's one). Finally, it creates a new df that will be transformed into a new table in the local postgreSQL DB.

# In[1]:


import pandas as pd
import psycopg2
import numpy as np
from dateutil.relativedelta import relativedelta
from dateutil.parser import parse


# In[2]:


df = pd.read_csv('/Users/juanreyesgarcia/Library/CloudStorage/OneDrive-FundacionUniversidaddelasAmericasPuebla/DEVELOPER/PROJECTS/CRAWLER_ALL/OUTPUTS/himalaya.csv')


# ##### Play with the settings...

# In[3]:


pd.set_option('display.max_colwidth', 150)


# In[4]:


pd.set_option("display.max_rows", None)


# # EDA Checklist 

# In[5]:


df[:100]


# In[6]:


df.tail()


# ### Describe the DF (current)

# In[7]:


df.describe()


# ### Checking for missing values DF (current)

# In[8]:


df.isnull().sum()


# ### Fill missing values with "NaN" DF (current)

# In[9]:


# Fill missing values with "NaN"
df.fillna("NaN", inplace=True)


# In[10]:


df.isnull().sum()


# ### Checking for duplicates DF (current)

# In[11]:


df.duplicated().sum()


# In[12]:


df.describe()


# In[13]:


df.dtypes


# ### Mask to convert %minutes|hours to 1 day ago so it can be parse to date time (cba to say it was posted today, maybe later should be fixed)

# In[14]:


mask = df['pubdate'].str.contains('minutes|hour%')


# In[15]:


df.loc[mask, 'pubdate'] = "1 day ago"


# In[16]:


df.head()


# # From relative date strings to date time...

# In[17]:


# create a mask to identify rows with relative date strings
mask = df['pubdate'].str.contains('ago')

# apply the relative date calculation to the date_string column
df.loc[mask, 'datetime'] = pd.Timestamp.today() - df.loc[mask, 'pubdate'].apply(lambda x: relativedelta(days=int(x.split()[0])))

# convert the datetime column to datetime format
df['datetime'] = pd.to_datetime(df['datetime'], infer_datetime_format=True)


# In[18]:


df.head(50)


# ### Drop pubDate, change name of datetime & reindex...

# In[19]:


#Drop it
df = df.drop('pubdate', axis=1)


# In[20]:


#Change name of datetime to pubDate 
df = df.rename(columns={'datetime': 'pubdate'})


# In[21]:


#reindex
df = df.reindex(columns=['title', 'link', 'description', 'pubdate', 'location'])


# In[22]:


df.head()


# ## Filter rows by a date range (this reduces the number of rows... duh)

# In[23]:


# filter rows by a date range
start_date = pd.to_datetime('2016-01-01')
end_date = pd.to_datetime('2023-02-15')

#for df
date_range_filter = (df['pubdate'] >= start_date) & (df['pubdate'] <= end_date)
df = df.loc[~date_range_filter]


# In[24]:


df = df.sort_values(by='pubdate')


# In[25]:


df.describe(datetime_is_numeric=True)


# In[26]:


df.describe()


# In[27]:


df.dtypes


# ### Make a copy just in case

# In[28]:


df1 = df.copy()


# In[29]:


# replace NaT values in the DataFrame with None
df = df.replace({np.nan: None, pd.NaT: None})


# In[30]:


df


# # PostgreSQL

# ### This code creates a new table per iteration

# In[31]:


# create a connection to the PostgreSQL database
cnx = psycopg2.connect(user='postgres', password='3312', host='localhost', port='5432', database='postgres')

# create a cursor object
cursor = cnx.cursor()

# get the name of the next table to create
get_table_name_query = '''
    SELECT COUNT(*) FROM information_schema.tables
    WHERE table_name LIKE 'himalayas_%'
'''
cursor.execute(get_table_name_query)
result = cursor.fetchone()
next_table_number = result[0] + 1
next_table_name = 'himalayas_{}'.format(next_table_number)

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

