import asyncio
import logging
from sql.clean_loc import clean_location_rows
from utils.handy import *




def clean_postgre_sel(df, csv_path, db):
    
    """ CLEANING AVOIDING DEPRECATION WARNING """
    for col in df.columns:
        if col != 'pubdate':
            i = df.columns.get_loc(col)
            newvals = df.loc[:, col].astype(str).apply(cleansing_selenium_crawlers)
            df[df.columns[i]] = newvals
    
    for col in df.columns:
        if col == 'location':
            i = df.columns.get_loc(col)
            newvals = df.loc[:, col].astype(str).apply(clean_location_rows)
            df[df.columns[i]] = newvals
        
    #Save it in local machine
    df.to_csv(csv_path, index=False)

    #Log it 
    logging.info('Finished Selenium_Crawlers. Results below ⬇︎')
    
    # SEND IT TO TO PostgreSQL    
    db(df)
