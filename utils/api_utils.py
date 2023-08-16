import asyncio
import logging
from sql.clean_loc import clean_location_rows, convert_names_to_codes
from utils.handy import *

def clean_postgre_api(df, csv_path, db):

    """ Clean the rows accordingly """
    for col in df.columns:
        if col == 'location':
            #df[col] = df[col].astype(str).str.replace(r'{}', '', regex=True)
            df[col] = df[col].astype(str).apply(clean_rows)
            df[col] = df[col].apply(clean_location_rows)
            df[col] = df[col].apply(convert_names_to_codes)
        if col == 'description':
            df[col] = df[col].astype(str).apply(clean_rows).apply(cleansing_selenium_crawlers)


    df.to_csv(csv_path, index=False)

    #Log
    logging.info('Finished API crawlers. Results below ⬇︎')

    ## PostgreSQL
    db(df)

