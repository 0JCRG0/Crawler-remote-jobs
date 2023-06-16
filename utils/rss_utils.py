import logging
from sql.clean_loc import clean_location_rows
from utils.handy import *

def clean_postgre_rss(df, csv_path, db):
    #Cleaning columns
    for col in df.columns:
        if col == 'location':
            df[col] = df[col].apply(clean_location_rows)
        elif col == 'description':
            df[col] = df[col].apply(clean_other_rss)
    #Save it in local machine
    df.to_csv(csv_path, index=False)

    #Log it 
    logging.info('Finished RSS Reader. Results below ⬇︎')
    
    # SEND IT TO TO PostgreSQL    
    db(df)