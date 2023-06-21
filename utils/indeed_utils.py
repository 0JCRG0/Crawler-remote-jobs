import asyncio
import logging
from sql.clean_loc import clean_location_rows
from utils.handy import *

def clean_postgre_indeed(df, S, Q, c_code):
	df.to_csv(S, index=False)

	for col in df.columns:
		if col == 'description':
			df[col] = df[col].apply(cleansing_selenium_crawlers)
			#df[col] = ["{MX}".format(i) for i in df[col]]
		elif col == 'location':
			df[col] = df[col] + " " + str(c_code)
	
	logging.info('Finished Indeed. Results below ⬇︎')

	Q(df)
