import asyncio
import logging
from sql.clean_loc import clean_location_rows
from utils.handy import *



def clean_postgre_bs4(df, S, Q):
	#df = pd.DataFrame(data) # type: ignore
	"""data_dic = dict(data) # type: ignore
	df = pd.DataFrame.from_dict(data_dic, orient='index')
	df = df.transpose()"""

		# count the number of duplicate rows
	num_duplicates = df.duplicated().sum()

			# print the number of duplicate rows
	print("Number of duplicate rows:", num_duplicates)

# remove duplicate rows based on all columns
	df = df.drop_duplicates()
		
		#CLEANING AVOIDING DEPRECATION WARNING
	for col in df.columns:
		if col == 'title' or col == 'description':
			i = df.columns.get_loc(col)
			newvals = df.loc[:, col].astype(str).apply(cleansing_selenium_crawlers)
			df[df.columns[i]] = newvals
		elif col == 'location':
			i = df.columns.get_loc(col)
			newvals = df.loc[:, col].astype(str).apply(cleansing_selenium_crawlers)
			df[df.columns[i]] = newvals
			i = df.columns.get_loc(col)
			newvals = df.loc[:, col].astype(str).apply(clean_location_rows)
			df[df.columns[i]] = newvals

		
		#Save it in local machine
	df.to_csv(S, index=False)

		#Log it 
	logging.info('Finished bs4 crawlers. Results below ⬇︎')
		
		# SEND IT TO TO PostgreSQL    
	Q(df)

		#print the time
	#elapsed_time = asyncio.get_event_loop().time() - start_time

	print("\n")
	#print(f"BS4 crawlers finished! all in: {elapsed_time:.2f} seconds.", "\n")