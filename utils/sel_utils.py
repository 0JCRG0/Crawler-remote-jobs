import asyncio
import logging
from sql.clean_loc import clean_location_rows, convert_names_to_codes
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
			i = df.columns.get_loc(col)
			newvals = df.loc[:, col].astype(str).apply(convert_names_to_codes)
			df[df.columns[i]] = newvals
		
	#Save it in local machine
	df.to_csv(csv_path, index=False)

	#Log it 
	logging.info('Finished Selenium_Crawlers. Results below ⬇︎')
	
	# SEND IT TO TO PostgreSQL    
	db(df)

def pipeline_json_postgre_sel(pipeline: str, PROD: str, TEST):
	if pipeline:
		if pipeline == 'MAIN':
			if PROD:
				JSON = PROD
				POSTGRESQL = to_postgre
				print("\n", f"Pipeline is set to 'MAIN'. Jobs will be sent to PostgreSQL's main_jobs table", "\n")
				# configure the logger
				LoggingMasterCrawler()
				return JSON, POSTGRESQL
		elif pipeline == 'FREELANCE':
			#TODO: Fix path
			JSON = '/selenium_resources/freelance.json'
			POSTGRESQL = freelance_postgre
			# configure the logger
			LoggingFreelanceCrawler()
			#print("\n", f"Reading {JSON}. Jobs will be sent to PostgreSQL's freelance table", "\n")
		elif pipeline == 'TEST':
			if TEST:
				JSON = TEST
				POSTGRESQL = test_postgre
				print("\n", f"Pipeline is set to 'TEST'. Jobs will be sent to PostgreSQL's test table", "\n")
				# configure the logger
				LoggingMasterCrawler()
				return JSON, POSTGRESQL
		else:
			print("\n", "Incorrect argument! Use 'MAIN', 'TEST' or 'FREELANCE' to run this script.", "\n")
