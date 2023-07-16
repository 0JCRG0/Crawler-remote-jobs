#!/usr/local/bin/python3

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
import re
import pretty_errors
import json
import asyncio
import timeit
from utils.indeed_utils import *
from utils.FollowLink import *
from datetime import date, datetime
from dotenv import load_dotenv
import os
import sys
from utils.handy import *

""" LOAD THE ENVIRONMENT VARIABLES """

load_dotenv()

PROD = os.environ.get('JSON_PROD_INDEED', "")
TEST = os.environ.get('JSON_TEST_INDEED', "")
SAVE_PATH = os.environ.get('SAVE_PATH_INDEED', "")
#UTILS_PATH = os.environ.get('SYS_PATH_APPEND', "")

""" APPEND UTILS' PATH """
#sys.path.append(UTILS_PATH)
#from handy import *

#Initiate Logging
#LoggingMasterCrawler()

async def async_indeed_template(SCHEME, KEYWORD, pipeline):
	start_time = timeit.default_timer()

	options = webdriver.FirefoxOptions()
	options.add_argument('-headless')
	#driver = webdriver.Firefox(options=options)

	""" DETERMINING WHICH JSON TO LOAD & WHICH POSTGRE TABLE WILL BE USED """

	if pipeline == 'MAIN':
		if PROD:
			JSON = PROD
		POSTGRESQL = to_postgre
		print("\n", f"Pipeline is set to 'MAIN'. Jobs will be sent to PostgreSQL's main_jobs table", "\n")
		# configure the logger
		LoggingMasterCrawler()
	elif pipeline == 'FREELANCE':
		#TODO: FIX - ADD PÁTH
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
	else:
		print("\n", "Incorrect argument! Use 'MAIN', 'TEST' or 'FREELANCE' to run this script.", "\n")
		logging.error("Incorrect argument! Use either 'MAIN' or 'TEST' to run this script.")

	async def fetch_indeed(url, driver):
		loop = asyncio.get_event_loop()
		with ThreadPoolExecutor() as executor:
			await loop.run_in_executor(executor, driver.get, url)
		return driver.page_source

	async def async_indeed_crawling(strategies, options):
		#print("\n", f"Reading {file}... ", "\n")
		print("\n", "Async INDEED has started.")
		logging.info("Async INDEED crawler deployed!.")

		#NEW DRIVER EACH ITERATION FOR SITE
		driver = webdriver.Firefox(options=options)
		
		total_titles = []
		total_links = []
		total_pubdates = []
		total_locations = [] 
		total_descriptions = []
		total_timestamps = []
		rows = {'title': total_titles,
				'link': total_links,
				'description': total_descriptions,
				'pubdate': total_pubdates,
				'location': total_locations,
				"timestamp": total_timestamps}
		strategy = strategies[0]
		country_code = strategy["code"]
		special_url = strategy["special_url"]
		url_1 = strategy["url_1"]
		url_2 = strategy["url_2"]
		pages_to_crawl = strategy["pages_to_crawl"]
		elements_path = strategy["elements_path"][0]
		follow_link = strategy['follow_link']
		inner_link_tag = strategy['inner_link_tag']

		""" SET THE WAITING STRATEGIES """
		# Add these lines before the for loop
		driver.implicitly_wait(1.5)
		wait = WebDriverWait(driver, 10)

		for i in range(0, pages_to_crawl * 10, 10):
			page_print = round(i/10) + 1
			if special_url == "yes":
				url = url_1 + KEYWORD + url_2 + str(i)
				print("\n", f"Crawler deployed on Indeed using {SCHEME} strategy. Looking for \"{KEYWORD}\". Currently crawling page number: {page_print}.")
			else:
				url = strategy['url_1'] + str(i)
				print("\n", f"Crawler deployed on Indeed using {SCHEME} strategy. Currently crawling page number: {page_print}.", "\n")
				#Make the request
			try:
				await fetch_indeed(url, driver)
				print(f"Crawling {url}...")

				jobs = wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, elements_path["jobs_path"])))    
				if jobs:
					try:
						for job in jobs:
							##Get the attributes...
							job_data = {}
								
							title_raw = job.find_element(By.CSS_SELECTOR, elements_path["title_path"])
							job_data["title"] = title_raw.get_attribute("innerHTML") if title_raw else "NaN"
								
							#LINKS
							link_raw = job.find_element(By.CSS_SELECTOR, elements_path["link_path"])
							job_data["link"] = link_raw.get_attribute("href") if link_raw else "NaN"

							#DATES
							today = date.today()
							job_data["pubdate"] = today
								
							#LOCATION    
							location_raw = job.find_element(By.CSS_SELECTOR, elements_path["location_path"])
							job_data["location"] = location_raw.text if location_raw else "NaN"

							#TIMESTAMP
							timestamp = datetime.now()
							job_data["timestamp"] = timestamp

							#Put it all together...
							total_titles.append(job_data["title"])
							total_links.append(job_data["link"])
							total_pubdates.append(job_data["pubdate"])
							total_locations.append(job_data["location"])
							#total_descriptions.append(job_data["description"])
							total_timestamps.append(job_data["timestamp"])

							""" HOW TO FOLLOW THE SCRAPED LINKS """
							description_raw = job.find_element(By.CSS_SELECTOR, elements_path["description_path"])
							default = description_raw.get_attribute("innerHTML") if description_raw else "NaN"
							job_data["description"] = default
							total_descriptions.append(job_data["description"])
						
						"""FOLLOW THE SCRAPED LINKS OUTSIDE THE FOR LOOP"""
						if follow_link == "yes":
							for i, link in enumerate(total_links):
								total_descriptions[i]  = await async_follow_link_sel(followed_link=link, inner_link_tag=inner_link_tag, driver=driver, fetch_sel=fetch_indeed)
						else:
							# Get the descriptions & append it to its list
							total_descriptions = total_descriptions
					except NoSuchElementException as e:
						print(f"NoSuchElementException: {str(e)}")
						logging.error(f"NoSuchElementException: {str(e)}")
						pass
			except NoSuchElementException as e:
			# Handle the specific exception
				print("Element not found:", e)
				logging.error(f"Element not found: {str(e)}")
				pass
			except Exception as e:
				print("\n", f"INDEED. Exception: {e}", "\n")
				logging.error(f"INDEED. Exception on {str(url)}. {str(e)}")
				pass
		
		driver.quit()
		return rows
	async def gather_tasks_indeed(options):
		with open(JSON) as f:
			data = json.load(f)
			strategies = [strategy for strategy in data[0]["strategies"] if strategy["strategy_name"] == SCHEME]
			tasks = [async_indeed_crawling(strategies, options)]
			results = await asyncio.gather(*tasks)
			# Combine the results
			combined_data = {
				"title": [],
				"link": [],
				"description": [],
				"pubdate": [],
				"location": [],
				"timestamp": [],
			}
			for result in results:
				for key in combined_data:
					combined_data[key].extend(result[key])
			
			print("Lengths of lists before creating DataFrame:")
			print("Titles:", len(combined_data["title"]))
			print("Links:", len(combined_data["link"]))
			print("Descriptions:", len(combined_data["description"]))
			print("Pubdates:", len(combined_data["pubdate"]))
			print("Locations:", len(combined_data["location"]))
			print("Timestamps:", len(combined_data["timestamp"]))

			clean_postgre_indeed(df=pd.DataFrame(combined_data), S=SAVE_PATH, Q=POSTGRESQL, c_code=SCHEME)

	await gather_tasks_indeed(options=options)
	
	#stop the timer
	elapsed_time = timeit.default_timer() - start_time
	print("\n", f"Finished async Indeed. All in {elapsed_time:.5f} seconds!!!", "\n")
	logging.info(f"Finished async Indeed. All in {elapsed_time:.5f} seconds!!!")
async def main():
	await async_indeed_template("MX", "", "TEST")

if __name__ == "__main__":
	asyncio.run(main())
	"""
	1st = "MX", "specific_mx" or "specific_usa"
	2nd = KEYWORD if specific if not empty string
	
	"""