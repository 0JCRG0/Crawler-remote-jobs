#!/usr/local/bin/python3

import bs4
import pandas as pd
import re
import pretty_errors
from urllib.error import HTTPError
import timeit
from selenium import webdriver
from selenium.webdriver.common.by import By
from dateutil.parser import parse
from datetime import date, datetime
import json
import logging
import requests
import os
from utils.FollowLink import async_follow_link, async_follow_link_container
from dotenv import load_dotenv
from utils.handy import *
from utils.bs4_utils import *
import asyncio
import aiohttp
""" LOAD THE ENVIRONMENT VARIABLES """

load_dotenv()

PROD = os.environ.get('JSON_PROD_BS4')
TEST = os.environ.get('JSON_TEST_BS4')
SAVE_PATH = os.environ.get('SAVE_PATH_BS4')

async def bs4_template(pipeline):
	print("\n", "BS4 crawlers deployed!.")

	#start timer
	start_time = asyncio.get_event_loop().time()

	#START SEL
	options = webdriver.FirefoxOptions()
	options.add_argument('-headless')

	# Start the session
	driver = webdriver.Firefox(options=options)

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


	async def fetch(url, session):
		async with session.get(url) as response:
			return await response.text()

	async def async_bs4_crawler(session, url_obj):
		total_links = []
		total_titles = []
		total_pubdates = []
		total_locations = []
		total_descriptions = []
		total_timestamps = []
		rows = {"title": total_titles,
				"link": total_links,
				"description": total_descriptions,
				"pubdate": total_pubdates,
				"location": total_locations,
				"timestamp": total_timestamps}

		#Extract the name of the crawler
		name = url_obj['name']
		print("\n", f"{name} has started", "\n")
		# Extract the 'url' key from the current dictionary and assign it to the variable 'url_prefix'
		url_prefix = url_obj['url']
		# Extract the first dictionary from the 'elements_path' list in the current dictionary and assign it to the variable 'elements_path'
		elements_path = url_obj['elements_path'][0]
			#Each site is different to a json file can give us the flexibility we need
		pages = url_obj['pages_to_crawl']
		#Extract the number in which the range is going to start from
		start_point = url_obj['start_point']
		#strategy
		strategy = url_obj['strategy']
		#Whether to follow link
		follow_link = url_obj['follow_link']
		#Extract inner link if follow link
		inner_link_tag = url_obj['inner_link_tag']
		# You need to +1 because range is exclusive
		async with aiohttp.ClientSession() as session:
				# ... (same URL processing logic as before)

				for i in range(start_point, pages + 1):
					url = url_prefix + str(i)

					try:
						html = await fetch(url, session)
						soup = bs4.BeautifulSoup(html, 'lxml')
						print(f"Crawling {url} with {strategy} strategy")
						if strategy == "main":
							jobs = soup.select(elements_path["jobs_path"])
							for job in jobs:

									# create a new dictionary to store the data for the current job
								job_data = {}

								title_element = job.select_one(elements_path["title_path"])
								job_data["title"] = title_element.text if title_element else "NaN"

								link_element = job.select_one(elements_path["link_path"])
								job_data["link"] = name + link_element["href"] if link_element else "NaN"

								""" Access each scraped link to get the description """
								description_default = job.select_one(elements_path["description_path"])
								default = description_default.text if description_default else "NaN"
								if follow_link == "yes":
									job_data["description"] = ""
									job_data["description"] = await async_follow_link(session=session, followed_link=job_data['link'], description_final=job_data["description"], inner_link_tag=inner_link_tag, default=default, driver=driver)
								elif follow_link == "sel":
									try:
										print("Using Selenium to obtain descriptions")
										driver.get(job_data["link"])
										description_tag = driver.find_element(By.CSS_SELECTOR, inner_link_tag)
										job_data["description"] = description_tag.get_attribute("innerHTML") if description_tag else "NaN"
									except Exception as e:
										print(e, f"""Skipping...{job_data["link"]}. Strategy: {strategy}""", "\n")
										continue
								else:
									# Get the descriptions & append it to its list
									job_data["description"]= default

								#PUBDATE
								today = date.today()
								job_data["pubdate"] = today

								location_element = job.select_one(elements_path["location_path"])
								job_data["location"] = location_element.text if location_element else "NaN"

								timestamp = datetime.now() # type: ignore
								job_data["timestamp"] = timestamp

										# add the data for the current job to the rows list
								total_links.append(job_data["link"])
								total_titles.append(job_data["title"])
								total_pubdates.append(job_data["pubdate"])
								total_locations.append(job_data["location"])
								total_timestamps.append(job_data["timestamp"])
								total_descriptions.append(job_data["description"])
						else:
							# Identify the container with all the jobs
							container = soup.select_one(elements_path["jobs_path"])

							try:
								if container:
										# TITLES
									title_elements = container.select(elements_path["title_path"])
									new_titles = [i.get_text(strip=True) if i else "NaN" for i in title_elements]
									total_titles.extend(new_titles)

										# LINKS
									link_elements = container.select(elements_path["link_path"])
									new_links = [name + i.get("href") if i else "NaN" for i in link_elements]
									total_links.extend(new_links)

									""" Access each scraped link to get the description """
									description_elements = container.select(elements_path["description_path"])
									default = [i.get_text(strip=True) if i else "NaN" for i in description_elements]
									if follow_link == "yes":
										for link in new_links:
											await async_follow_link_container(session, link, total_descriptions, inner_link_tag, default, driver)

									elif follow_link == "sel":
										print("Using Selenium to obtain descriptions")
										for link in new_links:
											try:
												#print("Using Selenium to obtain descriptions")
												driver.get(link)
												description_tag = driver.find_element(By.CSS_SELECTOR, inner_link_tag)
												description_inner = description_tag.get_attribute("innerHTML") if description_tag else "NaN"
												total_descriptions.append(description_inner)
											except Exception as e:
												print(e, f"Skipping...{link}. Strategy: {strategy}", "\n")
												continue
									else:
										total_descriptions.extend(default)

									# PUBDATES
									today = date.today()
									total_pubdates.extend([today] * len(link_elements))
									
									# LOCATIONS
									location_elements = container.select(elements_path["location_path"])
									new_locations = [i.get_text(strip=True) if i else "NaN" for i in location_elements]
									total_locations.extend(new_locations)

										#Timestamps
									timestamp = datetime.now()
									total_timestamps.extend([timestamp] * len(link_elements))

										# add the data
									rows = {'title':total_titles, 'link':total_links, 'description': total_descriptions, 'pubdate': total_pubdates, 'location': total_locations, 'timestamp': total_timestamps}
							except:
								print(f"An error occurred: CONTAINER NOT FOUND.. Skipping URL {url}")
								logging.error(f"An error occurred: CONTAINER NOT FOUND.. Skipping URL {url}")
								continue
					except aiohttp.ClientError as e:
						print(f"An error occurred: {e}. Skipping URL {url}")
						logging.error(f"An error occurred: {e}. Skipping URL {url}")
						continue
		return rows

	async def gather_json_loads_bs4(session):
		with open(JSON) as f:
			data = json.load(f)
			urls = data[0]["urls"]

		tasks = [async_bs4_crawler(session, url_obj) for url_obj in urls]
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

		clean_postgre_bs4(df=pd.DataFrame(combined_data), S=SAVE_PATH, Q=POSTGRESQL)

	async with aiohttp.ClientSession() as session:
		await gather_json_loads_bs4(session)

	elapsed_time = asyncio.get_event_loop().time() - start_time
	print(f"Async BS4 crawlers finished! all in: {elapsed_time:.2f} seconds.", "\n")

async def main():
	await bs4_template("TEST")

if __name__ == "__main__":
	asyncio.run(main())