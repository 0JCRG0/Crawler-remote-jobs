#!/usr/local/bin/python3

import logging
import timeit
import asyncio
import traceback
from async_rss import async_rss_template
from other.indeed import indeed
from async_api import async_api_template
from async_bs4 import async_bs4_template
from async_sel import async_selenium_template
from async_indeed import async_indeed_template
from utils.handy import LoggingMasterCrawler

#SET UP LOGGING
LoggingMasterCrawler()

async def async_main(pipeline):
	# start master timer
	master_start_time = timeit.default_timer()

	#Start calling each crawler
	logging.info('ALL CRAWLERS DEPLOYED!')
	print("\n", "ALL CRAWLERS DEPLOYED!")

	# Schedule tasks to run concurrently using asyncio.gather()
	results = await asyncio.gather(
		async_api_template(pipeline),
		async_rss_template(pipeline),
		async_bs4_template(pipeline),
		async_selenium_template(pipeline),
		async_indeed_template("MX", "", pipeline),
		return_exceptions=True
	)

	for result in results:
		if isinstance(result, Exception):
			# handle exception
			logging.error(f"{type(result).__name__} in {result}\n{traceback.format_exc()}")
			continue

	#print the time
	elapsed_time = asyncio.get_event_loop().time() - master_start_time
	min_elapsed_time = elapsed_time / 60
	print(f"ALL ASYNC CRALERS FINISHED IN: {min_elapsed_time:.2f} minutes.", "\n")
	logging.info(f"ALL ASYNC CRALERS FINISHED IN: {min_elapsed_time:.2f} minutes.")

async def main():
	await async_main("MAIN")

if __name__ == "__main__":
	asyncio.run(main())
