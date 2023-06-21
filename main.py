#!/usr/local/bin/python3

import logging
import timeit
import asyncio
from async_rss import async_rss_template
from other.indeed import indeed
from async_api import async_api_template
from async_bs4 import async_bs4_template
from async_sel import async_selenium_template
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
	await asyncio.gather(
		async_api_template(pipeline),
		async_rss_template(pipeline),
		async_bs4_template(pipeline),
		async_selenium_template(pipeline)
	)

	#indeed(SCHEME="main_mx", KEYWORD="")

	#print the time
	elapsed_time = asyncio.get_event_loop().time() - master_start_time
	print(f"Async BS4 crawlers finished! all in: {elapsed_time:.2f} seconds.", "\n")

async def main():
	await async_main("TEST")

if __name__ == "__main__":
	asyncio.run(main())
