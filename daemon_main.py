import schedule
import datetime
import pretty_errors
import asyncio
from main import async_main
import logging
from utils.handy import LoggingMasterCrawler

LoggingMasterCrawler()

async def schedule_job():
    while True:
        await async_main(pipeline="MAIN")
        await asyncio.sleep(3 * 60 * 60)  # Sleep for 3 hours

# Create an event loop
loop = asyncio.get_event_loop()

# Schedule the job as a task
task = asyncio.ensure_future(schedule_job())

# Run the event loop indefinitely
try:
    loop.run_until_complete(task)
except KeyboardInterrupt as e:
    logging.info(f"KeyboardInterrupt: {e}.")
    task.cancel()
    loop.run_until_complete(task)
except Exception as e:
    logging.error(f"EXCEPTION OCCURRED: {e}. CHECK ASAP!")

