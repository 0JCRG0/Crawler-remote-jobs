from httpcore import TimeoutException
import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException
import bs4
import asyncio
from asyncio import TimeoutError
from concurrent.futures import ThreadPoolExecutor
import logging
#from handy import LoggingMasterCrawler
import os

#LoggingMasterCrawler()

async def fetch_sel(url, driver):
    loop = asyncio.get_event_loop()
    with ThreadPoolExecutor() as executor:
        await loop.run_in_executor(executor, driver.get, url)
    return driver.page_source

#TODO: FIX REFACTOR -- THERE WAS A KEYERROR WITH DESCRIPTION

async def async_follow_link(session, followed_link, description_final, inner_link_tag, default):

    async with session.get(followed_link) as link_res:
        if link_res.status == 200:
            print(f"""CONNECTION ESTABLISHED ON {followed_link}""", "\n")
            link_text = await link_res.text()
            link_soup = bs4.BeautifulSoup(link_text, 'html.parser')
            description_tag = link_soup.select_one(inner_link_tag)
            if description_tag:
                description_final = description_tag.text
                return description_final
            else:
                description_final = 'NaN'
                return description_final
        elif link_res.status == 403:
            print(f"""CONNECTION PROHIBITED WITH BS4 ON {followed_link}. STATUS CODE: "{link_res.status}". TRYING WITH SELENIUM""", "\n")
            logging.warning(f"""CONNECTION PROHIBITED WITH BS4 ON {followed_link}. STATUS CODE: "{link_res.status}". TRYING WITH SELENIUM""")
            #START SEL
            options = webdriver.FirefoxOptions()
            options.add_argument('-headless')
            # Start the session
            driver = webdriver.Firefox(options=options)
            driver.get(followed_link)
            description_tag = driver.find_element(By.CSS_SELECTOR, inner_link_tag)
            if description_tag:
                description_final = description_tag.get_attribute("innerHTML")
                return description_final
            else:
                description_final = 'NaN'
                return description_final
        else:
            print(f"""CONNECTION FAILED ON {followed_link}. STATUS CODE: "{link_res.status}". Getting the description from default.""", "\n")
            logging.warning(f"""CONNECTION FAILED ON {followed_link}. STATUS CODE: "{link_res.status}". Getting the description from default.""")
            description_final = default
            return description_final

async def async_follow_link_container(session, followed_link, total_descriptions: list, inner_link_tag, default):
    async with session.get(followed_link) as link_res:
        if link_res.status == 200:
            print(f"""CONNECTION ESTABLISHED ON {followed_link}""", "\n")
            link_text = await link_res.text()
            link_soup = bs4.BeautifulSoup(link_text, 'html.parser')
            description_tag = link_soup.select_one(inner_link_tag)
            description_inner = description_tag.text if description_tag else "NaN"
            return total_descriptions.append(description_inner)
        elif link_res.status == 403:
            print(f"""CONNECTION PROHIBITED WITH BS4 ON {followed_link}. STATUS CODE: "{link_res.status}". TRYING WITH SELENIUM""", "\n")
            logging.warning(f"""CONNECTION PROHIBITED WITH BS4 ON {followed_link}. STATUS CODE: "{link_res.status}". TRYING WITH SELENIUM""")
            #START SEL
            options = webdriver.FirefoxOptions()
            options.add_argument('-headless')
            # Start the session
            driver = webdriver.Firefox(options=options)
            driver.get(followed_link)
            description_tag = driver.find_element(By.CSS_SELECTOR, inner_link_tag)
            description_inner = description_tag.get_attribute("innerHTML") if description_tag else "NaN"
            return total_descriptions.append(description_inner)
        else:
            logging.warning(f"""CONNECTION FAILED ON {followed_link}. STATUS CODE: "{link_res.status}". Getting the description from default.""")
            return total_descriptions.extend(default)

def follow_link_sel(followed_link, inner_link_tag, driver):
    try:
        driver.get(followed_link)
        print(f"""CONNECTION ESTABLISHED ON {followed_link}""", "\n")
        try:
            # Set up a WebDriverWait instance with a timeout of 10 seconds
            wait = WebDriverWait(driver, 10)
            # Wait for the element to be present in the DOM and to be visible
            description_tag = wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, inner_link_tag)))
            description_final = description_tag.get_attribute("innerHTML") if description_tag else "NaN"
            return description_final
        except TimeoutException:
            print("Element not found within the specified wait time.", "Setting description to default")
            logging.error("Element not found within the specified wait time.", "Setting description to default")
            pass
        except NoSuchElementException as e:
            print("\n", f"""ELEMENT NOT FOUND ON {followed_link}. NoSuchElementError: {str(e)} "Setting description to default" """, "\n")
            logging.error(f"""ELEMENT NOT FOUND ON {followed_link}. NoSuchElementError: {str(e)} "Setting description to default" """)
            pass
    except NoSuchElementException as e:
        print("\n", f"""ELEMENT NOT FOUND ON {followed_link}. NoSuchElementError: {str(e)}""", "Setting description to default", "\n")
        logging.error(f"""ELEMENT NOT FOUND ON {followed_link}. NoSuchElementError: {str(e)} "Setting description to default" """)
        pass

def follow_link_container_sel(followed_link, inner_link_tag, driver):
    try:
        driver.get(followed_link)
        wait = WebDriverWait(driver, 10)
        print(f"""CONNECTION ESTABLISHED ON {followed_link}""", "\n")
        try:
            description_tag = wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, inner_link_tag)))
            description_inner = description_tag.get_attribute("innerHTML") if description_tag else "NaN"
            return description_inner
        except TimeoutException:
            print("Element not found within the specified wait time.", "Setting description to default")
            logging.error("Element not found within the specified wait time.", "Setting description to default")
            pass
        except NoSuchElementException as e:
            print("\n", f"""ELEMENT NOT FOUND ON {followed_link}. NoSuchElementError: {str(e)}""", "Setting description to default", "\n")
            logging.error(f"""ELEMENT NOT FOUND ON {followed_link}. NoSuchElementError: {str(e)} "Setting description to default" """)
            pass
    except NoSuchElementException as e:
        print("\n", f"""ELEMENT NOT FOUND ON {followed_link}. NoSuchElementError: {str(e)}""", "Setting description to default", "\n")
        logging.error(f"""ELEMENT NOT FOUND ON {followed_link}. NoSuchElementError: {str(e)} "Setting description to default" """)
        pass

async def async_follow_link_sel(followed_link, inner_link_tag, driver, fetch_sel):
    try:
        await fetch_sel(followed_link, driver)  # Replace driver.get with await fetch_sel
        print(f"""CONNECTION ESTABLISHED ON {followed_link}""", "\n")
        try:
            # Set up a WebDriverWait instance with a timeout of 10 seconds
            wait = WebDriverWait(driver, 10)
            # Wait for the element to be present in the DOM and to be visible
            description_tag = wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, inner_link_tag)))
            description_final = description_tag.get_attribute("innerHTML") if description_tag else "NaN"
            return description_final
        except TimeoutException:
            print("Element not found within the specified wait time.", "Setting description to default")
            logging.error("""Element not found within the specified wait time. Setting description to default""")
            pass
        except NoSuchElementException as e:
            print("\n", f"""ELEMENT NOT FOUND ON {followed_link}. NoSuchElementError: {str(e)}""", "Setting description to default", "\n")
            logging.error(f"""ELEMENT NOT FOUND ON {followed_link}. NoSuchElementError: {str(e)} "Setting description to default" """)
            pass
    except NoSuchElementException as e:
        print("\n", f"""ELEMENT NOT FOUND ON {followed_link}. NoSuchElementError: {str(e)}""", "Setting description to default", "\n")
        logging.error(f"""ELEMENT NOT FOUND ON {followed_link}. NoSuchElementError: {str(e)} "Setting description to default" """)
        pass