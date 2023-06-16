import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
import bs4

#TODO: FIX REFACTOR -- THERE WAS A KEYERROR WITH DESCRIPTION

async def async_follow_link(session, followed_link, description_final, inner_link_tag, default, driver):

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
            driver.get(followed_link)
            description_tag = driver.find_element(By.CSS_SELECTOR, inner_link_tag)
            if description_tag:
                description_final = description_tag.get_attribute("innerHTML")
                return description_final
            else:
                description_final = 'NaN'
                return description_final
        else:
            print(f"""CONNECTION FAILED ON {followed_link}. STATUS CODE: "{link_res.status}". Getting the description from API.""", "\n")
            description_final = default
            return description_final