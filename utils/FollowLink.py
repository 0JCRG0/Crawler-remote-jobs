import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
import bs4

#TODO: FIX REFACTOR -- THERE WAS A KEYERROR WITH DESCRIPTION

def inner_crawler_bs4(followed_link, description_final, inner_link_path, i, description_path_original):

    #START SEL
    options = webdriver.FirefoxOptions()
    options.add_argument('-headless')

    driver = webdriver.Firefox(options=options)

    link_res = requests.get(followed_link)
    if link_res.status_code == 200:
        print(f"""CONNECTION ESTABLISHED ON {followed_link}""", "\n")
        link_soup = bs4.BeautifulSoup(link_res.text, 'html.parser')
        description_tag = link_soup.select_one(inner_link_path)
        if description_tag:
            description_final = description_tag.text
            return description_final
        else:
            description_final = 'NaN'
            return description_final
    elif link_res.status_code == 403:
        print(f"""CONNECTION PROHIBITED WITH BS4 ON {followed_link}. STATUS CODE: "{link_res.status_code}". TRYING WITH SELENIUM""", "\n")
        driver.get(followed_link)
        description_tag = driver.find_element(By.CSS_SELECTOR, inner_link_path)
        if description_tag:
            description_final = description_tag.get_attribute("innerHTML") 
            return description_final
        else:
            description_final = 'NaN'
            return description_final
    else:
        print(f"""CONNECTION FAILED ON {followed_link}. STATUS CODE: "{link_res.status_code}". Getting the description from initial scraped website.""", "\n")
        # Get the descriptions & append it to its list
        description_element = i.select_one(description_path_original)
        description_final= description_element.text if description_element else "NaN"
        return description_final