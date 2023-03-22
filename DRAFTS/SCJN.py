from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.actions.wheel_input import ScrollOrigin
import random
import time
import pandas as pd
import re
import pretty_errors
import csv
import psycopg2
import numpy as np
import bs4
import json
import requests
import sys
import timeit


def scjn():

    #Start the timer
    start_time = timeit.default_timer()

    #Driver...
    driver = webdriver.Firefox()

    # set the number of pages you want to scrape
    num_pages = 3

    def url_araña():
        print("\n", f"Iniciando araña... 0%", "\n")
        urls = []
        total_urls = []

        for i in range(1, num_pages + 1):
            url = f"https://bj.scjn.gob.mx/busqueda?q=*&indice=tesis&subFiltros=tipo.keyword:Tesis%20de%20Jurisprudencia,materia.keyword:Laboral&size=25&page={i}"
            #Make the request
            #Make the soup...
            print("\n", 'Tejiendo telaraña en...' + url, "\n")
            #Make the request
            driver.get(url)
            #Wait until the elements appear
            WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, '.pub-results__list .card.my-3.ng-star-inserted.find a')))
            #Get the urls
            url_elements = driver.find_elements(By.CSS_SELECTOR, '.pub-results__list .card.my-3.ng-star-inserted.find a')
            #Loop through all of them and add them individually to the list
            for i in url_elements:
                url = i.get_attribute("href")
                urls.append(url)
                total_urls = {"link": urls}
        return total_urls
    
    data_ = url_araña()

    #close the driver
    driver.close()

    data = dict(data_)
    df = pd.DataFrame.from_dict(data, orient='index')
    df = df.transpose()
    print(df)

    print("\n", "Guardando tesis en archivos locales...", "\n")
    directory = "./OUTPUTS/"
    df.to_csv(f'{directory}SCJN_LABORAL_JURISPRUDENCIA.csv', index=False)

#print the time
    elapsed_time = timeit.default_timer() - start_time
    print("\n", f"Terminó la araña en: {elapsed_time:.5f} segundos", "\n")


if __name__ == "__main__":
    scjn()
    
