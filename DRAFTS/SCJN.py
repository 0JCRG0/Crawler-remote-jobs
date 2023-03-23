from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd
import re
import pretty_errors
import numpy as np
import bs4
import json
import requests
import timeit
from utils.clean_regex import bye_regex, pandas_regex


def scjn():

    
    #Start the timer
    start_time = timeit.default_timer()

    #Driver...
    driver = webdriver.Firefox()

    # set the number of pages you want to scrape
    num_pages = 372

    def url_araña():
        print("\n", f"ARAÑA 1 activa...", "\n")
        urls = []
        total_urls = []

        for i in range(1, num_pages + 1):
            url = f"https://bj.scjn.gob.mx/busqueda?q=*&indice=tesis&subFiltros=tipo.keyword:Tesis%20de%20Jurisprudencia,materia.keyword:Laboral&size=25&page={i}"
            #Make the request
            #Make the soup...
            print("\n", f'Obteniendo urls en ---> {url}', "\n")
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
    
    urls = url_araña()

    #close the driver
    driver.close()

    data = dict(urls)
    df = pd.DataFrame.from_dict(data, orient='index')
    df = df.transpose()

    print("\n", "Guardando urls en archivos locales...", "\n")
    directory = "./OUTPUTS/"
    df.to_csv(f'{directory}urls_scjn_laboral_jurisprudencia.csv', index=False)
    

#print the time
    elapsed_time = timeit.default_timer() - start_time
    print("\n", f"ARAÑA 1 encontró {len(df)} urls en: {elapsed_time:.1f} segundos", "\n")

    #START 2nd CRAWLER 
    print("\n", "ARAÑA 2 activa.", "\n")
    #Start the session
    driver1 = webdriver.Firefox()
    #Start the timer
    start_time = timeit.default_timer()
    #Get the text from the links
    def iterate_df():
        #columns...
        total_ids = []
        total_titulos = []
        total_contenidos = []
        total_notas = []
        #df
        data = []
        for index, row in df.iterrows():
            link = row['link']
            driver1.get(link)
            print("\n", f"Obteniendo todo el texto de ---> {link}. Iteration number: {index}", "\n")

            #Identifica los elementos
            id = WebDriverWait(driver1, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, '#cont-principal .int-doc-tesis.ng-star-inserted .int-doc-tesis__metadata.row.p-2.m-0')))
            titulo = WebDriverWait(driver1, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, '#cont-principal .int-doc-tesis.ng-star-inserted .int-doc-tesis__information')))
            contenido = WebDriverWait(driver1, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, '#cont-principal .int-doc-tesis.ng-star-inserted .int-doc-tesis__content')))
            notas = WebDriverWait(driver1, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, '#cont-principal .int-doc-tesis.ng-star-inserted .int-doc-tesis__notas')))
            
            #Obten el texto de id
            id_sucio = id.get_attribute("innerHTML")
            #limpia 
            id = bye_regex(id_sucio)
            #agregalo
            total_ids.append(id)
            
            #Obten el texto de titulos
            titulo_sucio = titulo.get_attribute("innerHTML")
            #limpia
            titulo = bye_regex(titulo_sucio)
            #agregalo
            total_titulos.append(titulo)
            
            #Obten todos los contenidos
            contenido_sucio = contenido.get_attribute("innerHTML")
            #limpia
            contenido = bye_regex(contenido_sucio)
            #agregalo
            total_contenidos.append(contenido)

            #Obten todas las notas
            notas_sucio = notas.get_attribute("innerHTML")
            #limpia
            notas_limpio = bye_regex(notas_sucio)
            #agregalo
            total_notas.append(notas_limpio)

            data = {'ID': total_ids, 'TITULO': total_titulos, 'CONTENIDO': total_contenidos, 'NOTAS':total_notas}
        return data
    
    data = iterate_df()
    driver1.close()
    
    #Convert data to a pandas df for further analysis
    print("\n", "Convirtiendo en df...", "\n")
    data_dic = dict(data)
    df1 = pd.DataFrame.from_dict(data_dic, orient='index')
    df1 = df1.transpose()

    #apply regex to every letter of the df
    df1 = df1.apply(pandas_regex, axis=0)

    print("\n", "Guardando tesis en archivos locales...", "\n")
    directory = "./OUTPUTS/"
    df1.to_csv(f'{directory}scjn_laboral_jurisprudencias.csv', index=False)

    #print the time
    elapsed_time = timeit.default_timer() - start_time
    print("\n", f"ARAÑA 2 obtuvo {len(df1)} jurisprudencias en: {elapsed_time:.1f} segundos", "\n")


if __name__ == "__main__":
    scjn()
    
