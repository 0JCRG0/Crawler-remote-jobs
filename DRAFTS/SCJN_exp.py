from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd
import re
import pretty_errors
import csv
import timeit
from clean_regex import bye_regex, pandas_regex

#TODO: Whether to use bs4 or go with selenium


def scjn():

    #start timer
    start_time = timeit.default_timer()

    #Start the session
    driver = webdriver.Firefox()

    def urls():
        file = "./OUTPUTS/SCJN_LABORAL_JURISPRUDENCIA.csv"
        urls = []
        with open(file, newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                url = row['link']
                urls.append(url)
        return urls

    def elements():
        total_ids = []
        total_titulos = []
        total_contenidos = []
        total_notas = []
        data = []
        #todo = []
        for i in urls():
            driver.get(i)
            print("\n", f"Crawling... {i}", "\n")

            #Identifica los elementos
            #all_text = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, '#cont-principal .int-doc-tesis.ng-star-inserted')))
            id = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, '#cont-principal .int-doc-tesis.ng-star-inserted .int-doc-tesis__metadata.row.p-2.m-0')))
            titulo = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, '#cont-principal .int-doc-tesis.ng-star-inserted .int-doc-tesis__information')))
            contenido = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, '#cont-principal .int-doc-tesis.ng-star-inserted .int-doc-tesis__content')))
            notas = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, '#cont-principal .int-doc-tesis.ng-star-inserted .int-doc-tesis__notas')))
            
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

            print("\n", f"Done with {i}. Moving on.", "\n")
            data = {'ID:': total_ids, 'TITULO': total_titulos, 'CONTENIDO': total_contenidos, 'NOTAS':total_notas}
        return data

    data = elements()

    driver.close()

    #Convert data to a pandas df for further analysis
    print("\n", "Converting data to a pandas df...", "\n")
    data_dic = dict(data)
    df = pd.DataFrame.from_dict(data_dic, orient='index')
    df = df.transpose()
    #apply regex to every letter of the df
    #modify the settings for fun 
    pd.set_option('display.max_colwidth', 150)
    pd.set_option("display.max_rows", None)
    #inspect the df
    print("\n", df.info, "\n", df.describe(), "\n")

    print("\n", "Saving jobs in local machine...", "\n")
    directory = "./OUTPUTS/"
    df.to_csv(f'{directory}scjn_laboral.csv', index=False)

    df = df.apply(pandas_regex, axis=0)

    elapsed_time = timeit.default_timer() - start_time
    print("\n", f"Crawler successfully found {len(df)} thesis in {elapsed_time:.5f} seconds", "\n")

if __name__ == "__main__":
    scjn()

