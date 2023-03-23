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

#TODO: join this with SCJN


def scjn():

    #start timer
    start_time = timeit.default_timer()

    #Start the session
    driver = webdriver.Firefox()

    def urls():
        file = "./OUTPUTS/urls_scjn_laboral_jurisprudencia.csv"
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
        #testing
        count = 0 
        for i in urls():
            driver.get(i)
            #Testing // index 
            count += 1
            #if count == 10:  # Exit loop after 10 lines
                #break
            
            print("\n", f"Obteniendo jurisprudencia en -> {i}. Index: {count}", "\n")

            #Identifica los elementos
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

            data = {'ID:': total_ids, 'TITULO': total_titulos, 'CONTENIDO': total_contenidos, 'NOTAS':total_notas}
        return data

    data = elements()

    driver.close()

    #Convert data to a pandas df for further analysis
    print("\n", "Convirtiendo jurisprudencias en df...", "\n")
    data_dic = dict(data)
    df = pd.DataFrame.from_dict(data_dic, orient='index')
    df = df.transpose()

    print("\n", "Guardando jurisprudencias en archivos locales...", "\n")
    directory = "./OUTPUTS/"
    df.to_csv(f'{directory}scjn_laboral_jurisprudencia.csv', index=False)

    #apply regex to every letter of the df
    df = df.apply(pandas_regex, axis=0)

    end_time = timeit.default_timer()
    elapsed_time_minutes = (end_time - start_time) / 60
    print("\n", f"ARAÑA 2 obtuvo {len(df)} jurisprudencias de la SCJN en {elapsed_time_minutes:.2f} minutos", "\n")

if __name__ == "__main__":
    scjn()

