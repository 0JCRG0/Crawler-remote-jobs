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

#TODO: Whether to use bs4 or go with selenium

def scjn():
    def make_soup(url):
            try:
                res = requests.get(url)
                res.raise_for_status()
            except Exception as e:
                print(f'An error occurred: {e}')
                sys.exit(1)
            return res

    def text_araña():
        file = "./OUTPUTS/SCJN_LABORAL_JURISPRUDENCIA.csv"
        text_all = []
        with open(file, newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                url = row['link']
                soup = bs4.BeautifulSoup(make_soup(url).text, 'html.parser')
                print(soup)
                titulo = soup.select('#main > app-root > section')
                for i in titulo:
                    text = i.get_text()
                    text_all.append(text)
        return text_all
    text = text_araña()
    print(text)

if __name__ == "__main__":
    scjn()