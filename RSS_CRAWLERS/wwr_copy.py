#! python3

import requests
import bs4
import sys
import pandas as pd
import re
import psycopg2
import pretty_errors
from utils.handy import send_postgre

df = pd.read_csv('./OUTPUTS/WWR.csv')

pd.set_option("display.max_columns", None)
pd.set_option('display.max_colwidth', 500)
pd.set_option('display.max_rows', None)


duplicates = df[df.duplicated(subset=['link'], keep=False)]

print(duplicates.shape)

df = df.drop_duplicates(subset=['link'])

print(df.shape)