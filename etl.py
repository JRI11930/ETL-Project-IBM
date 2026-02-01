# DATA MANAGEMENT
import pandas as pd
import numpy as np
import sqlite3

# UTILS
from dotenv import load_dotenv
import glob 
from datetime import datetime
import os

# WEB SCRAPING
import requests
from bs4 import BeautifulSoup


load_dotenv()

url = "https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29"
json_path = os.getenv("json_gdp")
database = 'World_Economies.db'
table_name = "Countries_by_GDP"
columns = ["Country", "GDP_USD_Billions"]

def extract_data(url):
    response = requests.get(url).text
    html = BeautifulSoup(response, 'html.parser')


extract_data(url)