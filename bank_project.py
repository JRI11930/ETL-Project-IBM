# DATA MANAGEMENT
import pandas as pd
import numpy as np
import sqlite3

# WEB SCRAPING
import requests
from bs4 import BeautifulSoup

# UTILS
import os
from datetime import datetime, time
from dotenv import load_dotenv, find_dotenv

# ETC
from enum import Enum


load_dotenv()

# CONSTS
url = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"
exchange_url = "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv"
extract_cols = ["Name", "MC_USD_Billion"] 
table_cols = ["Name", "MC_USD_Billion", "MC_GBP_Billion", "MC_EUR_Billion", "MC_INR_Billion"]
output_csv_path = './Largest_banks_data.csv'
db_name = os.getenv('BANKS_DB', 'Banks.db')
table_name = os.getenv('BANKS_TABLE', 'Largest_banks')
log_file = 'code_log.txt'

# AUXILIARY CLASSES
class LogLevel(Enum):
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    DEBUG = "DEBUG"

# HELPER FUNCTIONS
def log_progress(level: LogLevel, message: str) -> None:
    if isinstance(level, LogLevel):
        timestamp = datetime.now().isoformat(timespec="seconds")
        with open(log_file, 'a') as f:
            f.write(f"{timestamp} : {message}\n")

    return

def extract(url, table_attribs):
    html = requests.get(url).text
    data = BeautifulSoup(html, 'html.parser')
    tables = data.find_all('tbody')
    rows = tables[0].find_all('tr')[1:] # Title row does not count as data

    bank_names = []
    mc_usd = []
    for row in rows:
        bank_names.append(row.find_all('td')[1].find_all('a')[-1:][0].text)
        mc_usd.append(float(row.find_all('td')[2].text))

    df = pd.DataFrame(list(zip(bank_names, mc_usd)), columns=table_attribs)

    log_progress(LogLevel.DEBUG, 'Data extraction complete. Initiating Transformation process')
    return df

def transform(df, csv_path):
    exchange = pd.read_csv(csv_path)
    currencies = exchange['Currency'].unique()

    df_transformed = df.copy()
    for currency in currencies:
        df_transformed[f'MC_{currency}_Billion'] = df_transformed.iloc[:,1] * exchange.loc[exchange['Currency'] == currency]['Rate'].values
    
    print('\n', df_transformed['MC_EUR_Billion'][4], '\n')

    log_progress(LogLevel.DEBUG, 'Data transformation complete. Initiating Loading process')
    print('\n', df_transformed, '\n')
    return df_transformed

def load_to_csv(df: pd.DataFrame, output_path):
    try:
        df.to_csv(output_path, encoding='utf-8')
    except Exception as e:
        log_progress(LogLevel.ERROR, f'There was an error while loading data to csv file: {e}')
    else:
        log_progress(LogLevel.DEBUG, 'Data saved to CSV file')

def load_to_db(df, sql_connection, table_name):
    
    try:
        df.to_sql(table_name, sql_connection, if_exists='replace', index=False)
    except Exception as e:
        log_progress(LogLevel.ERROR, f'There was an error while loading data to db: {e}')
    else:
        log_progress(LogLevel.DEBUG, f'Data loaded to Database as a table. Executing queries')
 
def run_query(query_statement, sql_connection):
    try:
        print(query_statement)
        print(pd.read_sql(query_statement, sql_connection))
    except Exception as e:
        log_progress(LogLevel.ERROR, f'There was an error while querying the database: {e}')


def main():
    # Preliminaries
    log_progress(LogLevel.DEBUG, 'Preliminaries complete. Initiating ETL process')

    # Extract
    raw_data = extract(url, extract_cols)    

    # Transform
    df = transform(raw_data, exchange_url)

    # Load 
    load_to_csv(df, output_csv_path)
    
    conn = sqlite3.connect(db_name)
    log_progress(LogLevel.DEBUG, 'SQL Connection initiated')
    load_to_db(df, conn, table_name)

    # Query the database
    run_query('SELECT Name from Largest_banks LIMIT 5', conn)
    log_progress(LogLevel.DEBUG, 'Process Complete')

    conn.close()
    log_progress(LogLevel.DEBUG, 'Server Connection closed')

if __name__ == "__main__":
    main()