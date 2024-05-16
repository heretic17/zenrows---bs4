import csv
import datetime
import logging
from lxml import html
import os
from lxml import etree 
import pandas as pd
import json
import requests
from bs4 import BeautifulSoup
from multiprocessing.pool import ThreadPool
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

apikey = os.getenv("API_KEY")
zenrows_api_base = "https://api.zenrows.com/v1/"
urls = []
concurrency = 10  # maximum concurrent requests, depends on the plan

logging.basicConfig(filename='error.log', level=logging.ERROR, format='%(asctime)s - %(message)s')

requests_session = requests.Session()
retries = Retry(
	total=3,  # number of retries
	backoff_factor=1,  # exponential time factor between attempts
	status_forcelist=[422, 429, 500, 502, 503, 504]  # status codes that will retry
)

requests_session.mount("http://", HTTPAdapter(max_retries=retries))
requests_session.mount("https://", HTTPAdapter(max_retries=retries))

# Read Excel file containing the numbers
excel_file = 'removed_dupes.xlsx'  # Change this to your Excel file path
df = pd.read_excel(excel_file, dtype=str)

numbers_list = [str(number) for number in df.iloc[:, 0].tolist()]

with open('database\data.json', 'r', encoding='utf-8') as file:
    database = json.load(file)

keys_list = [str(key) for key in database.keys()]

# Convert both lists to sets for easier comparison
numbers_set = set(numbers_list)
keys_set = set(keys_list)

common_elements = numbers_set.intersection(keys_set)

# Iterate through the numbers in the Excel file
for key in common_elements:  # Replace 'YourColumnName' with the actual column name in your Excel file
    urls.append(database[key]["url"])

print(len(urls))

def extract_content(url, soup):
    # Extracting logic goes here
    oos_block = soup.find(id="oosBlock")
    stat = "In Stock"
    
    # Find the SKU element
    sku_li = soup.find('li', string=lambda t: 'SKU #' in str(t))
    
    # Check if SKU element is found
    if sku_li:
        sku = sku_li.get_text(strip=True)
    else:
        sku = "SKU not found"
    
    if oos_block:
        stat = "Out of Stock"
    return {    
        "sku": sku,
        "stat": stat,
        "url": url
    }

def scrape_with_zenrows(url):
    try:
        print(f"Scraping: {url}")
        response = requests_session.get(zenrows_api_base, params={
            "apikey": apikey,
            "url": url,
        })
        soup = BeautifulSoup(response.text, "html.parser")
        # print(soup)
        return extract_content(url, soup)
    except Exception as e:
        error_message = f"Error processing {url}: {e}"
        print(error_message)
        logging.error(error_message)  # Log error to file
        return None


pool = ThreadPool(concurrency)
results = pool.map(scrape_with_zenrows, urls)
pool.close()
pool.join()

current_date = datetime.now()

# Convert year, month, and day to strings and concatenate with dots
formatted_date = str(current_date.year) + "." + str(current_date.month) + "." + str(current_date.day)

def append_to_csv(file_path, data):
    with open(file_path, 'a', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow([data['sku'], data['stat']])

for result in results:
    if result:
        append_to_csv(f'Output_{formatted_date}.csv', result)

[print(result) for result in results if result]  # custom scraped content

# input("Press Enter to exit...")