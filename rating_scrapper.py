import re
import multiprocessing
from tqdm import tqdm
from selenium import webdriver
import time
import pandas as pd
import pickle
import logging
from database_handler import mysql_client



def selenium_render(source_html):
    driver = webdriver.Chrome('C:/Windows/chromedriver.exe')  # Optional argument, if not specified will search path.
    driver.get(source_html)
    SCROLL_PAUSE_TIME = 0.5
    # Get scroll height
    last_height = driver.execute_script("return document.body.scrollHeight")
    while True:
        # Scroll down to bottom
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        # Wait to load page
        time.sleep(SCROLL_PAUSE_TIME)
        # Calculate new scroll height and compare with last scroll height
        new_height = driver.execute_script("return document.body.scrollHeight")
        if new_height == last_height:
            break
        last_height = new_height
    # time.sleep(10) # Let the user actually see something!
    htmlSource = driver.page_source
    driver.quit()
    return htmlSource


def get_symbol_list():
    files = ['symbol/nasdaq.csv', 'symbol/nyse.csv']
    symbol_list = []
    for i in files:
        data_df = pd.read_csv(i)
        symbol = data_df['Symbol']
        symbol_list+=symbol.values.tolist()
    return symbol_list

def single_page_workder(symbol):
    try:
        url = f'https://finance.yahoo.com/quote/{symbol}/analysts?p={symbol}'
        data = selenium_render(url)
        rating_pattern = re.compile(r'<div class="rating-text Arrow South.*>(?P<rating>\d+\.?\d*)</div>')
        price_pattern = re.compile(r'<div tabindex="0" aria-label="Low\s+(?P<low>\d+\.?\d*)\s+Current\s+(?P<current>\d+\.?\d*)\s+Average\s+(?P<target>\d+\.?\d*)\s+High\s+(?P<high>\d+\.?\d*)"><div class="')
        result = rating_pattern.finditer(data)
        rating_result_dict_list = [m.groupdict() for m in result]
        if rating_result_dict_list:
            rating_result_dict_list = rating_result_dict_list[0]
        else:
            rating_result_dict_list = {}

        price_result = price_pattern.finditer(data)
        price_result_dict_list = [m.groupdict() for m in price_result]
        if price_result_dict_list:
            price_result_dict_list = price_result_dict_list[0]
        else:
            price_result_dict_list = {}
        price_result_dict_list.update(rating_result_dict_list)
        output_dict = {symbol: price_result_dict_list}
    except Exception as e:
        logging.exception(e)
        output_dict = {}
    return output_dict

if __name__ == '__main__':
    logging.basicConfig(level=0, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    symbol_list = get_symbol_list()
    output_dict = {}
    with open('cache', 'rb') as f:
        current_dict = pickle.load(f)
    left_symbol = [s for s in symbol_list if s not in current_dict.keys()]
    p = multiprocessing.Pool(processes=multiprocessing.cpu_count())
    m = multiprocessing.Manager()
    lock = m.Lock()
    while left_symbol:
        print(len(left_symbol))
        for single_record in tqdm(p.imap_unordered(single_page_workder, left_symbol), total=len(left_symbol)):
            lock.acquire()
            output_dict.update(single_record)
            with open('cache', 'wb') as f:
                pickle.dump(output_dict, f)
            left_symbol = [s for s in symbol_list if s not in current_dict.keys()]
            lock.release()
        with open('cache', 'rb') as f:
            current_dict = pickle.load(f)

    rating_df = pd.DataFrame.from_dict(output_dict, 'index')
    rating_df.sort_values('rating', ascending=True, inplace=True)
    rating_df.to_csv('rating.csv')

