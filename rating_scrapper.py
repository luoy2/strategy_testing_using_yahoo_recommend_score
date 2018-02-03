import re
import multiprocessing
from tqdm import tqdm
from selenium import webdriver
import time
import pandas as pd
import pickle
import logging
import threading
import database_handler
from multiprocessing.dummy import Pool as ThreadPool
import datetime
import time
import platform
import os

def selenium_render(source_html):
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument('headless')
    if platform.system() == 'Windows':
        driver = webdriver.Chrome('C:/Windows/chromedriver.exe')  # Optional argument, if not specified will search path.
    elif platform.system() == 'Darwin':
        chromedriver = "/usr/local/bin/chromedriver"
        os.environ["webdriver.chrome.driver"] = chromedriver
        driver = webdriver.Chrome(chromedriver, chrome_options=chrome_options)
    else:
        # chrome_options.add_argument('no-sandbox')
        driver = webdriver.Chrome('/usr/local/bin/chromedriver', chrome_options=chrome_options)
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
    driver.close()
    driver.quit()
    return htmlSource


def get_symbol_list():
    files = ['symbol/nasdaq.csv', 'symbol/nyse.csv']
    symbol_list = []
    for i in files:
        data_df = pd.read_csv(i)
        symbol = data_df['Symbol']
        symbol_list += symbol.values.tolist()
    return symbol_list


def single_page_workder(symbol):
    output_dict = {'rating': None,
                   'low': None,
                   'current': None,
                   'target': None,
                   'high': None,
                   'analyst_num': None}
    mysql_client = database_handler.mysql_client()
    try:
        url = 'https://finance.yahoo.com/quote/{0}/analysts?p={0}'.format(symbol)
        data = selenium_render(url)
        rating_pattern = re.compile(r'<div class="rating-text Arrow South.*>(?P<rating>\d+\.?\d*)</div>')
        price_pattern = re.compile(
            r'<div tabindex="0" aria-label="Low\s+(?P<low>\d+\.?\d*)\s+Current\s+(?P<current>\d+\.?\d*)\s+Average\s+('
            r'?P<target>\d+\.?\d*)\s+High\s+(?P<high>\d+\.?\d*)"><div class="')
        anayst_num_pattern = re.compile(r'Analyst Price Targets \((?P<analyst_num>\d+)\)')
        result = rating_pattern.finditer(data)
        rating_result_dict_list = [m.groupdict() for m in result]
        if rating_result_dict_list:
            rating_result_dict_list = rating_result_dict_list[0]
        else:
            rating_result_dict_list = {}
        output_dict.update(rating_result_dict_list)

        price_result = price_pattern.finditer(data)
        price_result_dict_list = [m.groupdict() for m in price_result]
        if price_result_dict_list:
            price_result_dict_list = price_result_dict_list[0]
        else:
            price_result_dict_list = {}
        output_dict.update(price_result_dict_list)

        anayst_num_result = anayst_num_pattern.finditer(data)
        anayst_num_result_dict_list = [m.groupdict() for m in anayst_num_result]
        if anayst_num_result_dict_list:
            anayst_num_result_dict_list = anayst_num_result_dict_list[0]
        else:
            anayst_num_result_dict_list = {}
        output_dict.update(anayst_num_result_dict_list)
    except Exception as e:
        logging.exception(e)
    if all([i for i in output_dict.values()]):
        insert_query = """
        INSERT INTO `scrappers`.`yahoo_finance_stock_rating`
            (`time`,
            `symbol`,
            `low`,
            `current`,
            `target`,
            `high`,
            `rating`,
            `anaylst_num`)
            VALUES
            ('{0}', '{1}', {2}, {3}, {4}, {5}, {6}, {7});            
        """.format(datetime.datetime.now(),
                   symbol,
                   output_dict['low'] if output_dict['low'] else "NULL",
                   output_dict['current'] if output_dict['current'] else "NULL",
                   output_dict['target'] if output_dict['target'] else "NULL",
                   output_dict['high'] if output_dict['high'] else "NULL",
                   output_dict['rating'] if output_dict['rating'] else "NULL",
                   output_dict['analyst_num'] if output_dict['analyst_num'] else "NULL")
        mysql_client.commit_query(insert_query)
    else:
        time.sleep(10)
    return


if __name__ == '__main__':
    logging.basicConfig(level=20, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    symbol_list = get_symbol_list()

    if platform.system() == 'Darwin':
        # mac high serria cannot run python multi processing
        while 1:
            for symbol in tqdm(symbol_list):
                single_page_workder(symbol)
    else:
        while 1:
            for symbol in tqdm(symbol_list):
                p = multiprocessing.Process(target=single_page_workder, args=(symbol,))
                p.start()
                p.join()
    # rating_df = pd.DataFrame.from_dict(output_dict, 'index')
    # rating_df.sort_values('rating', ascending=True, inplace=True)
    # rating_df.to_csv('rating.csv')

# if __name__ == '__main__':
#     logging.basicConfig(level=20, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
#     symbol_list = get_symbol_list()
#     output_dict = {}
#     try:
#         with open('cache', 'rb') as f:
#             current_dict = pickle.load(f)
#     except Exception as e:
#         logging.exception(e)
#         current_dict = {}
#     left_symbol = [s for s in symbol_list if s not in current_dict.keys()]
#     p = multiprocessing.Pool(processes=multiprocessing.cpu_count())
#     m = multiprocessing.Manager()
#     lock = m.Lock()
#     while left_symbol:
#         print(len(left_symbol))
#         for single_record in tqdm(p.imap_unordered(single_page_workder, left_symbol), total=len(left_symbol)):
#             lock.acquire()
#             output_dict.update(single_record)
#             with open('cache', 'wb') as f:
#                 pickle.dump(output_dict, f)
#             left_symbol = [s for s in symbol_list if s not in current_dict.keys()]
#             lock.release()
#         with open('cache', 'rb') as f:
#             current_dict = pickle.load(f)
#
#     rating_df = pd.DataFrame.from_dict(output_dict, 'index')
#     rating_df.sort_values('rating', ascending=True, inplace=True)
#     rating_df.to_csv('rating.csv')
#