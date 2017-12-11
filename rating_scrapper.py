from urllib import request
import re
import multiprocessing
from tqdm import tqdm
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException
import time
import pandas as pd
import pickle


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

def qt_render(source_html):
    """Fully render HTML, JavaScript and all."""

    import sys
    from PyQt5.QtWidgets import QApplication
    from PyQt5.QtWebEngineWidgets import QWebEngineView

    class Render(QWebEngineView):
        def __init__(self, html):
            self.html = None
            self.app = QApplication(sys.argv)
            QWebEngineView.__init__(self)
            self.loadFinished.connect(self._loadFinished)
            self.setHtml(html)
            self.app.exec_()

        def _loadFinished(self, result):
            # This is an async call, you need to wait for this
            # to be called before closing the app
            self.page().toHtml(self.callable)

        def callable(self, data):
            self.html = data
            # Data has been stored, it's safe to quit the app
            self.app.quit()

    return Render(source_html).html

def get_symbol_list():
    files = ['symbol/nasdaq.csv', 'symbol/nyse.csv']
    symbol_list = []
    for i in files:
        data_df = pd.read_csv(i)
        symbol = data_df['Symbol']
        symbol_list+=symbol.values.tolist()
    return symbol_list

def single_page_workder(symbol):
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
    return output_dict

if __name__ == '__main__':
    symbol_list = get_symbol_list()
    symbol_list = symbol_list
    p = multiprocessing.Pool(processes=multiprocessing.cpu_count() * 2)
    output_dict = {}
    for single_record in tqdm(p.imap_unordered(single_page_workder, symbol_list), total=len(symbol_list)):
        output_dict.update(single_record)
    f = open('cache', 'wb')
    pickle.dump(output_dict, f)
    rating_df = pd.DataFrame.from_dict(output_dict, 'index')
    rating_df.sort_values('rating', ascending=True, inplace=True)
    rating_df.to_csv('rating.csv')

