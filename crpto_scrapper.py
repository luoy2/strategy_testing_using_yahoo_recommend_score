import requests
import pandas as pd
from database_handler import mysql_client
from bs4 import BeautifulSoup
import re
import datetime
from tqdm import *
import logging
import time


def parse_numbers(number_string):
    result = None
    numbers = re.findall(r'\d+|\.|-|e', number_string)
    if numbers:
        try:
            result = float(''.join(numbers))
        except Exception as e:
            logging.exception(e)
    return result




def read_total_market_cap():
    #  read the market cap
    url_content = requests.get('https://coinmarketcap.com/all/views/all/')

    # parse the content using bs4
    soup = BeautifulSoup(url_content.text)
    total_market_cap_tag = soup.find_all('div', attrs={'id': "total_market_cap"})
    total_market_cap = parse_numbers(total_market_cap_tag[0].text)
    return total_market_cap


def crypto_main_scrapper():
    mysql_db = mysql_client(False)

    # read the detail table
    crpto_detail_table = pd.read_html('https://coinmarketcap.com/all/views/all/')[0]
    crpto_detail_table.drop('#', 1, inplace=True)

    # reformat the numbers
    columns = crpto_detail_table.columns
    for i in columns:
        if i not in ('Name', 'Symbol'):
            crpto_detail_table[i] = crpto_detail_table[i].apply(lambda x: parse_numbers(x))

    # adding total_market_cap to column
    crpto_detail_table['total_market_cap'] = read_total_market_cap()
    # adding modified timestamp
    crpto_detail_table['modified_time'] = datetime.datetime.now()



    output_list = crpto_detail_table.values.tolist()
    for i in tqdm(output_list):
        insert_statement = mysql_db.list_to_update_query('scrappers', 'CryptocurrencyMarketCap',i, 'replace')
        mysql_db.commit_query(insert_statement)




if __name__ == '__main__':
    logging.basicConfig(level=20, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    while 1:
        crypto_main_scrapper()
        time.sleep(60*60)

