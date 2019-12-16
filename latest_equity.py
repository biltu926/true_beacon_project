import os
import traceback
import logging
import requests as r
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime

import configuration as cfg

logging.basicConfig(level=logging.INFO,
                    format='%(levelname)s : %(threadName)s : %(message)s',
                    filename=cfg.log_file_name,
                    filemode='w')
logger = logging.getLogger('latest_equity_logger')

import db_utility as db

def download_bhav_copy(url, download_at):
    try:
        response = r.get(url)
        soup = BeautifulSoup(response.content)
        links = soup.find_all('a', href=True)
        today = datetime.today()
        file_name_extension = 'ISINCODE_'+str(today.day)+str(today.month)+str(today.year)[2:]+'.zip'
        eq_bhav_copy_file_link = [x.get('href') for x in links if x.get('href').endswith(cfg.test_extension)][0]
        bhav_copy_response = r.get(eq_bhav_copy_file_link)

        with open(os.path.join(download_at, cfg.bhav_copy), 'wb') as file:
            file.write(bhav_copy_response.content)
            logger.info(f'bhav_copy zip file download completed')

    except (r.HTTPError, FileNotFoundError) as e:
        if cfg.verbose:
            print(traceback.format_exc())
        logger.error(e)

def read_bhav_copy_in_db(file_path):
    logger.info(f'Reading bhav_copy from {file_path}')
    bhav_copy_df = pd.read_csv(file_path)
    no_of_records = 0
    for col in bhav_copy_df.itertuples():
        no_of_records += 1
        query_string, values = '''insert into public."Test_Stocks" (SC_CODE, SC_NAME, SC_GROUP,
         SC_TYPE, OPEN, HIGH, LOW, CLOSE, LAST, PREVCLOSE, NO_TRADES, NO_OF_SHRS,
         NET_TURNOV, TDCLOINDI, ISIN_CODE, TRADING_DATE) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);''',\
        (col.SC_CODE, col.SC_NAME, col.SC_GROUP,
         col.SC_TYPE, col.OPEN, col.HIGH, col.LOW, col.CLOSE, col.LAST,
         col.PREVCLOSE, col.NO_TRADES, col.NO_OF_SHRS, col.NET_TURNOV, col.TDCLOINDI,
         col.ISIN_CODE, col.TRADING_DATE)
        db.db_push(query_string, values)

if __name__ == '__main__':
    download_loc = cfg.download_at
    url = cfg.bse_web_url
    #download_bhav_copy(url, download_loc)
    read_bhav_copy_in_db(os.path.join(cfg.download_at, cfg.bhav_copy))