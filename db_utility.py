import traceback
import psycopg2 as pgs
import configuration as cfg
import logging
logging.basicConfig(level=logging.INFO,
                    format='%(levelname)s : %(threadName)s : %(message)s',
                    filename=cfg.log_file_name,
                    filemode='w')
logger = logging.getLogger('db_util_logger')

def db_connect():
    connection = None
    try:
        connection = pgs.connect(**cfg.db_params)
    except (pgs.Error) as e:
        if cfg.verbose:
            logger.error(traceback.format_exc())
        logger.error(e)
    return connection

def db_create():
    command = '''CREATE TABLE public."Test_Stocks" (
    SC_CODE integer, SC_NAME character varying(50), SC_GROUP character varying(10),
    SC_TYPE character varying(10), OPEN float(4), HIGH float(4), LOW float(4),
    CLOSE float(4), LAST float(4),
    PREVCLOSE float(4), NO_TRADES integer, NO_OF_SHRS integer,
    NET_TURNOV float(4), TDCLOINDI character varying(20)
    , ISIN_CODE character varying(30), TRADING_DATE date
    );'''
    try:
        if connection:
            cursor = connection.cursor()
            cursor.execute(command)
            cursor.close()
            connection.commit()
    except pgs.Error as e:
        print(e)

def db_push(query_string, values):
    try:
        if connection:
            cursor = connection.cursor()
            cursor.execute(query_string, values)
            cursor.close()
            connection.commit()
    except pgs.Error as e:
        logger.error(e)

def db_pull():
    query = ''' select * from public."Employee" '''
    if connection:
        cursor = connection.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        for row in rows:
            print(row)
        cursor.close()

connection = db_connect()