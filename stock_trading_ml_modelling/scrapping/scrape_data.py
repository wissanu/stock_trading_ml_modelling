import pandas as pd
import re
import datetime as dt

from stock_trading_ml_modelling.libs.logs import log
from stock_trading_ml_modelling.utils.str_formatting import zero_pad_single
from stock_trading_ml_modelling.config import CONFIG

from stock_trading_ml_modelling.scrapping.scrapes import scrape_num_pages, scrape_tickers, scrape_bank_holidays

def get_tickers():
    log.info("\nSCRAPPING TICKERS")
    #This section will scrap the ticker values for the FTSE 100 and FTSE 250 and store them in dataframes 'tick_ftse100' and 'tick_ftse250'.
    #Finally concatenate into 1 dataframe 'tick_ftse'.

    #Fetch the data for ftse 100
    num_pages = scrape_num_pages("ftse100")

    #Collect the rows of data
    log.info("\nFTSE 100")
    tick_ftse100 = scrape_tickers(num_pages, "ftse100", )

    #Fetch the data for ftse 250
    num_pages = scrape_num_pages("ftse250")

    #Collect the rows of data
    log.info("\nFTSE 250")
    tick_ftse250 = scrape_tickers(num_pages, "ftse250", )

    #Combine into 1 dataframe
    tick_ftse = pd.concat([tick_ftse100,tick_ftse250])
    tick_ftse.sort_values(['ticker'])
    tick_ftse['ticker'] = [re.sub('(?=[0-9A-Z])*\.(?=[0-9A-Z]+)','-',tick) for tick in tick_ftse['ticker']]
    tick_ftse['ticker'] = [re.sub('[^0-9A-Z\-]','',tick) for tick in tick_ftse['ticker']]
    tick_ftse['last_seen_date'] = dt.date.today()

    return tick_ftse

def get_public_holidays(year):
    """Function for getting bank holidays and converting to datetime objects"""
    dates = scrape_bank_holidays(year, )
    #Keep the public holiday dates
    dates = [d for d in dates if d[1] in CONFIG["public_holidays"]]
    #Convert numbers to zero padded
    dates = [zero_pad_single(d[0]) for d in dates]
    #Convert the dates
    dates = [dt.datetime.strptime(f'{d} {year}', r"%B %d %Y") for d in dates]
    return dates
