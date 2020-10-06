"""Script for scrapping the stock price histories of stock accross the 
FTSE 350

# Daily web scrape of stock prices
This code accesses site to retrieve stock price information daily ad add it to the h5 file. The steps are:
1. Get FTSE 100 and FTSE 250 lists from wikipedia, then compile into one list.
2. open pipe to sql file holding current price data.
3. Loop though the tickers and for each one; 
    1. find the most recent price date.
    2. convert this into a time stamp to be used on Yahoo finance.
    3. go to Yahoo Finance and get all the prices between the last time stamp and the current timestamp.
4. Add these new prices to the sql file.

The sources of data are:
- https://www.londonstockexchange.com/exchange/prices-and-markets/stocks/indices/summary/summary-indices-constituents.html?index=NMX&page=1 -> list of FTSE 350 company stock tickers
- https://finance.yahoo.com/quote/{stock-ticker}/history?period1={start-time-mark}&period2={end-time-mark}&interval={interval}&filter=history&frequency={frequency} -> Example web address to retrieve information from Yahoo finance
    - Data on this page is scroll loaded so many time indexes must be used toretrieve the dcorrect data
    - Up to 145 records can be seen from initial page load note that this includes dividends so limit to 140 for safety

The inputs required for scrapping are:
 - {stock-ticker} -> this is the ticker taken from wiki with '.L' appended to it
 - {start-tme-mark} -> This is the time in seconds since 01/01/1970 at which you would like the data retrieval to start, data retrieved is inclusive of this time
 - {end-tme-mark} -> This is the time in seconds since 01/01/1970, data retrieved is inclusive of this time
 - {interval} & {frequency} -> This is the interval for which values are given, the two must match
     - 1d = 1 every 1 days
     - 1wk = 1 every week
     - 1mo = 1 eveery month
"""
import pandas as pd
import numpy as np
from pathlib import Path
import re
import datetime as dt
import os
from tqdm import tqdm

from stock_trading_ml_modelling.config import CONFIG
from stock_trading_ml_modelling.libs.logs import log
from stock_trading_ml_modelling.utils.timing import ProcessTime
from stock_trading_ml_modelling.utils.date import calc_en_date, calc_st_date
from stock_trading_ml_modelling.database.get_data import sqlaq_to_df
from stock_trading_ml_modelling.database import ticker, daily_price, weekly_price
from stock_trading_ml_modelling.database.models import Session as session

from stock_trading_ml_modelling.scrapping.scrape_data import get_tickers
from stock_trading_ml_modelling.libs.scrapping import process_daily_prices, process_weekly_prices
from stock_trading_ml_modelling.scrapping.database import create_new_tickers, create_new_ticker_markets

def full_scrape():
    """Function to perform a full scrape of all available prices"""

    #########################
    ### SCRAPPING TICKERS ###
    #########################
    #scrape the tickers
    tick_ftse = get_tickers()
    #create new records in the ticker table
    tick_db = create_new_tickers(tick_ftse, )
    #update ticker records with last seen date
    tick_ftse = pd.merge(tick_ftse, tick_db[["ticker","id"]], on="ticker")
    ticker.update_df(tick_ftse)
    #create new records in the ticker_market table
    _ = create_new_ticker_markets(tick_ftse, )
    #Create a list of ticker ids
    ticker_ids = tick_ftse.id.to_list()

    ####################
    ### DAILY PRICES ###
    ####################
    log.info("\nSCRAPPING DAILY PRICES")

    #Make a call for all the latest dates
    latest_dates_df = sqlaq_to_df(daily_price.fetch_latest(session, ticker_ids=ticker_ids))
    latest_dates_df["max_date"] = latest_dates_df.max_date.astype("datetime64")
    #Calc the en_date for today
    en_date = calc_en_date()
    if str(CONFIG['web_scrape']['mode']).lower() == 'update':
        latest_dates_df["st_date"] = [calc_st_date(v) for v in latest_dates_df.max_date]
    else:
        latest_dates_df["st_date"] = dt.datetime(1970,1,1)
        #Delete existing data
        daily_price.remove()

    #Loop through the tickers in tick_ftse and for each one get the latest date of scrape.
    #Convert this date into a timestamp.
    #Scrape all new data and add to the database.
    dp_errors = []
    run_time = ProcessTime()
    for _,r in tqdm(latest_dates_df.iterrows(), total=latest_dates_df.shape[0], desc="Scrape daily prices"):
        log.info(f'\n{len(run_time.lap_li)} RUNNING FOR -> {r.id}, {r.ticker}')
        log.info(f'Latst date - {r.max_date}')
        try:
            #Get new price data if neccesary and add/update the database
            process_daily_prices(
                r.ticker,
                r.id,
                st_date=r.st_date,
                en_date=en_date,
                split_from_date=r.max_date,
                split_to_date=None
                )
        except Exception as e:
            log.error(e)
            dp_errors.append({'ticker':r.ticker, "error":e})
        #Lap
        log.info(run_time.lap())
        log.info(run_time.show_latest_lap_time(show_time=True))
    log.info(f"DAILY SCRAPE RUN TIME - {run_time.end()}")

    #####################
    ### WEEKLY PRICES ###
    #####################
    log.info("\nSCRAPPING WEEKLY PRICES")

    #Make a call for all the latest dates
    latest_dates_df = sqlaq_to_df(weekly_price.fetch_latest(session, ticker_ids=ticker_ids))
    latest_dates_df["max_date"] = latest_dates_df.max_date.astype("datetime64")

    #Loop through the tickers in tick_ftse and for each one get the latest date of scrape.
    #Convert this date into a timestamp.
    #Scrape all new data and add to the database.
    wp_errors = []
    run_time = ProcessTime()
    for _,r in tqdm(latest_dates_df.iterrows(), total=latest_dates_df.shape[0], desc="Process weekly prices"):
        log.info(f'\n{len(run_time.lap_li)} RUNNING FOR -> {r.id}, {r.ticker}')
        try:
            #Get new price data if neccesary
            if r.max_date < en_date:
                process_weekly_prices(
                    r.id,
                    split_from_date=r.max_date,
                    
                    )
            else:
                log.info('No new records to collect')
                continue
        except Exception as e:
            log.error(e)
            wp_errors.append({'ticker':r.ticker,"error":e})
        #Lap
        log.info(run_time.lap())
        log.info(run_time.show_latest_lap_time(show_time=True))
    log.info('\n\n')
    log.info(f"WEEKLY SCRAPE RUN TIME - {run_time.end()}")

    ####################
    ### PRINT ERRORS ###
    ####################

    log.info(f'\nDAILY ERROR COUNT -> {len(dp_errors)}')
    if len(dp_errors) > 0:
        log.info('DALIY ERRORS ->')
        for e in dp_errors:
            log.error(e)

    log.info(f'\nWEEKLY ERROR COUNT -> {len(wp_errors)}')
    if len(wp_errors) > 0:
        log.info('WEEKLY ERRORS ->')
        for e in wp_errors:
            log.error(e)
