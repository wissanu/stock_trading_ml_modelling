import pandas as pd
from pathlib import Path
from sqlalchemy.orm import sessionmaker, scoped_session
from tqdm import tqdm

from stock_trading_ml_modelling.config import CONFIG
from sqlalchemy import create_engine
from stock_trading_ml_modelling.prices.get_data import all_df, fetch_tickers, fetch_ticker_markets, \
    fetch_daily_prices, fetch_weekly_prices
from stock_trading_ml_modelling.prices.add_data import add_df
from stock_trading_ml_modelling.models import engine, Session as session
from stock_trading_ml_modelling.models.prices import create_db, Ticker, TickerMarket, DailyPrice, WeeklyPrice

eng_old = create_engine(
    f'sqlite:///{str(Path(CONFIG["files"]["store_path"]) / "prices_old.db")}'
)
old_session = scoped_session(sessionmaker(bind=eng_old, expire_on_commit=False))

#Create the new db
create_db(engine)

#ticker
ticker_df = all_df(fetch_tickers(), session=old_session)
#add to the new database
add_df(ticker_df, Ticker)

#ticker_market
ticker_market_df = all_df(fetch_ticker_markets(), session=old_session)
#add to the new database
add_df(ticker_market_df, TickerMarket)

#daily_price
for id in tqdm(ticker_df.id, total=ticker_df.shape[0]):
    daily_price_df = all_df(fetch_daily_prices(ticker_ids=[id]), session=old_session)
    #add to the new database
    add_df(daily_price_df, DailyPrice)

#weekly_price
for id in tqdm(ticker_df.id, total=ticker_df.shape[0]):
    weekly_price_df = all_df(fetch_weekly_prices(ticker_ids=[id]), session=old_session)
    #add to the new database
    add_df(weekly_price_df, WeeklyPrice)

