"""File to fetch data from stock_trading_ml_modelling.database database"""
import pandas as pd
from sqlalchemy import func, and_

from stock_trading_ml_modelling.config import CONFIG
from stock_trading_ml_modelling.database.models.prices import Ticker, TickerMarket, DailyPrice, WeeklyPrice
from stock_trading_ml_modelling.database.models import Session as session

#Converting query to dataframe
def sqlaq_to_df(query, session=session):
    out_df = pd.read_sql(query.statement, con=session.bind)
    return out_df

def sqlaq_to_df_first(query, session=session):
    out_df = pd.read_sql(query.statement, con=session.bind)
    return out_df.iloc[0]