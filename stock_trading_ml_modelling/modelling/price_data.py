
from datetime import datetime, timedelta

from stock_trading_ml_modelling.database import daily_price
from stock_trading_ml_modelling.database.get_data import sqlaq_to_df

class PriceData:
    def __init__(self):
        pass

    def get_prices(self, ticker_ids=[], weeks=52*10):
        """Function to fetch the pricing data"""
        #Get the price data
        prices = sqlaq_to_df(daily_price.fetch(ticker_ids=ticker_ids))
        #Limit dates
        st_date = (datetime.now() - timedelta(weeks=weeks)).date()
        prices = prices[prices.date > st_date]
        #Ensure they are in date order - async fetching may have prevented this
        prices = prices.sort_values(["ticker_id","date"]) \
            .reset_index(drop=True)
        return prices
