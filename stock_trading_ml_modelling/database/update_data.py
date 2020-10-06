"""Functions for updating data in the prices database"""
import re

from stock_trading_ml_modelling.utils.data import overlap

from stock_trading_ml_modelling.database.models import Session as session
from stock_trading_ml_modelling.database.models.prices import Ticker, TickerMarket, DailyPrice, WeeklyPrice

def _update_df(df, DestClass, session=session):
    """Function for updating records from a dataframe"""
    #Get table columns
    tab_cols = DestClass.__table__.columns
    #Remove table name prefix
    tab_name = DestClass.__table__.name
    tab_cols = [re.sub(fr"^{tab_name}\.", "", str(c)) for c in tab_cols]
    cols = overlap([tab_cols, df.columns])
    session.bulk_update_mappings(DestClass, df[cols].to_dict(orient="records"))
    session.commit()
