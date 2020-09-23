"""Functions fr scrapping data from websites"""
import re
from tqdm import tqdm
import pandas as pd

from stock_trading_ml_modelling.config import CONFIG
from stock_trading_ml_modelling.libs.logs import log
from stock_trading_ml_modelling.utils.scrape import get_soup, refine_soup
from stock_trading_ml_modelling.utils.str_formatting import clean_col_name

def scrape_num_pages(ref:str="ftse100"):
    #Fetch the data for ftse 100
    soup = get_soup(CONFIG["web_addr"][ref].format(1000))
    par_elem = refine_soup(soup, filter_li=[{"attrs":{"class":"paginator"}}])[0]
    last_page = refine_soup(par_elem, filter_li=[{"attrs":{"class":"page-number"}}])[-2] #-2 as last item is "go to last page"
    num_pages = int(re.sub('[^0-9]','', last_page.text))
    return num_pages

def scrape_tickers(num_pages, ref:str="ftse100"):
    row_li = []
    for page in tqdm(range(1, num_pages+1), total=len(range(1, num_pages+1))):
        web_addr = CONFIG["web_addr"][ref].format(page)
        soup = get_soup(web_addr)
        #Collect the table
        table_elem = refine_soup(soup, filter_li=[
            {"name":"section","class":"ftse-index-table-component"},
            {"name":"table"}
            ],
            obj_limit=[0,0])[0]
        #Collect the rows of data
        for row in refine_soup(table_elem, filter_li=[
            {"name":"tbody"},
            {"name":"tr"}
            ]):
            temp_row = []
            for cell in refine_soup(row, filter_li=[{"name":"td"}])[:2]:
                temp_row.append(re.sub('\n','', cell.text.upper()))
            row_li.append(temp_row)
    log.info(f'{ref} count -> {len(row_li)}')
    #Create a dataframe
    tick_ftse = pd.DataFrame(data=row_li, columns=['ticker','company'])
    tick_ftse['market'] = ref.upper()
    return tick_ftse

def scrape_prices(ticker, st_secs, en_secs, interval="1d"):
    #clean ticker
    ticker = re.sub('\.', '-', ticker) + '.L'
    web_addr = CONFIG["web_addr"]["share_price"].format(ticker,st_secs,en_secs,interval,interval)
    log.info(f"web_addr -> {web_addr}")
    #Get the soup
    soup = get_soup(web_addr)
    if soup == "":
        log.info('no results returned')
        #return to say that there has been an error
        return False, None
    #Grab the header rows
    header = refine_soup(soup, [
        {"name":"table","attrs":{'data-test':'historical-prices'}},
        {"name":"thead"},
        {"name":"tr"}
        ])[0]
    cols = [clean_col_name(th.text) for th in header]
    #Grab the data rows
    rows = refine_soup(soup, [
        {"name":"table","attrs":{'data-test':'historical-prices'}},
        {"name":"tbody"},
        {"name":"tr"}
        ])
    #If there are no dates there's no point going back further
    if len(rows) == 0:
        log.info('No more records to collect')
        return False, None
    #Put the rows into the dataframe
    data = []
    for r in rows:
        td = refine_soup(r,[{"name":'td'}])
        if len(td) == len(cols):
            data.append([x.text for x in td])
    tick_df = pd.DataFrame(data, columns=cols)
    
    return True, tick_df

def scrape_bank_holidays(year):
    web_addr = CONFIG["web_addr"]["holidays"].format(year)
    #Get the soup
    soup = get_soup(web_addr)
    if soup == "":
        log.info('no results returned')
        #return to say that there has been an error
        return []
    #Get the data rows
    rows = refine_soup(soup, [
        {"name":"table"},
        {"name":"tbody"},
        {"name":"tr"},
        ])
    #Grab the dates
    dates = [refine_soup(r, [{"name":"td"},{"name":"span"}]) for r in rows]
    dates = [d[0].text for d in dates if len(d) > 0]
    #Grab the labels
    labels = [refine_soup(r, [{"name":"td"},{"name":"a"}]) for r in rows]
    labels = [l[0].text for l in labels if len(l) > 0]
    return list(zip(dates,labels))
