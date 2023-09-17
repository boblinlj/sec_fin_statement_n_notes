import yfinance as yf
import pandas as pd
import datetime
from .database_management import DatabaseManagement
from .database_management import DatabaseManagement
from .parallel_processing import parallel_process

from warnings import simplefilter
# simplefilter(action="ignore", category=pd.errors.PerformanceWarning)
simplefilter(action="ignore")

PROXY = "socks5://10.0.0.216:9050"

class get_stock_earning:
    def __init__(self, stock, updated_dt=datetime.date.today()) -> None:
        if isinstance(stock, list):
            self.stock_list = [stock.upper() for stock in stock]
        else:
            self.stock_list = [stock]
        self.updated_dt = updated_dt
        
    def _get_earning(self, stock):
        earn_df = yf.Ticker(stock).get_earnings_dates(proxy=PROXY, limit=12)
        
        if earn_df is None:
            return pd.DataFrame()
        
        earn_df.reset_index(inplace=True)
        earn_df.rename(columns={'Earnings Date':'earning_date',
                                       'EPS Estimate':'esp_est',
                                       'Reported EPS':'esp_actual',
                                       'Surprise(%)':'esp_surprise'}, inplace=True)
        earn_df['ticker'] = stock
        earn_df['updated_dt'] = self.updated_dt
        earn_df['earning_date'] = pd.to_datetime(earn_df['earning_date']).dt.date
        earn_df.dropna(subset=['esp_est','esp_actual','esp_surprise'], how='any', inplace=True)
        
        return earn_df
    
    def parse(self, stock):
        stock_df = self._get_earning(stock)
        if stock_df is None: pass
        else:
            DatabaseManagement(dataframe=stock_df
                                , target_table='yahoo_earnings'
                                , insert_index=False).insert_dataframe_to_table()
    
    def run(self):
        parallel_process(self.stock_list, self.parse, n_jobs=30, use_tqdm=True)
        
if __name__ == "__main__":
    call = get_stock_earning('AAPL', '9999-12-31').run()