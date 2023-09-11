import yfinance as yf
import pandas as pd
import datetime
from .database_management import DatabaseManagement

pd.set_option('display.max_columns',500)
pd.set_option('display.max_rows',500)
PROXY = "socks5://10.0.0.216:9050"

class get_stock_earning:
    def __init__(self, stock, updated_date=datetime.date.today()) -> None:
        self.stock = stock
        self.updated_dt = updated_date
        self.output_df = pd.DataFrame()
        
    def _get_earning(self):
        earn_df = yf.Ticker(self.stock).get_earnings_dates(proxy=PROXY, limit=12)
        return earn_df
    
    def _validate_data(self, df) -> None:
        self.output_df = df.copy()
        self.output_df.reset_index(inplace=True)
        self.output_df.rename(columns={'Earnings Date':'earning_date',
                                       'EPS Estimate':'esp_est',
                                       'Reported EPS':'esp_actual',
                                       'Surprise(%)':'esp_surprise'}, inplace=True)
        self.output_df['ticker'] = self.stock
        self.output_df['updated_dt'] = self.updated_dt
        self.output_df['earning_date'] = pd.to_datetime(self.output_df['earning_date']).dt.date
        self.output_df.dropna(subset=['esp_est','esp_actual','esp_surprise'], how='any', inplace=True)
    
    def parse(self):
        self._validate_data(self._get_earning())
        return self.output_df
    
    def insert_to_db(self):
        self._validate_data(self._get_earning())
        DatabaseManagement(dataframe=self.output_df
                           , target_table='yahoo_earnings'
                           , insert_index=False).insert_dataframe_to_table()
        
    def __call__(self):
        self.insert_to_db()
        
if __name__ == "__main__":
    call = get_stock_earning('AAPL', '9999-12-31')()