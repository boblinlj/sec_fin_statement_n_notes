import yfinance as yf
import pandas as pd
import numpy as  np
import datetime
from enter_to_database import DatabaseManagement
pd.set_option('display.max_columns',500)
pd.set_option('display.max_rows',500)
PROXY = "socks5://10.0.0.216:9050"

class get_stock_financial():
    
    def __init__(self, stock, update_dt = None) -> None:
        self.stock = stock
        self.updated_dt = update_dt
        self.output_df = pd.DataFrame()
    
    def _get_income_statements(self, type = 'quarterly') -> pd.DataFrame:
        is_dic = yf.Ticker(self.stock).get_income_stmt(proxy=PROXY, as_dict=True, freq=type)
        is_df = pd.DataFrame.from_dict(is_dic, orient='index')
        return is_df

    def _get_balance_sheets(self, type = 'quarterly') -> pd.DataFrame:
        bs_dic = yf.Ticker(self.stock).get_balance_sheet(proxy=PROXY, as_dict=True, freq=type)
        bs_df = pd.DataFrame.from_dict(bs_dic, orient='index')
        return bs_df

    def _get_cashflow_statements(self, type = 'quarterly') -> pd.DataFrame:
        cs_dic = yf.Ticker(self.stock).get_cash_flow(proxy=PROXY, as_dict=True, freq=type)
        cs_df = pd.DataFrame.from_dict(cs_dic, orient='index')
        return cs_df

    def _validate_data(self, df) -> None:
        df['updated_dt'] = self.updated_dt
        return df
    
    def parse(self):
        self.output_df = pd.concat([self._get_balance_sheets('quarterly')
                                    ,self._get_income_statements('quarterly')
                                    ,self._get_cashflow_statements('quarterly')]
                                   ,axis=1
                                   )
            
    def insert_to_db(self):
        pass
    
if __name__ == '__main__':
    call = get_stock_financial('AAPL', '9999-12-31')
    call.parse()
    print(call.output_df)