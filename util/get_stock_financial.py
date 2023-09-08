import yfinance as yf
import pandas as pd
import numpy as  np
import datetime
from enter_to_database import DatabaseManagement

PROXY = "socks5://10.0.0.216:9050"

class get_stock_financial():
    
    def __init__(self, stock, update_dt = None) -> None:
        self.stock = stock
        self.updated_dt = update_dt
    
    def _get_income_statements(self, type = 'quarterly') -> pd.DataFrame:
        is_dic = yf.Ticker(self.stock).get_income_stmt(proxy=PROXY, as_dict=True, freq=type)
        is_df = pd.DataFrame.from_dict(is_dic, orient='index')
        return is_df

    def _get_balance_sheets(self, type = 'quarterly') -> pd.DataFrame:
        pass

    def _get_cashflow_statements(self, type = 'quarterly') -> pd.DataFrame:
        pass

    def _validate_data(self) -> None:
        pass
    
    def parse(self):
        pass
    
if __name__ == '__main__':
    call = get_stock_financial('AAPL', '9999-12-31')
    print(call._get_income_statements(type='quarterly'))