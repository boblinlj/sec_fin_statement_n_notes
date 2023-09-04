import yfinance as yf
import pandas as pd

pd.set_option('display.max_columns',500)

class get_stock_info():
    
    def __init__(self, stock) -> None:
        self.stock = stock

    def _get_info(self) -> pd.DataFrame:
        info_in_dict = yf.Ticker(self.stock).info
        df = pd.DataFrame.from_dict(info_in_dict, orient='index')
        return df
    
    def _validate_data(self, df) -> pd.DataFrame:
        df = df.transpose()
        df.set_index(keys=['symbol'], inplace=True)
        df.drop(columns=['address1', 'city','state','zip','phone','website', 'companyOfficers','maxAge', 'uuid']
                , axis=0
                , inplace=True
                , errors='raise')
        if 'firstTradeDateEpochUtc' in df.columns:
            df['firstTradeDateEpochUtc'] = pd.to_datetime(df['firstTradeDateEpochUtc'], unit='s').dt.date
        
        if 'lastDividendDate' in df.columns:
            df['lastDividendDate'] = pd.to_datetime(df['lastDividendDate'], unit='s').dt.date
            
        return df
    
    def parse(self):
        return self._validate_data(self._get_info())
        

if __name__ == '__main__':
    call = get_stock_info('MS')
    print(call.parse())