import yfinance as yf
import pandas as pd
import numpy as  np
import datetime
from enter_to_database import DatabaseManagement

pd.set_option('display.max_columns',500)

KEEP_COLUMNS = ['sharesOutstanding',
                'enterpriseToRevenue',
                'enterpriseToEbitda',
                'forwardPE',
                'trailingPE',
                'priceToBook',
                'enterpriseValue',
                'priceToSalesTrailing12Months',
                'pegRatio',
                'marketCap',
                'shortName',
                'totalRevenue',
                'revenueGrowth',
                'revenueQuarterlyGrowth',
                'ebitda',
                'totalAssets',
                'totalCash',
                'totalDebt',
                'operatingCashflow',
                'freeCashflow',
                'revenuePerShare',
                'bookValue',
                'forwardEps',
                'trailingEps',
                'netIncomeToCommon',
                'profitMargins',
                'ebitdaMargins',
                'grossMargins',
                'operatingMargins',
                'grossProfits',
                'currentRatio',
                'returnOnAssets',
                'totalCashPerShare',
                'quickRatio',
                'payoutRatio',
                'debtToEquity',
                'returnOnEquity',
                'beta',
                'beta3Year',
                'floatShares',
                'sharesShort',
                '52WeekChange',
                'sharesPercentSharesOut',
                'heldPercentInsiders',
                'heldPercentInstitutions',
                'shortRatio',
                'shortPercentOfFloat',
                'sharesShortPreviousMonthDate',
                'sharesShortPriorMonth',
                'lastFiscalYearEnd',
                'nextFiscalYearEnd',
                'mostRecentQuarter',
                'fiveYearAverageReturn',
                'twoHundredDayAverage',
                'volume24Hr',
                'averageDailyVolume10Day',
                'fiftyDayAverage',
                'averageVolume10days',
                'SandP52WeekChange',
                'dateShortInterest',
                'regularMarketVolume',
                'averageVolume',
                'averageDailyVolume3Month',
                'volume',
                'fiftyTwoWeekHigh',
                'fiveYearAvgDividendYield',
                'fiftyTwoWeekLow',
                'currentPrice',
                'previousClose',
                'regularMarketOpen',
                'regularMarketPreviousClose',
                'open',
                'dayLow',
                'dayHigh',
                'regularMarketDayHigh',
                'postMarketChange',
                'postMarketPrice',
                'preMarketChange',
                'regularMarketPrice',
                'preMarketChangePercent',
                'postMarketChangePercent',
                'regularMarketChange',
                'regularMarketChangePercent',
                'preMarketPrice',
                'targetLowPrice',
                'targetMeanPrice',
                'targetMedianPrice',
                'targetHighPrice',
                'dividendYield',
                'lastDividendValue',
                'lastSplitDate',
                'lastDividendDate',
                'lastSplitFactor',
                'earningsGrowth',
                'numberOfAnalystOpinions',
                'trailingAnnualDividendYield',
                'trailingAnnualDividendRate',
                'dividendRate',
                'exDividendDate']

class get_stock_info():
    
    def __init__(self, stock, updated_dt=None) -> None:
        self.stock = stock
        self.output_df = pd.DataFrame()
        if updated_dt is None:
            self.updated_dt = datetime.date.today()
        else:
            self.updated_dt = updated_dt

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
            
        if 'sharesShortPreviousMonthDate' in df.columns:
            df['sharesShortPreviousMonthDate'] = pd.to_datetime(df['sharesShortPreviousMonthDate'], unit='s').dt.date
        
        if 'lastFiscalYearEnd' in df.columns:
            df['lastFiscalYearEnd'] = pd.to_datetime(df['lastFiscalYearEnd'], unit='s').dt.date
            
        if 'nextFiscalYearEnd' in df.columns:
            df['nextFiscalYearEnd'] = pd.to_datetime(df['nextFiscalYearEnd'], unit='s').dt.date

        if 'mostRecentQuarter' in df.columns:
            df['mostRecentQuarter'] = pd.to_datetime(df['mostRecentQuarter'], unit='s').dt.date

        if 'lastSplitDate' in df.columns:
            df['lastSplitDate'] = pd.to_datetime(df['lastSplitDate'], unit='s').dt.date

        if 'exDividendDate' in df.columns:
            df['exDividendDate'] = pd.to_datetime(df['exDividendDate'], unit='s').dt.date
                                
        for e in KEEP_COLUMNS:
            if e in df.columns:
                self.output_df = pd.concat([self.output_df, df[e]], axis=1)
            else:
                self.output_df[e] = np.nan
        
        self.output_df['updated_dt'] = self.updated_dt
        self.output_df.reset_index(inplace=True)
        self.output_df.rename(columns={'index':'ticker'}, inplace=True)
    
    def parse(self):
        return self._validate_data(self._get_info())
    
    def insert_to_db(self):
        self._validate_data(self._get_info())
        DatabaseManagement(dataframe=self.output_df
                           , target_table='yahoo_fundamental'
                           , insert_index=False).insert_dataframe_to_table()
        

if __name__ == '__main__':
    call = get_stock_info('MS', '9999-12-31')
    call.insert_to_db()