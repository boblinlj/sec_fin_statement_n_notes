import yfinance as yf
import pandas as pd
import numpy as  np
import datetime
import requests 
import random
import logging
from .database_management import DatabaseManagement
from .parallel_processing import parallel_process
from .configs import UA_LIST, PROXY
from warnings import simplefilter
simplefilter(action="ignore", category=pd.errors.PerformanceWarning)

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

    def __init__(self, stock, updated_dt=datetime.date.today()) -> None:
        if isinstance(stock, list):
            self.stock_list = [stock.upper() for stock in stock]
        else:
            self.stock_list = [stock]

        self.updated_dt = updated_dt

    def _get_info(self, stock) -> pd.DataFrame:
        session = requests.session()
        session.headers = {
            'user-agent': random.choice(UA_LIST),
            'Accept-Language': 'en-GB,en;q=0.9,en-US;q=0.8,zh-CN;q=0.7,zh;q=0.6,zh-TW;q=0.5',
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
            'Sec-Fetch-Dest': 'iframe',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'cross-site',
            'origin': 'https://google.com'
        }
        info_in_dict = yf.Ticker(stock).get_info(proxy=PROXY)
        df = pd.DataFrame.from_dict(info_in_dict, orient='index')
        df = df.transpose()
        df.set_index(keys=['symbol'], inplace=True)
        df.drop(columns=['address1', 'city','state','zip','phone','website', 'companyOfficers','maxAge', 'uuid']
                , axis=0
                , inplace=True
                , errors='ignore')
        
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

        df.drop(columns=[i for i in df.columns if i not in KEEP_COLUMNS], errors='ignore', inplace=True)
        df[[i for i in KEEP_COLUMNS if i not in df.columns]] = np.nan
        
        df['updated_dt'] = self.updated_dt
        df.reset_index(inplace=True)
        df.rename(columns={'symbol':'ticker'}, inplace=True)
        
        return df
    
    def parse(self, stock):
        stock_df = self._get_info(stock)
        DatabaseManagement(dataframe=stock_df
                            , target_table='yahoo_fundamental'
                            , insert_index=False).insert_dataframe_to_table()
    
    def run(self):
        parallel_process(self.stock_list, self.parse, n_jobs=30, use_tqdm=True)

if __name__ == '__main__':
    call = get_stock_info(['AACIW'], '9999-12-31')
    
    print(call._get_info('aapl'))