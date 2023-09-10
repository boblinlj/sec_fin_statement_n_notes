from util.get_stock_financial import get_stock_financial
from util.get_stock_info import get_stock_info
from util.get_stock_earning import get_stock_earning
from util.get_stock_population import get_stock_population
from util.parallel_processing import parallel_process


def main():
    stocks = get_stock_population().parse()
    
    parallel_process(stocks, get_stock_info, 20, use_tqdm=True)