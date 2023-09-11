from util.get_stock_financial import get_stock_financial
from util.get_stock_info import get_stock_info
from util.get_stock_earning import get_stock_earning
from util.get_stock_population import get_stock_population
from util.parallel_processing import parallel_process
from datetime import date


def main():
    stocks = get_stock_population().parse()[:]
    get_stock_info(stock=stocks, updated_dt=date.today()).run()

if __name__ == '__main__':
    main()