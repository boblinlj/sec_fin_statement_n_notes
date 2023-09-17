from util.get_stock_financial import get_stock_financial
from util.get_stock_info import get_stock_info
from util.get_stock_earning import get_stock_earning
from util.get_stock_population import get_stock_population
from util.parallel_processing import parallel_process
from datetime import date


def main(option):
    stocks = get_stock_population().parse()[:]
    run_dt=date.today()
    if option.lower() == 'stock_info':
        get_stock_info(stock=stocks, updated_dt=run_dt).run()
    elif option.lower() == 'stock_financial':
        get_stock_financial(stock=stocks, updated_dt=run_dt).run()
    elif option.lower() == 'stock_earning':
        get_stock_earning(stock=stocks, updated_dt=run_dt).run()
    else:
        f'Please provide one of below: 1)stock_info, 2)stock_financial, 3)stock_earning'
        

if __name__ == '__main__':
    main(option='stock_earning')