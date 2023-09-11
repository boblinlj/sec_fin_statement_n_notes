from .database_management import DatabaseManagement
import pandas as pd

class get_stock_population:
    def __init__(self) -> None:
        self.sql = """
                SELECT yahoo_ticker
                FROM nasdaq_universe
                """
    def parse(self):
        return DatabaseManagement(sql=self.sql).read_sql_to_df()['yahoo_ticker'].to_list()


if __name__ == '__main__':
    call = get_stock_population()
    print(call.parse())
        