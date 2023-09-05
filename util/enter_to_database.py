from sqlalchemy import create_engine
import pandas as pd

class DatabaseManagementError(Exception):
    pass

class DatabaseManagement:
    def __init__(self, dataframe = None, target_table = None, insert_index = False) -> None:
        self.dataframe = dataframe
        self.target_table = target_table
        self.insert_index = insert_index
        
        self.database_ip = '10.0.0.123'
        self.database_user = 'boblinlj'
        self.database_pw = 'Zuodan199064!'
        self.database_port = 3306
        self.database_nm = 'financial'
    
        self.cnn = create_engine(
                                f"""mysql+mysqlconnector://{self.database_user}"""
                                f""":{self.database_pw}"""
                                f"""@{self.database_ip}"""
                                f""":{self.database_port}"""
                                f"""/{self.database_nm}""",
                                pool_size=20,
                                max_overflow=0)
            
    def insert_dataframe_to_table(self):
        try:
            if self.dataframe is None or self.dataframe.empty:
                pass
            elif self.target_table is None:
                pass
            else:
                print(self.dataframe)
                self.dataframe.to_sql(name = self.target_table
                                      ,con = self.cnn
                                      ,if_exists = 'append'
                                      ,index=self.insert_index)
        except Exception as e:
            raise DatabaseManagementError(f'Failed to insert data into {self.target_table}, {e}')
        
if __name__ == '__main__':
    call = DatabaseManagement()
    call.insert_dataframe_to_table()