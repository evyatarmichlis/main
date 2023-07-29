import mysql.connector
import pandas as pd


class MySQL:
    def __init__(self) -> None:
        self.db = mysql.connector.connect(
            host="sql7.freesqldatabase.com",
            user="sql7636093",
            password="yx6aDgSuzq",
            database="sql7636093"
        )
        self.cursor = self.db.cursor()

    def enter_query(self, sql):
        self.cursor.execute(sql)
        self.db.commit()

    def insert_df(self, df, table_name='items'):
        df.to_sql(name=table_name, con=self.db,
                  if_exists='append', index=False)



# if __name__ == 'main'():
#     handler = MySQL()
#     results = handler.enter_query(
#         'insert into items(franchise, name, price, category, store_id, item_id, subgroup) values("omer", "omer", 2, "asd", 1, "5", "sd")')
#     print('Done')
