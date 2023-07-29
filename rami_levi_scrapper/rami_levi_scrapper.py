import pandas as pd
import requests

import mysql.connector

db_config_ = {
    "host": "sql7.freesqldatabase.com",
    "user": "sql7636093",
    "database": "sql7636093",
    "password": "yx6aDgSuzq"
}

def upload_df_to_mysql(dataframe, table_name, db_config=None):
    if db_config is None:
        db_config = db_config_

    try:
        connection = mysql.connector.connect(**db_config)
        dataframe.to_sql(name=table_name, con=connection, if_exists='replace', index=False)

        print(f"DataFrame uploaded to table '{table_name}' in MySQL database successfully.")

    except Exception as error:
        print(f"Error: {error}")

    finally:
        connection.close()



url = 'https://www.rami-levy.co.il/api/catalog?=null'
headers = {
    'Content-Type': 'application/json',
    'Cookie': 'auth.strategy=local; i18n_redirected=he'
}


def create_payload(d, store, from_):
    data = {
        'd': d,
        'store': store,
        'from': from_
    }
    return data

def fetch_all_products(store_id=331, limit=300):
    all_products = []
    init_limit = False
    for d in range(0, 1):
        for f in range(0, limit, 30):
            response = requests.post(url, headers=headers, json=create_payload(d, store=store_id, from_=f))
            if response.status_code == 200:
                response_data = response.json()
                if not init_limit:
                    limit = response_data["total"]
                    init_limit = True
                data = response_data["data"]
                all_products.append(data)
    df = pd.DataFrame([item for sublist in all_products for item in sublist])
    df = df[["name", "price", "id", "group", "subGroup"]]
    upload_df_to_mysql(df,"items",)

fetch_all_products()

# import pandas as pd
# import requests
#
# url = 'https://www.rami-levy.co.il/api/catalog?=null'
# headers = {
#     'Content-Type': 'application/json',
#     'Cookie': 'auth.strategy=local; i18n_redirected=he'
# }
#
# def create_payload(d, store, from_):
#     data = {
#         'd': d,
#         'store': store,
#         'from': from_
#     }
#     return data
#
# def fetch_all_products(store_id=331, max_depth=300):
#     all_products = []
#     limit = 600
#     init_limit = False
#
#     for d in range(0, max_depth):
#         for f in range(0, limit, 30):
#             response = requests.post(url, headers=headers, json=create_payload(d, store=store_id, from_=f))
#
#             if response.status_code == 200:
#                 response_data = response.json()
#
#                 if not init_limit:
#                     limit = response_data["total"]
#                     init_limit = True
#
#                 data = response_data["data"]
#                 all_products.append(data)
#
#     return all_products
#
# def main():
#     store_id = 331
#     max_depth = 300
#
#     all_products = fetch_all_products(store_id=store_id, max_depth=max_depth)
#
#     if all_products:
#         df = pd.DataFrame(all_products)
#         print(df)
#     else:
#         print("No products found!")
#
# if __name__ == "__main__":
#     main()
