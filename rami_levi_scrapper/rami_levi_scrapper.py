import time

import pandas as pd
import requests


from mysql_handler import MySQL

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
    df["franchise"] = "rami_levi"
    df["timestamp"] = time.time()
    df["store_id"] = store_id
    df = df.rename({"group":"category","id":"item_id","subGroup":"subgroup"})

    MySQL.insert_df(df,"items")

fetch_all_products()

