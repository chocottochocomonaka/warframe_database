import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sqlalchemy import Table,Column,String,Integer,MetaData,create_engine,inspect,MetaData
from sqlalchemy.orm import declarative_base
import requests
import re
import time

#アイテムのデータ一覧を取得
def item_list():
    time.sleep(0.33)
    url = "https://api.warframe.market/v1/items"
    response = requests.get(url)
    data = response.json()
    if "payload" in data:
        for count in range(len(data['payload']['items'])):
            return [item['item_name'] for item in data['payload']['items']]
    else:
        pass

#テーブル名にふさわしいアイテム名に変更
def name_changer(item):
    name = item.lower()
    name = re.sub(r'\W+', '_', name)
    return name.strip('_')

#指定されたアイテムの48時間以内のデータを取得
def res(item_name):
    time.sleep(1)
    url = f"https://api.warframe.market/v1/items/{item_name}/statistics"
    response = requests.get(url)
    data = response.json()
    try:
        if data is None or data==[]:
            print("hellllllllllllllllllllllllllo")
        else:
            df = pd.DataFrame(data["payload"]['statistics_closed']['48hours'])
            return df
    except KeyError as e:
        print(e)
        pass
#指定されたdfが存在するか　中身が空っぽじゃないか確認
def existence(df):
    if df is not None and not df.empty:
        return True
    else:
        False


#アイテム一覧
items = item_list()

#アイテムが存在するか、dfを表示　このファイルのメイン
def checker():
    for count in range(len(items)):
        try:
            item = items[count]
            name = name_changer(item)
            result = existence(res(name))
            print(result)
            print(res(name))
        except KeyError as e:
            print(f"KeyError on item {item}: {e}")
            continue
        except Exception as e:
            print(f"Other error on item {item}: {e}")
            continue