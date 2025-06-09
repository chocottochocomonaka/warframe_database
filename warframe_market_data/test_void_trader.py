import pandas as pd
import requests
from sqlalchemy import create_engine
engine=create_engine("mysql+pymysql://root:himitu0625@127.0.0.1:3306/test1")
#現在のデータを取得
def insert_void_trader():
    platform = "pc"  # "ps4", "xbox", "switch" も使用可能
    url = f"https://api.warframestat.us/{platform}/voidTrader"

    # APIリクエスト
    response = requests.get(url)
    data = response.json()
    df=pd.DataFrame([data])
    df["activation"] = pd.to_datetime(df["activation"]).dt.strftime("%Y-%m-%d %H:%M:%S")
    df["expiry"] = pd.to_datetime(df["expiry"]).dt.strftime("%Y-%m-%d %H:%M:%S")
    df["initialStart"] = pd.to_datetime(df["initialStart"]).dt.strftime("%Y-%m-%d %H:%M:%S")
    # 空のリストを検出
    for column in df.columns:
        if df[column].apply(lambda x: isinstance(x, list) and len(x) == 0).all():
            df[column]=None

    return df

#データベースのデータを取得
def get_void_trader():
    query = '''
    SELECT *
    FROM void_trader
    LIMIT 1
    '''
    df_void_trader=pd.read_sql(query,con=engine)
    return df_void_trader
def void_trader():
    if  not get_void_trader()["active"].empty:#現在来訪中
        if get_void_trader().equals(insert_void_trader()):#重複しているか確認
           print("重複しています")
        else:
            insert_void_trader().to_sql("void_trader",con=engine,if_exists='append', index=False)
    elif get_void_trader()["active"].empty:#いない
        print("来訪中ではないです")
    else:
        print("エラー: バロ吉")