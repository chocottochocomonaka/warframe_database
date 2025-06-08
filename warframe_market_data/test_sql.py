from sqlalchemy import Table,Column,String,Integer,MetaData,create_engine,inspect,MetaData,Boolean,DateTime,Float
from sqlalchemy.orm import declarative_base
import pandas as pd
from test_checker import item_list
from test_checker import res,item_list,name_changer,existence
Base=declarative_base()
metadata = MetaData()
engine=create_engine("mysql+pymysql://root:himitu0625@127.0.0.1:3306/test1")
inspector=inspect(engine)
items = item_list()
item_count=len(item_list())




def map_dtype(dtype):
    if pd.api.types.is_integer_dtype(dtype):
        return Integer
    elif pd.api.types.is_float_dtype(dtype):
        return Float
    elif pd.api.types.is_bool_dtype(dtype):
        return Boolean
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return DateTime
    else:
        return String(255)
    
def database():
    def insert_df(item_name):
        df_new=res(item_name)
        length=len(df_new)
        query = f"""
        SELECT *
        FROM `{item_name}`
        ORDER BY datetime DESC
        LIMIT {length}
        """
        df_db = pd.read_sql(query, con=engine)
        df_unique = df_new[~df_new['datetime'].isin(df_db['datetime'])]
        return df_unique


    for count in item_list():
        table_name=name_changer(count)
        new_df=res(table_name)
        #指定したテーブルがなかった場合テーブル作成
        if not inspector.has_table(table_name):
            try:
                print(map_dtype(new_df.columns))

                columns = [
                            Column(name, map_dtype(new_df.columns), primary_key=(name == 'id'))
                            for name in new_df.columns
                        ]
                if existence(new_df) is not None:
                    table = Table(table_name, metadata, *columns)
                    metadata.create_all(engine)
                    print(f"🆕 created: {table_name}")
                    insert_df(table_name).to_sql(table_name, con=engine, if_exists='append', index=False)
                else:
                    print("error")
            except AttributeError as e:
                print(e)
        #テーブルがすでにあったらデータを追加
        else:
            try:
                insert_df(table_name).to_sql(table_name, con=engine, if_exists='append', index=False)
                print(f"insert to {table_name}")
            except Exception as e:
                print("already done")