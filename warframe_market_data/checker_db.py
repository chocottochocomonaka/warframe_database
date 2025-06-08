from sqlalchemy import create_engine, inspect
import pandas as pd

# DB接続設定
engine = create_engine("mysql+pymysql://root:himitu0625@127.0.0.1:3306/test1")
inspector = inspect(engine)

def remove_duplicates_by_datetime():
    tables = inspector.get_table_names()
    for table in tables:
        try:
            # datetimeカラムが存在するか確認
            columns = [col['name'] for col in inspector.get_columns(table)]
            if 'datetime' not in columns:
                print(f"⏭️ スキップ（datetimeなし）: {table}")
                continue

            # データ読み込み
            df = pd.read_sql(f"SELECT * FROM `{table}`", con=engine)

            # 重複削除（datetime基準）
            before = len(df)
            df_deduped = df.drop_duplicates(subset=['datetime'], keep='first')
            after = len(df_deduped)

            # 上書き保存
            if before != after:
                df_deduped.to_sql(table, con=engine, if_exists='replace', index=False)
                print(f"✅ 重複削除: {table}（{before - after}件 削除）")
            else:
                print(f"👌 重複なし: {table}")

        except Exception as e:
            print(f"⚠️ エラー（{table}）: {e}")


