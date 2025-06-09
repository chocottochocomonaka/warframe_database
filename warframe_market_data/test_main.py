from test_sql import database
from checker_db import remove_duplicates_by_datetime
from test_void_trader import void_trader
import time
start=time.time()
void_trader()#バロ吉のデータを確認
database()#データベースにアイテムデータを保存
remove_duplicates_by_datetime()#重複確認
end=time.time()
print(f"実行時間: {end-start}")