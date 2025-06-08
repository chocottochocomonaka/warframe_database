from test_sql import database
from checker_db import remove_duplicates_by_datetime
import time
start=time.time()
database()
remove_duplicates_by_datetime()
end=time.time()
print(f"実行時間: {end-start}")