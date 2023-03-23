import dbconnect
import functions
from tabulate import tabulate

column_keys = ['스키마', '테이블', '컬럼명', '데이터타입', '길이', '널여부', 'default']
query = "SELECT concat( TABLE_SCHEMA,'.', TABLE_NAME,'.', COLUMN_NAME,'.', if(DATA_TYPE='enum', COLUMN_TYPE, DATA_TYPE),'.', ifNULL(NUMERIC_PRECISION,''),'.', IS_NULLABLE,'.', ifNULL(COLUMN_DEFAULT,'')) as colkey  FROM information_schema.COLUMNS where TABLE_SCHEMA = 'ecommerce' order by colkey asc"

# db 1
db_local = dbconnect.Connection(**functions.get_json_data('config/local.json'))
db_local.connect()
db_local.cursor.execute(query)
result_local = db_local.cursor.fetchall()
db_local.close()

# db 2
db_prd = dbconnect.Connection(**functions.get_json_data('config/prd.json'))
db_prd.connect()
db_prd.cursor.execute(query)
result_prd = db_prd.cursor.fetchall()
db_prd.close()

# tuple to list
result_local = [i[0] for i in result_local]
result_prd = [i[0] for i in result_prd]

# 결과
result_diff = []
result_prd_end = []

# 비교
for i in range(len(result_local)):
    if not result_local[i] in result_prd:
        result_diff.append(result_local[i].split('.'))
    else:
        result_prd.remove(result_local[i])

for i in range(len(result_prd)):
    result_prd_end.append(result_prd[i].split('.'))

print("=====================> 일치 하지 않는것")
print(tabulate(result_diff, headers=column_keys,
      tablefmt='pretty', numalign="right", stralign="left"))
print("=====================> prod에만 있는것")
print(tabulate(result_prd_end, headers=column_keys,
      tablefmt='pretty', numalign="right", stralign="left"))
