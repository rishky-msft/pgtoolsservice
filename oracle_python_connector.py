import time
import mysql.connector

#import insert_salaries_sample
#sql = insert_salaries_sample.query_data()
sql = "select * from employees limit 10"

def benchmark_query(sql, cur):
    cur.execute(sql)
    res  = cur.fetchall()
    return res
    
def pymysql_connector():
    connection = mysql.connector.connect(
        host='nitish-test.mysql.database.azure.com',
        user='nitish',
        password='test@1234',
        database='employees',
        use_pure=False
    )
    cursor = connection.cursor()
    starttime = time.time()
    res = benchmark_query(sql, cursor)
    print(res)
    endtime = time.time()
    connection.commit()
    return (endtime - starttime)
    
print("Time taken = " + str(pymysql_connector()))

print("The end")