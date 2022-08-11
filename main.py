import os
import pandas as pd
import psycopg2


df = pd.read_csv('strong.csv')
# print(df.head())

file = 'strong'

clean_tbl_name = file.lower().replace(" ","_").replace("?", '').replace("-","_").replace(r"/","_").replace("\\","_").replace("%","").replace(")","").replace(r"(","").replace("&","")

# print(clean_tbl_name)

df.columns = [x.lower().replace(" ","_").replace("?", '').replace("-","_").replace(r"/","_").replace("\\","_").replace("%","").replace(")","").replace(r"(","").replace("&","") for x in df.columns]

# print(df.columns)

replacements = {
    'object' : 'varchar',
    'float64' : 'float',
    'int64' : 'int',
    'datetime64' : 'timestamp',
    'timedelta64[ns]' : 'varchar' 
}

# print(replacements)

col_str = ', '.join('{} {}'.format(n,d) for (n,d) in zip(df.columns, df.dtypes.replace(replacements)))

# print(col_str)

conn_string = "host=strong-data.cnzuwg8ioub5.us-east-1.rds.amazonaws.com dbname='postgres' user='daviddugas' password='20Duffle$'"

conn = psycopg2.connect(conn_string)
cursor = conn.cursor()
print('open db success')

cursor.execute('drop table if exists strong')

cursor.execute('create table strong (date varchar, workout_name varchar, duration varchar, exercise_name varchar, set_order int, weight float, reps int, distance int, seconds int, notes varchar, workout_notes float, rpe float)')

df.to_csv('strong.csv', header=df.columns, index=False, encoding='utf-8')

my_file = open('strong.csv')
print('file opened')

SQL_STATEMENT = '''
COPY strong FROM STDIN WITH
CSV
HEADER
DELIMITER AS ','
'''

cursor.copy_expert(sql=SQL_STATEMENT, file=my_file)
print('file copied to db')

cursor.execute('grant select on table strong to public')
conn.commit()

cursor.close()
print('table strong imported to db complete')