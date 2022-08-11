import os
import pandas as pd
import numpy as np
import psycopg2

csv_files = []
for file in os.listdir(os.getcwd()):
    if file.endswith('.csv'):
        csv_files.append(file)

dataset_dir = 'datasets'

try:
    mkdir = 'mkdir {0}'.format(dataset_dir)
    os.system(mkdir)
except:
    pass

for csv in csv_files:
    mv_file = "mv '{0}' {1}".format(csv, dataset_dir)
    os.system(mv_file)


data_path = os.getcwd()+'/'+dataset_dir+'/'

df = {}
for file in csv_files:
    try:
        df[file] = pd.read_csv(data_path+file)
    except UnicodeDecodeError:
        df[file] = pd.read_csv(data_path+file, encoding='ISO-8859-1')
   


for k in csv_files:
    dataframe = df[k]

    clean_tbl_name = k.lower().replace(" ","_").replace("?", '').replace("-","_").replace(r"/","_").replace("\\","_").replace("%","").replace(")","").replace(r"(","").replace("&","")



    tbl_name = '{0}'.format(clean_tbl_name.split('.')[0])
  

    dataframe.columns = [x.lower().replace(" ","_").replace("?", '').replace("-","_").replace(r"/","_").replace("\\","_").replace("%","").replace(")","").replace(r"(","").replace("&","") for x in dataframe.columns] 

    replacements = {
        'object' : 'varchar',
        'float64' : 'float',
        'int64' : 'int',
        'datetime64' : 'timestamp',
        'timedelta64[ns]' : 'varchar' 
    }

    col_str = ', '.join('{} {}'.format(n,d) for (n,d) in zip(dataframe.columns, dataframe.dtypes.replace(replacements)))
   

    host = 'strong-data.cnzuwg8ioub5.us-east-1.rds.amazonaws.com'
    dbname = 'postgres'
    user = 'daviddugas'
    password = '20Duffle$'

    conn_string = f"host={host} dbname={dbname} user={user} password={password}"


    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()
    print('open db success')

    cursor.execute(f'drop table if exists {tbl_name}')

    cursor.execute(f'create table {tbl_name} ({col_str})')
    print(f'{tbl_name} was created successfully')

    dataframe.to_csv(k, header=dataframe.columns, index=False, encoding='utf-8')

    my_file = open(k)

    SQL_STATEMENT = '''
    COPY %s FROM STDIN WITH
        CSV
        HEADER
        DELIMITER AS ','
    '''

    cursor.copy_expert(sql=SQL_STATEMENT % tbl_name, file=my_file)
    print('file copied to db')

    cursor.execute(f'grant select on table {tbl_name} to public')
    conn.commit()

    cursor.close()
    print(f'table {tbl_name} imported to db complete')

print('All table have been successfully importing into the db')