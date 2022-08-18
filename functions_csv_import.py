import os
import pandas as pd
import psycopg2
from env import *
import boto3

# -------------------------------------------------------------
# Find CSV files in my current working directory
# Isolate only the CSV Files
def csv_files():
    csv_files = []
    for file in os.listdir(os.getcwd()):
        if file.endswith('.csv'):
            csv_files.append(file)
    return csv_files
# ----------------------------------------------------------------

# Create a new directory for the CSV files 
def configure_dataset_directory(csv_files, dataset_dir):

    # Make directory bash command
    try:
        mkdir = 'mkdir {0}'.format(dataset_dir)
        os.system(mkdir)
    except:
        pass

    # Move files to new directory
    for csv in csv_files:
        mv_file = "mv '{0}' {1}".format(csv, dataset_dir)
        os.system(mv_file)
        
    
    return

# -----------------------------------------------------------------------

#Create the pandas df from the CSV files to be able to transform them
def create_df(dataset_dir, csv_files):
    
    # Path to the CSV files
    data_path = os.getcwd()+'/'+dataset_dir+'/'

    # Loop through the files and create the dataframe
    df = {}
    for file in csv_files:
        try:
            df[file] = pd.read_csv(data_path+file)
        except UnicodeDecodeError:
            df[file] = pd.read_csv(data_path+file, encoding='ISO-8859-1')
            # This is a comment error, so this except will help by pass it! It's not an error we need to worry about! If there is an error in the encoding it will use that encoding on the pandas df 
    
    return df

# -----------------------------------------------------------------------

# Cleans table and column names for postgre
def clean_tbl_name(filename):

    # Cleans table names for postgre 
    clean_tbl_name = filename.lower().replace(" ","_").replace("?", '').replace("-","_").replace(r"/","_").replace("\\","_").replace("%","").replace(")","").replace(r"(","").replace("&","")
 
    # Removes .csv from the clean_tbl_name
    tbl_name = '{0}'.format(clean_tbl_name.split('.')[0])
    
    return tbl_name

# -----------------------------------------------------------------------

def clean_colname(dataframe):

 # Cleans column names for postgre 
    dataframe.columns = [x.lower().replace(" ","_").replace("?", '').replace("-","_").replace(r"/","_").replace("\\","_").replace("%","").replace(")","").replace(r"(","").replace("&","") for x in dataframe.columns] 

    # Replacement dictionary that maps pandas dtypes to sql dtypes
    replacements = {
        'object' : 'varchar',
        'float64' : 'float',
        'int64' : 'int',
        'datetime64' : 'timestamp',
        'timedelta64[ns]' : 'varchar' 
    }

    #Table Schema
    col_str = ', '.join('{} {}'.format(n,d) for (n,d) in zip(dataframe.columns, dataframe.dtypes.replace(replacements)))

    return col_str, dataframe.columns

# -----------------------------------------------------------------------------------------------
 
 # Connection to DB
def upload_to_db(host, dbname, user,password, tbl_name, col_str, file, dataframe, dataframe_columns):
    # Opens a database connection
    conn_string = f"host={host} dbname={dbname} user={user} password={password}"
    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()
    print('open db success')

    # Drops table with the same name
    cursor.execute(f'drop table if exists {tbl_name}')

    # Creates table
    cursor.execute(f'create table {tbl_name} ({col_str})')
    print(f'{tbl_name} was created successfully')

    # Saved pandas df back to a csv
    dataframe.to_csv(file, header=dataframe_columns, index=False, encoding='utf-8')

    # Opens the csv file and saves it as an object
    my_file = open(file)
    
    # Uploads CSV to db
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

# -----------------------------------------------------------------------------------------------

# Loads CSVs in S#
def upload_folder_to_s3(dataset_dir, s3bucket, aws_dir):
    s3_client = boto3.client('s3', 
                            aws_access_key_id = ACCESS_KEY,
                            aws_secret_access_key = SECRET_KEY,
                            region_name = REGION_NAME )
    
    # Created a dates stamped folder
    # s3_client.put_object(Bucket='strongdata', Key=(dataset_dir))
    # print('Successfully created Folder')
    
    # Transfer files to new folder in S3
    pbar = (os.walk(dataset_dir))
    for path, subdirs,files in pbar:
        for file in files:
            dest_path = path.replace(dataset_dir, "").replace(os.sep, '/')
            s3_file = f'{aws_dir}/{dest_path}/{file}'.replace('//', '/')
            local_file = os.path.join(path, file)
            s3_client.upload_file(local_file, s3bucket, s3_file)
    print(f"Successfully uploaded {dataset_dir} to S3 {aws_dir}")

# -----------------------------------------------------------------------------------------------

# Deletes new directory and CSVs from memory
def del_csv_dir(csv_files, dataset_dir):
    #Deletes new directory
    try:
        rmdir = 'rm -r {0}'.format(dataset_dir)
        os.system(rmdir)
    except:
        pass
    
    #Deletes CSVs
    for csv in csv_files:
        rm_file = "rm {0}".format(csv)
        os.system(rm_file)
    return

