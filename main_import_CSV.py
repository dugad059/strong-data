from functions_csv_import import *
from env import *
from datetime import datetime

#Settings
dataset_dir = datetime.now().strftime(f'%m-%d-%Y_stongdata')
aws_dir = dataset_dir


# Configure Environment and create main df
csv_files = csv_files()
configure_dataset_directory(csv_files, dataset_dir)
df = create_df(dataset_dir, csv_files)

for k in csv_files:
    
    # Call Dataframe
    dataframe = df[k]
   
    # Clean table name
    tbl_name = clean_tbl_name(k)
   
    # Clean column names
    col_str, dataframe.columns = clean_colname(dataframe) 

    # Uploads Data to DB
    upload_to_db(host, dbname, user,password, tbl_name, col_str,file=k, dataframe=dataframe , dataframe_columns=dataframe.columns)

# Uploads CSV's to S3
upload_folder_to_s3(dataset_dir, 'strongdata', aws_dir)

# Deletes CSVs and new Directory from memory
del_csv_dir(csv_files, dataset_dir)

