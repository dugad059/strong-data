from csv_import_functions import *
from env import *

#Settings
dataset_dir = 'datasets'

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



