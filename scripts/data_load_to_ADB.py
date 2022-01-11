from Helpers import MariaDBHelper
import pandas as pd
from configparser import ConfigParser
import datetime as dt
import os

# Define path variable
path = '/home/diriei/etl_project'

# Configure config file
config = ConfigParser()
config.read('/home/diriei/etl_project/config/env.ini')
adb_conf = dict(config.items('adb'))

# Connect to MariaDB
MariaDB = MariaDBHelper.MariaDBHelper(host=adb_conf['host'],
                                      port=int(adb_conf['port']),
                                      user=adb_conf['user'],
                                      password=adb_conf['password'],
                                      database_name=adb_conf['schema'])
def into_table(query):
    upload_result = MariaDB.run_query(sql=query)
    
    # print number of affected rows
    upload_result_affected_nrows = MariaDB.no_of_affected_rows()
    print(upload_result_affected_nrows)
    return None


file_path_name = path + '/transformed_files/final_cleaned_business1.csv'
query = 'LOAD DATA LOCAL INFILE "{file_path}" ' \
                       "INTO TABLE {db_name}.{table_name} " \
                       "FIELDS TERMINATED BY ',' " \
                       'LINES TERMINATED BY "\\n" ' \
                       'IGNORE 1 LINES;'.format(file_path=file_path_name,
                                                db_name=adb_conf['schema'],
                                                table_name=adb_conf['followers_table'])
                                                



into_table(query)
        
# os.remove(path)