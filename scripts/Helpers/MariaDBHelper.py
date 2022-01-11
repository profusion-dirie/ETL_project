import mariadb
import sys
import pandas as pd

from sqlalchemy import create_engine
from sqlalchemy.sql import text as sa_text

class MariaDBHelper:

    def __init__(self, host, port, user, password, database_name, autocommit=True, watcher=None):

        try:
            self.conn = mariadb.connect(user=user, password=password, host=host, port=int(port), database=database_name)
            self.conn.autocommit = autocommit
            # Get Cursor
            self.cur = self.conn.cursor()
            print('successfully connected to MariaDB engine')
            if watcher is not None:
                watcher.log(status='info', message="successfully connected to MariaDB engine",
                            category='')

        except mariadb.Error as e:
            print("Error connecting to MariaDB engine.", e)
            if watcher is not None:
                watcher.log(status='error', message="Error connecting to MariaDB engine." + str(e),
                            category='connection_initialisation_failure')
            #sys.exit(1)



    def run_query(self, sql):
        result = None
        print(sql[0:250])
        try:
            self.cur.execute(sql)
            if sql.lower().strip().startswith('select'):
                result = self.cur.fetchall()

        except mariadb.Error as e:
            print(f"Error: {e}")

        return result

    # this doesn't work
    # def insert_df_into_mysql_table(self, dataframe, db_name, table_name, if_exist='append', chunksize=1000):
    #     # https: // stackoverflow.com / questions / 16476413 / how - to - insert - pandas - dataframe - via - mysqldb - into - database
    #     # flavor='mysql': do i need to change this to mariadb
    #     r = None
    #     try:
    #         db_table = "`{db}`.`{table}`".format(db=db_name, table=table_name)
    #         r = dataframe.to_sql(db_table, con=self.conn, if_exists=if_exist, chunksize=chunksize)
    #     except mariadb.Error as e:
    #         print(f"Error: {e}")
    #     return r


    def get_upload_query(self, schema, table_name, file_path, LINES_TERMINATED_BY="'\\n'"):
        upload_query = "LOAD DATA LOCAL INFILE '{file_path}' INTO TABLE `{schema}`.`{table_name}` " \
                       "FIELDS TERMINATED BY ',' " \
                       "ENCLOSED BY 'ENCLOSED_BY' LINES TERMINATED BY {LINES_TERMINATED_BY} " \
                       "IGNORE 1 LINES;".format(file_path=file_path,
                                                schema=schema,
                                                table_name=table_name,
                                                LINES_TERMINATED_BY=LINES_TERMINATED_BY)
        upload_query = upload_query.replace('ENCLOSED_BY', '"')
        return upload_query


    def delete_if_record_exist(self, db_name, table_name, delivery_id, client, label, delivery_code):
        # check if entry exists
        sql1 = "SELECT count(*) as ct FROM {db_name}.{table_name} " \
               "WHERE delivery_id = {del_id} " \
               "AND client = '{client}' " \
               "AND label = '{label}' " \
               "AND delivery_code = '{del_code}' ;".format(db_name=db_name, table_name=table_name, del_id=delivery_id,
                                                           client=client, label=label, del_code=delivery_code)
        self.cur.execute(sql1)
        rowCount_tuple = self.cur.fetchone()
        rowCount = rowCount_tuple[0]

        sql2 = "DELETE FROM {db_name}.{table_name} " \
               "WHERE delivery_id = {del_id} " \
               "AND client = '{client}' " \
               "AND label = '{label}' " \
               "AND delivery_code = '{del_code}' ;".format(db_name=db_name, table_name=table_name,
                                                           del_id=delivery_id, client=client,
                                                           label=label, del_code=delivery_code)

        try:
            if rowCount > 0:
                # print('Deleting ' + str(rowCount) + ' rows from ' + db_name + '.' + table_name + ' for delivery_id: '+ \
                #       str(delivery_id))

                self.cur.execute(sql2)

        except Exception as e:
            print("Found an error checking if a row exists: ", e)
            print("Checking counts by running: " + sql1)
            print("Failing on insert command: " + sql2)
        return None

    def no_of_affected_rows(self):
        return self.cur.rowcount

    def close(self):
        self.conn.colse()
