from .database import Database
import mysql.connector as connection
import csv
import pandas as pd
import logging as lg


class MySQL_Database(Database):

    def create_schema(self, host, username, password, schema_name):
        mysql_db = ""
        try:
            mysql_db = connection.connect(host=host, user=username, passwd=password, use_pure=True)
            query = f"CREATE DATABASE IF NOT EXISTS {schema_name}"
            lg.info(query)
            cursor = mysql_db.cursor()
            cursor.execute(query)
            mysql_db.close()
            lg.info("Database Created Successfully!")
            return "Database Created Successfully!"
        except Exception as e:
            mysql_db.close()
            lg.error(str(e))
            return str(e)

    def drop_schema(self, host, username, password, schema_name):
        mysql_db = ""
        try:
            mysql_db = connection.connect(host=host, user=username, passwd=password, use_pure=True)
            query = f"DROP DATABASE IF EXISTS {schema_name}"
            lg.info(query)
            cursor = mysql_db.cursor()
            cursor.execute(query)
            mysql_db.close()
            lg.info("Database Dropped Successfully!")
            return "Database Dropped Successfully!"
        except Exception as e:
            mysql_db.close()
            lg.error(str(e))
            return str(e)

    def create_table(self, host, username, password, schema_name, table_name, **columns):
        mysql_db = ""
        try:
            mysql_db = connection.connect(host=host, user=username, passwd=password, database=schema_name, use_pure=True)
            cursor = mysql_db.cursor()
            column_names = ""
            for name, datatype in columns.items():
                if len(column_names) != 0:
                    column_names += ', '
                column_names += name + " " + datatype
            query = f"CREATE TABLE IF NOT EXISTS {table_name} ({column_names})"
            lg.info(query)
            cursor.execute(query)
            mysql_db.close()
            lg.info("Table Created Successfully!")
            return "Table Created Successfully!"
        except Exception as e:
            mysql_db.close()
            lg.error(str(e))
            return str(e)

    def drop_table(self, host, username, password, schema_name, table_name):
        mysql_db = ""
        try:
            mysql_db = connection.connect(host=host, user=username, passwd=password, database=schema_name, use_pure=True)
            query = f"DROP TABLE IF EXISTS {table_name}"
            lg.info(query)
            cursor = mysql_db.cursor()
            cursor.execute(query)
            mysql_db.close()
            lg.info("Table Dropped Successfully!")
            return "Table Dropped Successfully!"
        except Exception as e:
            mysql_db.close()
            lg.error(str(e))
            return str(e)

    def insert_record(self, host, username, password, schema_name, table_name, **columns):
        mysql_db = ""
        try:
            mysql_db = connection.connect(host=host, user=username, passwd=password, database=schema_name, use_pure=True)
            columns_name = ""
            values = ""
            for key, value in columns.items():
                if len(columns_name) != 0:
                    columns_name += ', '
                if len(values) != 0:
                    values += ', '
                columns_name += key
                values += "'" + str(value) + "'"
            query = f"INSERT INTO {table_name} ({columns_name}) VALUES ({values})"
            lg.info(query)
            cursor = mysql_db.cursor()
            cursor.execute(query)
            mysql_db.commit()
            mysql_db.close()
            lg.info("Record Inserted Successfully!")
            return "Record Inserted Successfully!"
        except Exception as e:
            mysql_db.close()
            lg.error(str(e))
            return str(e)

    def insert_multiple_records(self, host, username, password, schema_name, table_name, input_file_name, *columns):
        mysql_db = ""
        try:
            mysql_db = connection.connect(host=host, user=username, passwd=password, database=schema_name, use_pure=True)
            cursor = mysql_db.cursor()
            columns_name = ""
            for column in columns:
                if len(columns_name) != 0:
                    columns_name += ', '
                columns_name += column
            with open(input_file_name, 'r') as data:
                next(data)
                data_csv = csv.reader(data, delimiter='\n')
                for input_data in data_csv:
                    input_values = ""
                    for values in input_data[0].split(","):
                        if len(input_values) != 0:
                            input_values += ','
                        input_values += f"'{values}'"
                    query = f"INSERT INTO {table_name} ({columns_name}) VALUES ({input_values})"
                    lg.info(query)
                    cursor.execute(query)
            mysql_db.commit()
            mysql_db.close()
            lg.info("Records Inserted Successfully!")
            return "Records Inserted Successfully!"
        except Exception as e:
            mysql_db.close()
            lg.error(str(e))
            return str(e)

    def update_records(self, host, username, password, schema_name, table_name, filters, **updated_values):
        mysql_db = ""
        try:
            mysql_db = connection.connect(host=host, user=username, passwd=password, database=schema_name, use_pure=True)
            cursor = mysql_db.cursor()
            update_fields = ""
            for key, value in updated_values.items():
                if len(update_fields) != 0:
                    update_fields += ', '
                update_fields += f"{key} = '{value}'"
            filters_fields = ""
            for f_key, f_value in filters.items():
                if len(filters_fields) != 0:
                    filters_fields += ' OR '
                filters_fields += f"{f_key} = '{f_value}'"
            query = f"UPDATE {table_name} SET {update_fields} WHERE {filters_fields}"
            lg.info(query)
            cursor.execute(query)
            mysql_db.commit()
            mysql_db.close()
            lg.info("Data Updated Successfully!")
            return "Data Updated Successfully!!"
        except Exception as e:
            mysql_db.close()
            lg.error(str(e))
            return str(e)

    def retrieve_records(self, host, username, password, schema_name, table_name):
        mysql_db = ""
        try:
            mysql_db = connection.connect(host=host, user=username, passwd=password, database=schema_name, use_pure=True)
            query = f"SELECT * FROM {table_name}"
            lg.info(query)
            df = pd.read_sql(query, mysql_db)
            df.to_csv('retrieved_data.csv', header=True, index=False)
            mysql_db.close()
            lg.info("Data Extracted Successfully!")
            return "Data Extracted Successfully!"
        except Exception as e:
            mysql_db.close()
            lg.error(str(e))
            return str(e)

    def retrieve_records_with_filter(self, host, username, password, schema_name, table_name, **filters):
        mysql_db = ""
        try:
            mysql_db = connection.connect(host=host, user=username, passwd=password, database=schema_name, use_pure=True)
            filters_fields = ""
            for f_key, f_value in filters.items():
                if len(filters_fields) != 0:
                    filters_fields += ' OR '
                filters_fields += f"{f_key} = '{f_value}'"
            query = f"SELECT * FROM {table_name} WHERE {filters_fields}"
            lg.info(query)
            df = pd.read_sql(query, mysql_db)
            df.to_csv('retrieved_data_with_filters.csv', header=True, index=False)
            mysql_db.close()
            lg.info("Data Extracted Successfully!")
            return "Data Extracted Successfully!"
        except Exception as e:
            mysql_db.close()
            lg.error(str(e))
            return str(e)

    def delete_records_with_filter(self, host, username, password, schema_name, table_name, **filters):
        mysql_db = ""
        try:
            mysql_db = connection.connect(host=host, user=username, passwd=password, database=schema_name, use_pure=True)
            filters_fields = ""
            for f_key, f_value in filters.items():
                if len(filters_fields) != 0:
                    filters_fields += ' OR '
                filters_fields += f"{f_key} = '{f_value}'"
            query = f"DELETE FROM {table_name} WHERE {filters_fields}"
            lg.info(query)
            cursor = mysql_db.cursor()
            cursor.execute(query)
            mysql_db.commit()
            mysql_db.close()
            lg.info("Records Deleted Successfully!")
            return "Records Deleted Successfully!"
        except Exception as e:
            mysql_db.close()
            lg.error(str(e))
            return str(e)

    def delete_records(self, host, username, password, schema_name, table_name):
        mysql_db = ""
        try:
            mysql_db = connection.connect(host=host, user=username, passwd=password, database=schema_name, use_pure=True)
            query = f"DELETE FROM {table_name}"
            lg.info(query)
            cursor = mysql_db.cursor()
            cursor.execute(query)
            mysql_db.commit()
            mysql_db.close()
            lg.info("Records Deleted Successfully!")
            return "Records Deleted Successfully!"
        except Exception as e:
            mysql_db.close()
            lg.error(str(e))
            return str(e)

    def truncate_table(self, host, username, password, schema_name, table_name):
        mysql_db = ""
        try:
            mysql_db = connection.connect(host=host, user=username, passwd=password, database=schema_name, use_pure=True)
            query = f"TRUNCATE TABLE {table_name}"
            lg.info(query)
            cursor = mysql_db.cursor()
            cursor.execute(query)
            mysql_db.close()
            lg.info("Table Truncated Successfully!")
            return "Table Truncated Successfully!"
        except Exception as e:
            mysql_db.close()
            lg.error(str(e))
            return str(e)
