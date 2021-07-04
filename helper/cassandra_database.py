from .database import Database
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import pandas as pd
import csv
import logging as lg


def pandas_factory(colnames, rows):
    return pd.DataFrame(rows, columns=colnames)


class Cassandra_Database(Database):

    def create_schema(self, host, username, password, schema_name):
        try:
            cluster = Cluster(contact_points=[host],
                              auth_provider=PlainTextAuthProvider(username=username, password=password))
            session = cluster.connect()
            query = "CREATE KEYSPACE IF NOT EXISTS " + schema_name + " WITH REPLICATION = \
                {'class': 'SimpleStrategy', 'replication_factor': 4}"
            lg.info(query)
            session.execute(query)
            lg.info("Keyspace Created Successfully!!")
            return "Keyspace Created Successfully!!"
        except Exception as error:
            lg.error(error)
            return error

    def drop_schema(self, host, username, password, schema_name):
        try:
            cluster = Cluster(contact_points=[host],
                              auth_provider=PlainTextAuthProvider(username=username, password=password))
            session = cluster.connect()
            query = f"DROP KEYSPACE IF EXISTS {schema_name}"
            lg.info(query)
            session.execute(query)
            lg.info("Keyspace Dropped Successfully!!")
            return "Keyspace Dropped Successfully!!"
        except Exception as error:
            lg.error(error)
            return error

    def create_table(self, host, username, password, schema_name, table_name, **columns):
        try:
            cluster = Cluster(contact_points=[host],
                              auth_provider=PlainTextAuthProvider(username=username, password=password))
            session = cluster.connect()
            session.set_keyspace(f'{schema_name}')
            column_names = ""
            for name, datatype in columns.items():
                if len(column_names) != 0:
                    column_names += ', '
                column_names += name + " " + datatype
            query = f"CREATE TABLE IF NOT EXISTS {table_name} ({column_names})"
            lg.info(query)
            session.execute(query)
            lg.info("Table Created Successfully!!")
            return "Table Created Successfully!!"
        except Exception as error:
            lg.error(error)
            return error

    def drop_table(self, host, username, password, schema_name, table_name):
        try:
            cluster = Cluster(contact_points=[host],
                              auth_provider=PlainTextAuthProvider(username=username, password=password))
            session = cluster.connect()
            session.set_keyspace(f'{schema_name}')
            query = f"DROP TABLE IF EXISTS {table_name}"
            lg.info(query)
            session.execute(query)
            lg.info("Table Dropped Successfully!!")
            return "Table Dropped Successfully!!"
        except Exception as error:
            lg.error(error)
            return error

    def insert_record(self, host, username, password, schema_name, table_name, **columns):
        try:
            cluster = Cluster(contact_points=[host],
                              auth_provider=PlainTextAuthProvider(username=username, password=password))
            session = cluster.connect()
            columns_name = ""
            values = ""
            for key, value in columns.items():
                if len(columns_name) != 0:
                    columns_name += ', '
                if len(values) != 0:
                    values += ', '
                columns_name += key
                values += "'" + str(value) + "'"
            session.set_keyspace(f'{schema_name}')
            query = f"INSERT INTO {table_name} ({columns_name}) VALUES ({values})"
            lg.info(query)
            session.execute(query)
            lg.info("Record Inserted Successfully!!")
            return "Record Inserted Successfully!!"
        except Exception as error:
            lg.error(error)
            return error

    def insert_multiple_records(self, host, username, password, schema_name, table_name, input_file_name, *columns):
        try:
            cluster = Cluster(contact_points=[host],
                              auth_provider=PlainTextAuthProvider(username=username, password=password))
            session = cluster.connect()
            columns_name = ""
            for column in columns:
                if len(columns_name) != 0:
                    columns_name += ', '
                columns_name += column
            session.set_keyspace(f'{schema_name}')
            with open(input_file_name, 'r') as data:
                next(data)
                data_csv = csv.reader(data, delimiter='\n')
                for input_data in data_csv:
                    cols_value = ""
                    for cols in input_data[0].split(','):
                        if len(cols_value) != 0:
                            cols_value += ', '
                        cols_value += f"'{cols}'"
                    query = f"INSERT INTO {table_name} ({columns_name}) VALUES ({cols_value})"
                    lg.info(query)
                    session.execute(query)
            lg.info("Records Inserted Successfully!")
            return "Records Inserted Successfully!"
        except Exception as error:
            lg.error(error)
            return error

    def update_records(self, host, username, password, schema_name, table_name, filters, **updated_values):
        try:
            cluster = Cluster(contact_points=[host],
                              auth_provider=PlainTextAuthProvider(username=username, password=password))
            session = cluster.connect()
            session.set_keyspace(f'{schema_name}')
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
            session.execute(query)
            lg.info("Record Updated Successfully!!")
            return "Record Updated Successfully!!"
        except Exception as error:
            lg.error(error)
            return error

    def retrieve_records(self, host, username, password, schema_name, table_name):
        try:
            cluster = Cluster(contact_points=[host],
                              auth_provider=PlainTextAuthProvider(username=username, password=password))
            session = cluster.connect()
            session.set_keyspace(f'{schema_name}')
            session.row_factory = pandas_factory
            session.default_fetch_size = 10000000
            query = f"SELECT * FROM {table_name}"
            lg.info(query)
            rows = session.execute(query)
            df = rows._current_rows
            df.to_csv('retrieved_data.csv', header=True, index=False)
            lg.info("Records Extracted Successfully!!")
            return "Records Extracted Successfully!!"
        except Exception as error:
            lg.error(error)
            return error

    def retrieve_records_with_filter(self, host, username, password, schema_name, table_name, **filters):
        try:
            cluster = Cluster(contact_points=[host],
                              auth_provider=PlainTextAuthProvider(username=username, password=password))
            session = cluster.connect()
            session.set_keyspace(f'{schema_name}')
            session.row_factory = pandas_factory
            session.default_fetch_size = 10000000
            filters_fields = ""
            for f_key, f_value in filters.items():
                if len(filters_fields) != 0:
                    filters_fields += ' OR '
                filters_fields += f"{f_key} = '{f_value}'"
            query = f"SELECT * FROM {table_name} WHERE {filters_fields} ALLOW FILTERING"
            lg.info(query)
            rows = session.execute(query)
            df = rows._current_rows
            df.to_csv('retrieved_data_with_filters.csv', header=True, index=False)
            lg.info("Records Extracted Successfully!!")
            return "Records Extracted Successfully!!"
        except Exception as error:
            lg.error(error)
            return error

    def delete_records_with_filter(self, host, username, password, schema_name, table_name, **filters):
        try:
            cluster = Cluster(contact_points=[host],
                              auth_provider=PlainTextAuthProvider(username=username, password=password))
            session = cluster.connect()
            filters_fields = ""
            for f_key, f_value in filters.items():
                if len(filters_fields) != 0:
                    filters_fields += ' OR '
                filters_fields += f"{f_key} = '{f_value}'"
            session.set_keyspace(f'{schema_name}')
            query = f"DELETE FROM {table_name} WHERE {filters_fields}"
            lg.info(query)
            session.execute(query)
            lg.info("Records Deleted Successfully!!")
            return "Records Deleted Successfully!!"
        except Exception as error:
            lg.error(error)
            return error

    def delete_records(self, host, username, password, schema_name, table_name):
        try:
            lg.info("Please try delete_records_with_filters method.")
            return "Please try delete_records_with_filters method."
        except Exception as error:
            lg.error(error)
            return error

    def truncate_table(self, host, username, password, schema_name, table_name):
        try:
            cluster = Cluster(contact_points=[host],
                              auth_provider=PlainTextAuthProvider(username=username, password=password))
            session = cluster.connect()
            session.set_keyspace(f'{schema_name}')
            query = f"TRUNCATE {table_name}"
            lg.info(query)
            session.execute(query)
            lg.info("Table Truncated Successfully!!")
            return "Table Truncated Successfully!!"
        except Exception as error:
            lg.error(error)
            return error
