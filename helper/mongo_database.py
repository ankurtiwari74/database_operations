from .database import Database
import pymongo
import csv
import logging as lg


class Mongo_Database(Database):

    def create_schema(self, host, username, password, schema_name):
        try:
            if username not in (None, "") and password not in (None, ""):
                connection_string = f"mongodb://{username}:{password}@{host}:27017/"
            else:
                connection_string = f"mongodb://{host}:27017/"
            lg.info(connection_string)
            client = pymongo.MongoClient(connection_string)
            client[schema_name]
            lg.info("Schema Created Successfully!!")
            return "Schema Created Successfully!!"
        except Exception as error:
            lg.error(error)
            return error

    def drop_schema(self, host, username, password, schema_name):
        try:
            if username not in (None, "") and password not in (None, ""):
                connection_string = f"mongodb://{username}:{password}@{host}:27017/"
            else:
                connection_string = f"mongodb://{host}:27017/"
            lg.info(connection_string)
            client = pymongo.MongoClient(connection_string)
            client.drop_database(schema_name)
            lg.info("Schema Dropped Successfully!!")
            return "Schema Dropped Successfully!!"
        except Exception as error:
            lg.error(error)
            return error

    def create_table(self, host, username, password, schema_name, table_name, **columns):
        try:
            if username not in (None, "") and password not in (None, ""):
                connection_string = f"mongodb://{username}:{password}@{host}:27017/"
            else:
                connection_string = f"mongodb://{host}:27017/"
            lg.info(connection_string)
            client = pymongo.MongoClient(connection_string)
            mydb = client[schema_name]
            mydb[table_name]
            lg.info("Collection Created Successfully!!")
            return "Collection Created Successfully!!"
        except Exception as error:
            lg.error(error)
            return error

    def drop_table(self, host, username, password, schema_name, table_name):
        try:
            if username not in (None, "") and password not in (None, ""):
                connection_string = f"mongodb://{username}:{password}@{host}:27017/"
            else:
                connection_string = f"mongodb://{host}:27017/"
            lg.info(connection_string)
            client = pymongo.MongoClient(connection_string)
            mydb = client[schema_name]
            mycol = mydb[table_name]
            mycol.drop()
            lg.info("Collection Dropped Successfully!!")
            return "Collection Dropped Successfully!!"
        except Exception as error:
            lg.error(error)
            return error

    def insert_record(self, host, username, password, schema_name, table_name, **columns):
        try:
            if username not in (None, "") and password not in (None, ""):
                connection_string = f"mongodb://{username}:{password}@{host}:27017/"
            else:
                connection_string = f"mongodb://{host}:27017/"
            lg.info(connection_string)
            client = pymongo.MongoClient(connection_string)
            mydb = client[schema_name]
            mycol = mydb[table_name]
            mycol.insert_one(columns)
            lg.info("Record Inserted Successfully!!")
            return "Record Inserted Successfully!!"
        except Exception as error:
            lg.error(error)
            return error

    def insert_multiple_records(self, host, username, password, schema_name, table_name, input_file_name, *columns):
        try:
            if username not in (None, "") and password not in (None, ""):
                connection_string = f"mongodb://{username}:{password}@{host}:27017/"
            else:
                connection_string = f"mongodb://{host}:27017/"
            lg.info(connection_string)
            dataset = list()
            with open(input_file_name, 'r') as data:
                next(data)
                data_csv = csv.reader(data, delimiter='\n')
                for i in data_csv:
                    b = dict()
                    for j in range(len(i[0].split(','))):
                        b[columns[j]] = i[0].split(',')[j]
                    dataset.append(b)
            client = pymongo.MongoClient(connection_string)
            mydb = client[schema_name]
            mycol = mydb[table_name]
            mycol.insert_many(dataset)
            lg.info("Records Inserted Successfully!!")
            return "Records Inserted Successfully!!"
        except Exception as error:
            lg.error(error)
            return error

    def update_records(self, host, username, password, schema_name, table_name, filters, **updated_values):
        try:
            if username not in (None, "") and password not in (None, ""):
                connection_string = f"mongodb://{username}:{password}@{host}:27017/"
            else:
                connection_string = f"mongodb://{host}:27017/"
            lg.info(connection_string)
            client = pymongo.MongoClient(connection_string)
            mydb = client[schema_name]
            mycol = mydb[table_name]
            updated_values = {"$set": updated_values}
            mycol.update_one(filters, updated_values)
            lg.info("Record Updated Successfully!!")
            return "Record Updated Successfully!!"
        except Exception as error:
            lg.error(error)
            return error

    def retrieve_records(self, host, username, password, schema_name, table_name):
        try:
            if username not in (None, "") and password not in (None, ""):
                connection_string = f"mongodb://{username}:{password}@{host}:27017/"
            else:
                connection_string = f"mongodb://{host}:27017/"
            lg.info(connection_string)
            client = pymongo.MongoClient(connection_string)
            mydb = client[schema_name]
            mycol = mydb[table_name]
            mydoc = mycol.find()
            for i in mydoc:
                lg.info(i)
            lg.info("Records Extracted Successfully!!")
            return "Records Extracted Successfully!!"
        except Exception as error:
            lg.error(error)
            return error

    def retrieve_records_with_filter(self, host, username, password, schema_name, table_name, **filters):
        try:
            if username not in (None, "") and password not in (None, ""):
                connection_string = f"mongodb://{username}:{password}@{host}:27017/"
            else:
                connection_string = f"mongodb://{host}:27017/"
            lg.info(connection_string)
            client = pymongo.MongoClient(connection_string)
            mydb = client[schema_name]
            mycol = mydb[table_name]
            mydoc = mycol.find(filters)
            for i in mydoc:
                lg.info(i)
            lg.info("Record Extracted Successfully!!")
            return "Record Extracted Successfully!!"
        except Exception as error:
            lg.error(error)
            return error

    def delete_records_with_filter(self, host, username, password, schema_name, table_name, **filters):
        try:
            if username not in (None, "") and password not in (None, ""):
                connection_string = f"mongodb://{username}:{password}@{host}:27017/"
            else:
                connection_string = f"mongodb://{host}:27017/"
            lg.info(connection_string)
            client = pymongo.MongoClient(connection_string)
            mydb = client[schema_name]
            mycol = mydb[table_name]
            mycol.delete_many(filters)
            lg.info("Record Deleted Successfully!!")
            return "Record Deleted Successfully!!"
        except Exception as error:
            lg.error(error)
            return error

    def delete_records(self, host, username, password, schema_name, table_name):
        try:
            if username not in (None, "") and password not in (None, ""):
                connection_string = f"mongodb://{username}:{password}@{host}:27017/"
            else:
                connection_string = f"mongodb://{host}:27017/"
            lg.info(connection_string)
            client = pymongo.MongoClient(connection_string)
            mydb = client[schema_name]
            mycol = mydb[table_name]
            mycol.delete_many({})
            lg.info("Records Deleted Successfully!!")
            return "Records Deleted Successfully!!"
        except Exception as error:
            lg.error(error)
            return error

    def truncate_table(self, host, username, password, schema_name, table_name):
        try:
            if username not in (None, "") and password not in (None, ""):
                connection_string = f"mongodb://{username}:{password}@{host}:27017/"
            else:
                connection_string = f"mongodb://{host}:27017/"
            lg.info(connection_string)
            client = pymongo.MongoClient(connection_string)
            mydb = client[schema_name]
            mycol = mydb[table_name]
            mycol.remove()
            lg.info("Table Truncated Successfully!!")
            return "Table Truncated Successfully!!"
        except Exception as error:
            lg.error(error)
            return error
