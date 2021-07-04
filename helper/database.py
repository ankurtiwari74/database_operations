from abc import ABC, abstractmethod


class Database(ABC):
    @abstractmethod
    def create_schema(self, host, username, password, schema_name):
        pass

    @abstractmethod
    def drop_schema(self, host, username, password, schema_name):
        pass

    @abstractmethod
    def create_table(self, host, username, password, schema_name, table_name, **columns):
        pass

    @abstractmethod
    def drop_table(self, host, username, password, schema_name, table_name):
        pass

    @abstractmethod
    def insert_record(self, host, username, password, schema_name, table_name, **columns):
        pass

    @abstractmethod
    def insert_multiple_records(self, host, username, password, schema_name, table_name, input_file_name, *columns):
        pass

    @abstractmethod
    def update_records(self, host, username, password, schema_name, table_name, filters, **updated_values):
        pass

    @abstractmethod
    def retrieve_records(self, host, username, password, schema_name, table_name):
        pass

    @abstractmethod
    def retrieve_records_with_filter(self, host, username, password, schema_name, table_name, **filters):
        pass

    @abstractmethod
    def delete_records_with_filter(self, host, username, password, schema_name, table_name, **filters):
        pass

    @abstractmethod
    def delete_records(self, host, username, password, schema_name, table_name):
        pass

    @abstractmethod
    def truncate_table(self, host, username, password, schema_name, table_name):
        pass
