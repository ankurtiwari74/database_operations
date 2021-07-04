from flask import Flask, request, jsonify
from helper.mysql_database import MySQL_Database
from helper.mongo_database import Mongo_Database
from helper.cassandra_database import Cassandra_Database

import json
import logging as lg
import os

app = Flask(__name__)
mysql_db = MySQL_Database()
mongo_db = Mongo_Database()
cassandra_db = Cassandra_Database()
if "logging" not in os.listdir():
    os.mkdir("logging")
lg.basicConfig(filename="logging/log_02_06_2021.log", level=lg.INFO, format='%(asctime)s %(message)s')


@app.route('/')
def index():
    return jsonify("Hello World!!")


@app.route('/mysql/create_schema', methods=["POST"])
def mysql_create_schema():
    if request.method == 'POST':
        host = request.values.get("host")
        username = request.values.get("username")
        password = request.values.get("password")
        schema_name = request.values.get("schema_name")
        result = mysql_db.create_schema(host, username, password, schema_name)
        return jsonify(result)
    else:
        return jsonify("This method is not supported!!")


@app.route('/mysql/drop_schema', methods=["POST"])
def mysql_drop_schema():
    if request.method == 'POST':
        host = request.values.get("host")
        username = request.values.get("username")
        password = request.values.get("password")
        schema_name = request.values.get("schema_name")
        result = mysql_db.drop_schema(host, username, password, schema_name)
        return jsonify(result)
    else:
        return jsonify("This method is not supported!!")


@app.route('/mysql/create_table', methods=["POST"])
def mysql_create_table():
    if request.method == 'POST':
        host = request.values.get("host")
        username = request.values.get("username")
        password = request.values.get("password")
        schema_name = request.values.get("schema_name")
        table_name = request.values.get("table_name")
        columns = request.values.get("columns")
        columns = json.loads(columns)
        result = mysql_db.create_table(host, username, password, schema_name, table_name, **columns)
        return jsonify(result)
    else:
        return jsonify("This method is not supported!!")


@app.route('/mysql/drop_table', methods=["POST"])
def mysql_drop_table():
    if request.method == 'POST':
        host = request.values.get("host")
        username = request.values.get("username")
        password = request.values.get("password")
        schema_name = request.values.get("schema_name")
        table_name = request.values.get("table_name")
        result = mysql_db.drop_table(host, username, password, schema_name, table_name)
        return jsonify(result)
    else:
        return jsonify("This method is not supported!!")


@app.route('/mysql/truncate_table', methods=["POST"])
def mysql_truncate_table():
    if request.method == 'POST':
        host = request.values.get("host")
        username = request.values.get("username")
        password = request.values.get("password")
        schema_name = request.values.get("schema_name")
        table_name = request.values.get("table_name")
        result = mysql_db.truncate_table(host, username, password, schema_name, table_name)
        return jsonify(result)
    else:
        return jsonify("This method is not supported!!")


@app.route('/mysql/insert_record', methods=["POST"])
def mysql_insert_record():
    if request.method == 'POST':
        host = request.values.get("host")
        username = request.values.get("username")
        password = request.values.get("password")
        schema_name = request.values.get("schema_name")
        table_name = request.values.get("table_name")
        columns = request.values.get("columns")
        columns = json.loads(columns)
        result = mysql_db.insert_record(host, username, password, schema_name, table_name, **columns)
        return jsonify(result)
    else:
        return jsonify("This method is not supported!!")


@app.route('/mysql/insert_multiple_records', methods=["POST"])
def mysql_insert_multiple_records():
    if request.method == 'POST':
        host = request.values.get("host")
        username = request.values.get("username")
        password = request.values.get("password")
        schema_name = request.values.get("schema_name")
        table_name = request.values.get("table_name")
        columns = request.values.get("columns")
        columns = columns.replace('[', '').replace(']', '').replace("'", "").replace('"', '').split(",")
        input_file_name = request.values.get("input_file_name")
        result = mysql_db.insert_multiple_records(host, username, password, schema_name, table_name, input_file_name, *columns)
        return jsonify(result)
    else:
        return jsonify("This method is not supported!!")


@app.route('/mysql/update_records', methods=["POST"])
def mysql_update_records():
    if request.method == 'POST':
        host = request.values.get("host")
        username = request.values.get("username")
        password = request.values.get("password")
        schema_name = request.values.get("schema_name")
        table_name = request.values.get("table_name")
        updated_values = request.values.get("updated_values")
        updated_values = json.loads(updated_values)
        filters = request.values.get("filters")
        filters = json.loads(filters)
        result = mysql_db.update_records(host, username, password, schema_name, table_name, filters, **updated_values)
        return jsonify(result)
    else:
        return jsonify("This method is not supported!!")


@app.route('/mysql/retrieve_records', methods=["POST"])
def mysql_retrieve_records():
    if request.method == 'POST':
        host = request.values.get("host")
        username = request.values.get("username")
        password = request.values.get("password")
        schema_name = request.values.get("schema_name")
        table_name = request.values.get("table_name")
        result = mysql_db.retrieve_records(host, username, password, schema_name, table_name)
        return jsonify(result)
    else:
        return jsonify("This method is not supported!!")


@app.route('/mysql/retrieve_records_with_filter', methods=["POST"])
def mysql_retrieve_records_with_filter():
    if request.method == 'POST':
        host = request.values.get("host")
        username = request.values.get("username")
        password = request.values.get("password")
        schema_name = request.values.get("schema_name")
        table_name = request.values.get("table_name")
        filters = request.values.get("filters")
        filters = json.loads(filters)
        result = mysql_db.retrieve_records_with_filter(host, username, password, schema_name, table_name, **filters)
        return jsonify(result)
    else:
        return jsonify("This method is not supported!!")


@app.route('/mysql/delete_records_with_filter', methods=["POST"])
def mysql_delete_records_with_filter():
    if request.method == 'POST':
        host = request.values.get("host")
        username = request.values.get("username")
        password = request.values.get("password")
        schema_name = request.values.get("schema_name")
        table_name = request.values.get("table_name")
        filters = request.values.get("filters")
        filters = json.loads(filters)
        result = mysql_db.delete_records_with_filter(host, username, password, schema_name, table_name, **filters)
        return jsonify(result)
    else:
        return jsonify("This method is not supported!!")


@app.route('/mysql/delete_records', methods=["POST"])
def mysql_delete_records():
    if request.method == 'POST':
        host = request.values.get("host")
        username = request.values.get("username")
        password = request.values.get("password")
        schema_name = request.values.get("schema_name")
        table_name = request.values.get("table_name")
        result = mysql_db.delete_records(host, username, password, schema_name, table_name)
        return jsonify(result)
    else:
        return jsonify("This method is not supported!!")


########################################################################################################################
@app.route('/cassandra/create_schema', methods=["POST"])
def cassandra_create_schema():
    if request.method == 'POST':
        host = request.values.get("host")
        username = request.values.get("username")
        password = request.values.get("password")
        schema_name = request.values.get("schema_name")
        result = cassandra_db.create_schema(host, username, password, schema_name)
        return jsonify(result)
    else:
        return jsonify("This method is not supported!!")


@app.route('/cassandra/drop_schema', methods=["POST"])
def cassandra_drop_schema():
    if request.method == 'POST':
        host = request.values.get("host")
        username = request.values.get("username")
        password = request.values.get("password")
        schema_name = request.values.get("schema_name")
        result = cassandra_db.drop_schema(host, username, password, schema_name)
        return jsonify(result)
    else:
        return jsonify("This method is not supported!!")


@app.route('/cassandra/create_table', methods=["POST"])
def cassandra_create_table():
    if request.method == 'POST':
        host = request.values.get("host")
        username = request.values.get("username")
        password = request.values.get("password")
        schema_name = request.values.get("schema_name")
        table_name = request.values.get("table_name")
        columns = request.values.get("columns")
        columns = json.loads(columns)
        result = cassandra_db.create_table(host, username, password, schema_name, table_name, **columns)
        return jsonify(result)
    else:
        return jsonify("This method is not supported!!")


@app.route('/cassandra/drop_table', methods=["POST"])
def cassandra_drop_table():
    if request.method == 'POST':
        host = request.values.get("host")
        username = request.values.get("username")
        password = request.values.get("password")
        schema_name = request.values.get("schema_name")
        table_name = request.values.get("table_name")
        result = cassandra_db.drop_table(host, username, password, schema_name, table_name)
        return jsonify(result)
    else:
        return jsonify("This method is not supported!!")


@app.route('/cassandra/truncate_table', methods=["POST"])
def cassandra_truncate_table():
    if request.method == 'POST':
        host = request.values.get("host")
        username = request.values.get("username")
        password = request.values.get("password")
        schema_name = request.values.get("schema_name")
        table_name = request.values.get("table_name")
        result = cassandra_db.truncate_table(host, username, password, schema_name, table_name)
        return jsonify(result)
    else:
        return jsonify("This method is not supported!!")


@app.route('/cassandra/insert_record', methods=["POST"])
def cassandra_insert_record():
    if request.method == 'POST':
        host = request.values.get("host")
        username = request.values.get("username")
        password = request.values.get("password")
        schema_name = request.values.get("schema_name")
        table_name = request.values.get("table_name")
        columns = request.values.get("columns")
        columns = json.loads(columns)
        result = cassandra_db.insert_record(host, username, password, schema_name, table_name, **columns)
        return jsonify(result)
    else:
        return jsonify("This method is not supported!!")


@app.route('/cassandra/insert_multiple_records', methods=["POST"])
def cassandra_insert_multiple_records():
    if request.method == 'POST':
        host = request.values.get("host")
        username = request.values.get("username")
        password = request.values.get("password")
        schema_name = request.values.get("schema_name")
        table_name = request.values.get("table_name")
        columns = request.values.get("columns")
        columns = columns.replace('[', '').replace(']', '').replace("'", "").replace('"', '').split(",")
        input_file_name = request.values.get("input_file_name")
        result = cassandra_db.insert_multiple_records(host, username, password, schema_name,
                                                      table_name, input_file_name, *columns)
        return jsonify(result)
    else:
        return jsonify("This method is not supported!!")


@app.route('/cassandra/update_records', methods=["POST"])
def cassandra_update_records():
    if request.method == 'POST':
        host = request.values.get("host")
        username = request.values.get("username")
        password = request.values.get("password")
        schema_name = request.values.get("schema_name")
        table_name = request.values.get("table_name")
        updated_values = request.values.get("updated_values")
        updated_values = json.loads(updated_values)
        filters = request.values.get("filters")
        filters = json.loads(filters)
        result = cassandra_db.update_records(host, username, password, schema_name, table_name, filters,
                                             **updated_values)
        return jsonify(result)
    else:
        return jsonify("This method is not supported!!")


@app.route('/cassandra/retrieve_records', methods=["POST"])
def cassandra_retrieve_records():
    if request.method == 'POST':
        host = request.values.get("host")
        username = request.values.get("username")
        password = request.values.get("password")
        schema_name = request.values.get("schema_name")
        table_name = request.values.get("table_name")
        result = cassandra_db.retrieve_records(host, username, password, schema_name, table_name)
        return jsonify(result)
    else:
        return jsonify("This method is not supported!!")


@app.route('/cassandra/retrieve_records_with_filter', methods=["POST"])
def cassandra_retrieve_records_with_filter():
    if request.method == 'POST':
        host = request.values.get("host")
        username = request.values.get("username")
        password = request.values.get("password")
        schema_name = request.values.get("schema_name")
        table_name = request.values.get("table_name")
        filters = request.values.get("filters")
        filters = json.loads(filters)
        result = cassandra_db.retrieve_records_with_filter(host, username, password, schema_name, table_name, **filters)
        return jsonify(result)
    else:
        return jsonify("This method is not supported!!")


@app.route('/cassandra/delete_records_with_filter', methods=["POST"])
def cassandra_delete_records_with_filter():
    if request.method == 'POST':
        host = request.values.get("host")
        username = request.values.get("username")
        password = request.values.get("password")
        schema_name = request.values.get("schema_name")
        table_name = request.values.get("table_name")
        filters = request.values.get("filters")
        filters = json.loads(filters)
        result = cassandra_db.delete_records_with_filter(host, username, password, schema_name, table_name, **filters)
        return jsonify(result)
    else:
        return jsonify("This method is not supported!!")


@app.route('/cassandra/delete_records', methods=["POST"])
def cassandra_delete_records():
    if request.method == 'POST':
        host = request.values.get("host")
        username = request.values.get("username")
        password = request.values.get("password")
        schema_name = request.values.get("schema_name")
        table_name = request.values.get("table_name")
        result = cassandra_db.delete_records(host, username, password, schema_name, table_name)
        return jsonify(result)
    else:
        return jsonify("This method is not supported!!")


########################################################################################################################
@app.route('/mongo/create_schema', methods=["POST"])
def mongo_create_schema():
    if request.method == 'POST':
        host = request.values.get("host")
        username = request.values.get("username")
        password = request.values.get("password")
        schema_name = request.values.get("schema_name")
        result = mongo_db.create_schema(host, username, password, schema_name)
        return jsonify(result)
    else:
        return jsonify("This method is not supported!!")


@app.route('/mongo/drop_schema', methods=["POST"])
def mongo_drop_schema():
    if request.method == 'POST':
        host = request.values.get("host")
        username = request.values.get("username")
        password = request.values.get("password")
        schema_name = request.values.get("schema_name")
        result = mongo_db.drop_schema(host, username, password, schema_name)
        return jsonify(result)
    else:
        return jsonify("This method is not supported!!")


@app.route('/mongo/create_table', methods=["POST"])
def mongo_create_table():
    if request.method == 'POST':
        host = request.values.get("host")
        username = request.values.get("username")
        password = request.values.get("password")
        schema_name = request.values.get("schema_name")
        table_name = request.values.get("table_name")
        columns = request.values.get("columns")
        columns = json.loads(columns)
        result = mongo_db.create_table(host, username, password, schema_name, table_name, **columns)
        return jsonify(result)
    else:
        return jsonify("This method is not supported!!")


@app.route('/mongo/drop_table', methods=["POST"])
def mongo_drop_table():
    if request.method == 'POST':
        host = request.values.get("host")
        username = request.values.get("username")
        password = request.values.get("password")
        schema_name = request.values.get("schema_name")
        table_name = request.values.get("table_name")
        result = mongo_db.drop_table(host, username, password, schema_name, table_name)
        return jsonify(result)
    else:
        return jsonify("This method is not supported!!")


@app.route('/mongo/truncate_table', methods=["POST"])
def mongo_truncate_table():
    if request.method == 'POST':
        host = request.values.get("host")
        username = request.values.get("username")
        password = request.values.get("password")
        schema_name = request.values.get("schema_name")
        table_name = request.values.get("table_name")
        result = mongo_db.truncate_table(host, username, password, schema_name, table_name)
        return jsonify(result)
    else:
        return jsonify("This method is not supported!!")


@app.route('/mongo/insert_record', methods=["POST"])
def mongo_insert_record():
    if request.method == 'POST':
        host = request.values.get("host")
        username = request.values.get("username")
        password = request.values.get("password")
        schema_name = request.values.get("schema_name")
        table_name = request.values.get("table_name")
        columns = request.values.get("columns")
        columns = json.loads(columns)
        result = mongo_db.insert_record(host, username, password, schema_name, table_name, **columns)
        return jsonify(result)
    else:
        return jsonify("This method is not supported!!")


@app.route('/mongo/insert_multiple_records', methods=["POST"])
def mongo_insert_multiple_records():
    if request.method == 'POST':
        host = request.values.get("host")
        username = request.values.get("username")
        password = request.values.get("password")
        schema_name = request.values.get("schema_name")
        table_name = request.values.get("table_name")
        columns = request.values.get("columns")
        columns = columns.replace('[', '').replace(']', '').replace("'", "").replace('"', '').split(",")
        input_file_name = request.values.get("input_file_name")
        result = mongo_db.insert_multiple_records(host, username, password, schema_name, table_name, input_file_name, *columns)
        return jsonify(result)
    else:
        return jsonify("This method is not supported!!")


@app.route('/mongo/update_records', methods=["POST"])
def mongo_update_records():
    if request.method == 'POST':
        host = request.values.get("host")
        username = request.values.get("username")
        password = request.values.get("password")
        schema_name = request.values.get("schema_name")
        table_name = request.values.get("table_name")
        updated_values = request.values.get("updated_values")
        updated_values = json.loads(updated_values)
        filters = request.values.get("filters")
        filters = json.loads(filters)
        result = mongo_db.update_records(host, username, password, schema_name, table_name, filters, **updated_values)
        return jsonify(result)
    else:
        return jsonify("This method is not supported!!")


@app.route('/mongo/retrieve_records', methods=["POST"])
def mongo_retrieve_records():
    if request.method == 'POST':
        host = request.values.get("host")
        username = request.values.get("username")
        password = request.values.get("password")
        schema_name = request.values.get("schema_name")
        table_name = request.values.get("table_name")
        result = mongo_db.retrieve_records(host, username, password, schema_name, table_name)
        return jsonify(result)
    else:
        return jsonify("This method is not supported!!")


@app.route('/mongo/retrieve_records_with_filter', methods=["POST"])
def mongo_retrieve_records_with_filter():
    if request.method == 'POST':
        host = request.values.get("host")
        username = request.values.get("username")
        password = request.values.get("password")
        schema_name = request.values.get("schema_name")
        table_name = request.values.get("table_name")
        filters = request.values.get("filters")
        filters = json.loads(filters)
        result = mongo_db.retrieve_records_with_filter(host, username, password, schema_name, table_name, **filters)
        return jsonify(result)
    else:
        return jsonify("This method is not supported!!")


@app.route('/mongo/delete_records_with_filter', methods=["POST"])
def mongo_delete_records_with_filter():
    if request.method == 'POST':
        host = request.values.get("host")
        username = request.values.get("username")
        password = request.values.get("password")
        schema_name = request.values.get("schema_name")
        table_name = request.values.get("table_name")
        filters = request.values.get("filters")
        filters = json.loads(filters)
        result = mongo_db.delete_records_with_filter(host, username, password, schema_name, table_name, **filters)
        return jsonify(result)
    else:
        return jsonify("This method is not supported!!")


@app.route('/mongo/delete_records', methods=["POST"])
def mongo_delete_records():
    if request.method == 'POST':
        host = request.values.get("host")
        username = request.values.get("username")
        password = request.values.get("password")
        schema_name = request.values.get("schema_name")
        table_name = request.values.get("table_name")
        result = mongo_db.delete_records(host, username, password, schema_name, table_name)
        return jsonify(result)
    else:
        return jsonify("This method is not supported!!")


if __name__ == '__main__':
    app.debug = True
    app.run()
