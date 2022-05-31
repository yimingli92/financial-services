import pymysql
import json
from datetime import date, datetime

# 1. Install pymysql to local directory
# pip3 install -t $PWD pymysql

# 2. (If using lambda) Write your code, then select all files and zip into function.zip
# a) Mac/Linux --> zip -r9 ${PWD}/function.zip

# Lambda Permissions:
# AWSLambdaVPCAccessExecutionRole
# remove critical information

# Configuration
database_endpoint = ''
username = ''
password = ''
database_name = ''

transactionPath = '/transaction'
transactionsPath = '/transactions'

# Connections
# connection = pymysql.connect(database_endpoint, user=username, passwd=password, db=database_name)
connection = pymysql.connect(host=database_endpoint, port=3306, user=username, passwd=password, db=database_name)


def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError ("Type %s not serializable" % type(obj))


def get_path_ids(path):
    parts = path.split("/")
    if len(parts) == 2:
        return

    ids = ''
    for i in range(2, len(parts)):
        ids += (parts[i]) + ','

    return ids[:-1]


def get_list_str(ids):
    idin = ''
    for id in ids:
        idin += id + ','
    return idin[:-1]


def create_transaction_record(record):
    response = {}
    response['transactionId'] = record[0]
    response['customerId'] = record[1]
    response['amount'] = record[2]
    response['creditOrDebit'] = record[3]
    response['currency'] = record[4]
    response['createdDate'] = record[5]
    response['completedDate'] = record[6]
    response['cancelledDate'] = record[7]
    response['rejectedDate'] = record[8]
    response['status'] = record[9]
    return response


def create_http_response(response):
    http_response = {}
    http_response['statusCode'] = 200
    http_response['headers'] = {}
    http_response['headers']['Content-Type'] = 'application/json'
    http_response['body'] = json.dumps(response, default=json_serial)
    return http_response


def execute_statement(query, param):
    cursor = connection.cursor()
    cursor.execute(query.format(param))
    return list(cursor.fetchall())


def lambda_handler():
    # 1. Parse out query string params:
    # transationId = event['queryStringParameters']['transactionId']

    ids = ['10001', '10002', '10003']
    idin = get_list_str(ids)

    records = execute_statement('SELECT * from Transaction where transactionId in ({})', idin)
    all_records = []
    for record in records:
        all_records.append(create_transaction_record(record))

    http_response = create_http_response(all_records)
    print(http_response['body'])

lambda_handler()
