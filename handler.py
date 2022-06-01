import pymysql
import json
from datetime import date, datetime

# 1. Install pymysql to local directory
# pip3 install -t $PWD pymysql

# 2. (If using lambda) Write your code, then select all files and zip into function.zip
# a) Mac/Linux --> zip -r9 ${PWD}/function.zip

# Lambda Permissions:
# AWSLambdaVPCAccessExecutionRole
# remove info

# Configuration
database_endpoint = ''
username = ''
password = ''
database_name = ''


# Connections
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


def execute_statement(query, param):
    cursor = connection.cursor()
    query_statement = query.format(param)
    print(query_statement)
    cursor.execute(query_statement)
    return list(cursor.fetchall())


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

def create_account_record(record):
    response = {}
    response['accountNumber'] = record[0]
    response['customerId'] = record[1]
    response['type'] = record[2]
    response['balance'] = record[3]
    response['createdDate'] = record[4]
    response['active'] = record[5]
    return response


def create_transaction_info(record):
    response = {}
    response['transactionId'] = record[0]
    response['customerId'] = record[1]
    response['createdDate'] = record[2]
    status = record[3]
    response['status'] = status
    if status == 'COMPLETED':
        response['updatedDate'] = record[4]
    elif status == 'CANCELLED':
        response['updatedDate'] = record[5]
    elif status == 'REJECTED':
        response['updatedDate'] = record[6]

    # need flex between business and individual
    if record[7] == 'BUSINESS':
        response['customerFullName'] = record[10]
    else:
        response['customerFullName'] = str(record[9]) + ', ' + str(record[8])
    response['accountNumber'] = record[11]
    response['accountBalance'] = record[12]
    return response


def create_http_response(code, records):
    http_response = {}
    http_response['statusCode'] = code
    http_response['headers'] = {
        'Access-Control-Allow-Headers': 'Content-Type',
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
    }
    http_response['headers']['Content-Type'] = 'application/json'
    http_response['body'] = json.dumps(records, default=json_serial)
    return http_response


def process_transactions(method, path):
    response = {}
    ids = get_path_ids(path)
    if not ids:
        return response

    all_records = []
    if method == 'GET':
        records = execute_statement('SELECT * from Transaction where transactionId in ({})', ids)
        for record in records:
            all_records.append(create_transaction_record(record))

    http_response = create_http_response(200, all_records)
    return http_response


def process_transaction_info(method, path, query_params):
    response = {}
    query_key = ''
    query_value = ''
    for key, value in query_params.items():
        query_key = key
        query_value = value

    all_records = []
    if method == 'GET':
        query = "SELECT t.transactionId, t.customerId, t.createdDate, t.status, t.completedDate, t.cancelledDate, t.rejectedDate, c.customerGrouping, " \
                "c.firstName, c.lastName, c.businessName, a.accountNumber, a.balance FROM Transaction t " \
                "INNER JOIN Customer c ON c.customerId = t.customerId " \
                "INNER JOIN Account a ON a.customerId = t.customerId " \
                "WHERE t.transactionId = {} "
        records = execute_statement(query, query_value)
        for record in records:
            all_records.append(create_transaction_info(record))

    http_response = create_http_response(200, all_records)
    return http_response


def process_accounts(method, path):
    response = {}
    ids = get_path_ids(path)
    if not ids:
        return response

    all_records = []
    if method == 'GET':
        records = execute_statement('SELECT * from Account where accountNumber in ({})', ids)
        for record in records:
            all_records.append(create_account_record(record))

    http_response = create_http_response(200, all_records)
    return http_response


def process_customers(method, path):
    response = {}
    http_response = create_http_response(404, [])
    return http_response


def lambda_handler(event, context):
    # event structure
    # "path":"/transactions/123"
    # "headers": {"Accept": "application/json"}
    # "pathParameters": {"userName":"user1"}
    # "httpMethod":"GET"
    # "queryStringParameters":{"location":"USA","age":"25}

    method = event['httpMethod']
    path = event['path']
    query_params = {}
    query_params = event['queryStringParameters']

    parts = path.split("/")
    root = parts[1]

    response = {}
    print('root of path: ' + str(root))
    if root == 'transactions':
        response = process_transactions(method, path)
    elif root == 'accounts':
        response = process_accounts(method, path)
    elif root == 'customers':
        response = process_customers(method, path)
    elif root == 'transactioninfo':
        response = process_transaction_info(method, path, query_params)
    else:
        response = create_http_response(500, ["API not supported"])

    return response
