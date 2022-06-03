import pymysql
import json
import ast
from datetime import date, datetime

# 1. Install pymysql to local directory
# pip3 install -t $PWD pymysql

# 2. (If using lambda) Write your code, then select all files and zip into function.zip
# a) Mac/Linux --> zip -r9 ${PWD}/function.zip

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
    global cursor
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
    
def create_customer_record(record):
    response = {}
    response['customerId'] = record[0]
    response['firstName'] = record[1]
    response['lastName'] = record[2]
    response['businessName'] = record[3]
    response['phone'] = record[4]
    response['email'] = record[5]
    response['addressLine1'] = record[6]
    response['addressLine2'] = record[7]
    response['city'] = record[8]
    response['state'] = record[9]
    response['country'] = record[10]
    response['customerGrouping'] = record[11]
    response['owner'] = record[12]
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


def process_transactions(method, path, event):
    all_records = []
    
    if method == 'GET':
        response = {}
        ids = get_path_ids(path)
        if not ids:
            return response
            
        records = execute_statement('SELECT * FROM transactions WHERE transactionid IN ({})', ids)
        for record in records:
            all_records.append(create_transaction_record(record))
            
    elif method == 'POST':
        cols = ''
        vals = ''
        data = json.loads(event['body'])

        for key, value in data.items(): ### NEED TO BUILD IN HANDLING, IF WE RECEIVE MULTIPLE TRANSACTIONS IN SINGLE PAY-LOAD
            cols += key + ','
            vals += value + ','
            
        insert_statement = "INSERT INTO transactions VALUES ({})"
        
        try:
            execute_statement(insert_statement, vals[:-1])
            print("\nINSERT SUCCESSFUL...COMMITING CHANGES...\n")
            connection.commit()

            print("Number of records inserted: ", cursor.rowcount)
            print("TransactionID of inserted record:", cursor.lastrowid)
            last_transaction = cursor.lastrowid
            
            print("\nChecking customers balance to validate transaction...\n")
            balance_check_statment = "SELECT t.transactionid, t.amount, t.creditordebit, t.customerid, t.createddate, t.status, t.completeddate, t.cancelleddate, t.rejecteddate, c.customergrouping, " \
                                    "c.firstname, c.lastname, c.businessname, a.accountnumber, a.balance FROM transactions t " \
                                    "INNER JOIN customers c ON c.customerid = t.customerid " \
                                    "INNER JOIN accounts a ON a.customerid = t.customerid " \
                                    "WHERE t.transactionid = {}"

            results = execute_statement(balance_check_statment, last_transaction)

            for result in results:           
                print(f"\nCurrent Account balance: {result[-1]}")   

                if ((result[-1] > result[1]) & (result[2] == 1)) | (result[2] == 0):

                    try:
                         ### UPDATING TRANSACTION ###
                        update_transaction = "UPDATE transactions " \
                                    f"SET completeddate = '{datetime.now()}', status = 'COMPLETED' " \
                                    "WHERE transactionid = {}" 

                        print("\nUpdating transaction...")
                        execute_statement(update_transaction, last_transaction)
                        print("\nNumber of records updated: ", cursor.rowcount)
                        print("\nTransactionID of updated record:", last_transaction)

                        ### UPDATING ACCOUNT BALANCE ###

                        if result[2] == 1: #CREDIT TRANSACTION
                            print(f"""\nCustomer has enough balance for transaction #{last_transaction} for the amt of {result[1]}...
                                      \nUpdating account balance...""")

                            transaction_type = "Credit" 
                            new_balance = result[-1] - result[1]

                        elif result[2] == 0: #DEBIT TRANSACTION
                            transaction_type = "Debit"
                            new_balance = result[-1] + result[1]

                        update_balance = "UPDATE accounts "\
                            f"SET balance = {new_balance} "\
                            "WHERE accountnumber = {}"

                        account_num = result[-2]

                        print(f'\n{transaction_type}ing Account balance...')
                        execute_statement(update_balance, account_num)
                        print("\nNumber of records updated: ", cursor.rowcount)
                        print("\nAccountID of updated record:", account_num)        
                        print(f"\nCustomer new balance: {new_balance}\n")

                        connection.commit()

                    except Exception as e:
                        print("\nError while making Transaction/Account updates...ROLLING BACK CHANGES...")
                        print("\nError: \n", e)
                        connection.rollback()                  

                else:
                    print(f"\nCustomer has insufficient funds for transaction #{last_transaction}...\nRejecting transaction...")

                    update_transaction = "UPDATE transactions "\
                                            f"SET rejecteddate = '{datetime.now()}', status = 'REJECTED' " \
                                            "WHERE transactionid = {}"

                    try:
                        print("\nUpdating transaction...")
                        execute_statement(update_transaction, last_transaction)
                        print("\nNumber of records updated: ", cursor.rowcount)
                        print("\nTransactionID of updated record:", last_transaction)
                        connection.commit()

                    except Exception as e:
                        print("\nUpdating transaction FAILED...ROLLING BACK CHANGES...")
                        print("\nError: ", e)  
                        connection.rollback() 
                        
            records = execute_statement('SELECT * from transactions where transactionid in ({})', last_transaction)
            for record in records:
                all_records.append(create_transaction_record(record))

        except Exception as e:
            print("\nINSERT FAILED...ROLLING BACK CHANGES...\n")
            print("Error: ", e)
            connection.rollback()      
        
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
        query = "SELECT t.transactionid, t.customerid, t.createddate, t.status, t.completeddate, t.cancelleddate, t.rejecteddate, c.customergrouping, " \
                "c.firstname, c.lastname, c.businessname, a.accountnumber, a.balance FROM transactions t " \
                "INNER JOIN customers c ON c.customerid = t.customerid " \
                "INNER JOIN accounts a ON a.customerid = t.customerid " \
                "WHERE t.transactionid = {} "
        
        records = execute_statement(query, query_value)
        for record in records:
            all_records.append(create_transaction_info(record))

    http_response = create_http_response(200, all_records)
    return http_response


def process_accounts(method, path, event):
    all_records = []

    if method == 'GET':
        response = {}
        ids = get_path_ids(path)
        if not ids:
            return response
        
        records = execute_statement('SELECT * from accounts where accountNumber in ({})', ids) 
        for record in records:
            all_records.append(create_account_record(record))            
    
    elif method == 'POST':
        cols = ''
        vals = ''
        data = json.loads(event['body'])

        for key, value in data.items():
            cols += key + ','
            vals += value + ','

        insert_statement = "INSERT INTO accounts VALUES ({})" 

        try:
            execute_statement(insert_statement, vals[:-1])
            print("\nINSERT SUCCESSFUL...COMMITING CHANGES...\n")
            connection.commit()

            print("Number of records inserted: ", cursor.rowcount)
            print("Account number of inserted record:", cursor.lastrowid)
            last_account = cursor.lastrowid
            records = execute_statement('SELECT * from accounts where accountNumber in ({})', last_account)
            for record in records:
                all_records.append(create_account_record(record))
            
        except Exception as e:
            print("\nINSERT FAILED...ROLLING BACK CHANGES...\n")
            print("Error: ", e)
            connection.rollback()      

    http_response = create_http_response(200, all_records)
    return http_response


def process_customers(method, path, event):  
    all_records = []
    
    if method == 'GET':
        response = {}
        ids = get_path_ids(path)
        if not ids:
            return response

        records = execute_statement('SELECT * FROM customers where customerID in ({})', ids) 
        for record in records:
            all_records.append(create_customer_record(record))
            
    elif method == 'POST':
        cols = ''
        vals = ''
        data = json.loads(event['body'])

        for key, value in data.items():
            cols += key + ','
            vals += value + ','

        insert_statement = "INSERT INTO customers VALUES ({})" 

        try:
            execute_statement(insert_statement, vals[:-1])
            print("\nINSERT SUCCESSFUL...COMMITING CHANGES...\n")
            connection.commit()

            print("Number of records inserted: ", cursor.rowcount)
            print("CustomerID of inserted record:", cursor.lastrowid)
            last_customer = cursor.lastrowid
            records = execute_statement('SELECT * FROM customers where customerID in ({})', last_customer)
            for record in records:
                all_records.append(create_customer_record(record))

        except Exception as e:
            print("\nINSERT FAILED...ROLLING BACK CHANGES...\n")
            print("Error: ", e)
            connection.rollback()  
    
    http_response = create_http_response(200, all_records)
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
        response = process_transactions(method, path, event)
    elif root == 'accounts':
        response = process_accounts(method, path, event)
    elif root == 'customers':
        response = process_customers(method, path, event)
    elif root == 'transactioninfo':
        response = process_transaction_info(method, path, query_params)
    else:
        response = create_http_response(500, ["API not supported"])

    return response