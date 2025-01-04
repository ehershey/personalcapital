#!/usr/bin/python3
#
# environment variables:
# PC_USER
# PC_PASS
# PC_MONGODB_URI

import locale
import personalcapital
from datetime import datetime
from datetime import timedelta
import pymongo
from pymongo import MongoClient
import os
import sys

locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')  # Set locale for US English

email = os.getenv("PC_USER")
if not email:
    sys.stderr.write("must set $PC_USER\n")
    sys.exit(1)
password = os.getenv("PC_PASS")
if not password:
    sys.stderr.write("must set $PC_PASS\n")
    sys.exit(1)
mongodb_uri = os.getenv("PC_MONGODB_URI")
if not mongodb_uri:
    sys.stderr.write("must set $PC_MONGODB_URI\n")
    sys.exit(1)

db_name = "personal_capital"

nw_collection_name = "net_worth"

transaction_summary_collection_name = "transaction_summary"
transaction_collection_name = "transactions"

client = MongoClient(mongodb_uri)

db = client[db_name]
nw_collection = db[nw_collection_name]
transaction_collection = db[transaction_collection_name]
transaction_summary_collection = db[transaction_summary_collection_name]

pc = personalcapital.PersonalCapital()

# Create index on userTransactionId
index_result = transaction_collection.create_index([("userTransactionId", pymongo.ASCENDING)],
                                                   unique=True)
print("indexes:")
print(sorted(list(transaction_collection.index_information())))


try:
    pc.login(email, password)
except personalcapital.RequireTwoFactorException as err:
    print(f"You will be smsed a two factor code ({err})")
    mode = personalcapital.TwoFactorVerificationModeEnum.SMS
    pc.two_factor_challenge(mode)
    pc.two_factor_authenticate(mode, input('Enter sms code: '))
    pc.authenticate_password(password)
accounts_response = pc.fetch('/newaccount/getAccounts')
networth = accounts_response.json()['spData']['networth']
formatted_networth = locale.currency(networth, grouping=True)
print(f'Net worth: {formatted_networth}')

nw = {"networth": networth}

nw_inserted_id = nw_collection.insert_one(nw).inserted_id
print(f"nw_inserted_id: {nw_inserted_id}")

now = datetime.now()
date_format = '%Y-%m-%d'
days = 90
start_date = (now - (timedelta(days=days+1))).strftime(date_format)
end_date = (now - (timedelta(days=1))).strftime(date_format)
transactions_response = pc.fetch('/transaction/getUserTransactions', {
    'sort_cols': 'transactionTime',
    'sort_rev': 'true',
    'page': '0',
    'rows_per_page': '100',
    'startDate': start_date,
    'endDate': end_date,
    'component': 'DATAGRID'
})

transaction_summary = transactions_response.json()['spData']

transactions = transaction_summary['transactions']

# don't store transactiond etails in summary doc
del (transaction_summary['transactions'])

print('Downloaded {0} transactions between {1} and {2}'.format(
    len(transactions),
    transaction_summary['startDate'],
    transaction_summary['endDate'],))


transaction_summary_inserted_id = transaction_summary_collection.insert_one(
    transaction_summary).inserted_id
print(f"transaction_summary_inserted_id: {transaction_summary_inserted_id}")


inserted_count = 0
for transaction in transactions:
    replace_response = transaction_collection.replace_one({
        "userTransactionId": transaction["userTransactionId"]}, transaction, upsert=True)
    if replace_response.upserted_id:
        inserted_count += 1

print(f"Inserted {inserted_count} new transactions")
