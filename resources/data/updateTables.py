#!/usr/bin/env python3

import random
import time
import argparse
import boto3
import mysql.connector
from mysql.connector import errorcode
import os;
os.environ['LIBMYSQL_ENABLE_CLEARTEXT_PLUGIN'] = '1'

cities = ['BOM','DEL','BLR','CAL','MAA','GOI','COK','HYD','PNQ','SXR','TRV','AMD','ZER']
trxtypes = ['CREDIT','DEBIT','NONMON']
services = ['FUNDTRANSFER','UPITRANSACTION','ENQUIRY','FOREX','MF-INVEST','CAPITALMARKET-INVEST','FD-INVEST','PENSIONFUND-INVEST']

DB_NAME="workshopDb"

def updateTables(user,city,dbconn):
    cursor = dbconn.cursor(prepared=True)
    stmt = """INSERT INTO workshopDb.customeractivity(user_id,city,transaction_type,monetary_value,timeinapp,feature_used) VALUES(%s,%s,%s,%s,%s,%s)"""
    cursor.execute(stmt,(user,city,random.choice(trxtypes),round(random.uniform(100.00,10000.00),2),random.randint(100,180),random.choice(services)))
    dbconn.commit()
    cursor.close()

## TEST ##
#dbconn = mysql.connector.connect(user='admin',password='master123', host='datagenstack-datasourcedb-f2xrome5ajrm.cluster-ckyzeld8ogd5.us-east-1.rds.amazonaws.com')
#print("Created the connection..")
#normal_wf(user,city,dbconn)
#print("Inserted the data..")
#dbconn.close()
## TEST COMPLETE##

#Initialize parser
parser = argparse.ArgumentParser()

#Add argument flag
parser.add_argument("-e","--endpoint",help = "DB end point")
parser.add_argument("-u","--user",help = "user name")

#Read arguments from commandline
args = parser.parse_args()

if args.endpoint:
    endpoint = args.endpoint
if args.user:
    user = args.user

with open('account_ids.txt') as fr:
    lines = fr.readlines()

session = boto3.Session();
client = session.client('rds')
token = client.generate_db_auth_token(DBHostname=endpoint, Port=3306, DBUsername=user, Region=session.region_name)
dbconn = mysql.connector.connect(user=user,password=token, host=endpoint)

for userid in lines[:1000]:
    print("Inserting data for userid - {}".format(userid))
    updateTables(userid,random.choice(cities),dbconn)
    time.sleep(5)
dbconn.close()
