#!/usr/bin/env python3

import random 
import argparse
import logging
import boto3
from botocore.exceptions import ClientError
import mysql.connector
from mysql.connector import errorcode

import os;
os.environ['LIBMYSQL_ENABLE_CLEARTEXT_PLUGIN'] = '1'


userid = []
city = ['BOM','DEL','BLR','CAL','MAA','GOI','COK','HYD','PNQ','SXR','TRV','AMD','ZER']
trxtype = ['CREDIT','DEBIT','NONMON']
inrvalue = 0 ## An INR value between 100.00 and 10000000.00
features = dict()
features = {0: 'fundtransfer',1: 'upi',2: 'enquiry',3: 'forex',4: 'mutualfund-investment',5: 'capitalmarket-investment',6: 'FD',7: 'PensionFund-investment'}

def generateaccnnos():
    '''Returns a list of account numbers'''
    return random.sample(range(100000,1000000),500000)

# We now create a table with the following fields
# userid|city|transactiontype|inrvalue|timeinapp|featureused|timestamp

## Some constraints that the data should be always met by the data, else, this needs
## investigation.
## The app/account cannot be accessed by the same user from different cities within an hour.
## There shouldn't be an overlap in app usage between two sessions for the same account.
## If the a customer tries to withdraw more than the amount available in the account, the amount
#    should not be operative for 24 hrs after the incident, and the customers should be informed of this.
## If a customer never does forex transactions but does it this time, the customer has to enable this
#    facility, customer service should call this customer to find out the reason for this and enable the service.
## A customer should not spend the maximum limit on UPI transactions on 3 consecutive days.

### Some patterns that need to be observed and leveraged.
## Multiple enquiries implies the customer is looking to buy a product.
## Regular forex transactions requires continuous scrutiny, elevated relationship engagement with an RM.
## Regular MF investor, may be a high value customer.
## Large cpaital market investor may be a high value customer.
## Regular pension fund investor may need other investment products that have a better balance of risk and reward.

lastAccnNo = 000000
DB_NAME = 'workshopDb'
TABLES = {}
TABLES['customeractivity'] = (
        "CREATE TABLE `customeractivity` ("
        "`user_id` int(11) NOT NULL, "
        "`city` varchar(3) NOT NULL, "
        "`transaction_type` enum('CREDIT','DEBIT','NON_MONETARY'), "
        "`monetary_value` decimal(8,2) NOT NULL, "
        "`timeinapp` smallint NOT NULL, "
        "`feature_used` enum('FUNDTRANSFER','UPITRANSACTION','ENQUIRY','FOREX','MF-INVEST','CAPITALMARKET-INVEST','FD-INVEST','PENSIONFUND-INVEST') NOT NULL, "
        "ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP NOT NULL, "
        "PRIMARY KEY (`user_id`))"
        )

TABLES['debezium'] = (
        "CREATE USER 'replicator'@'%' IDENTIFIED BY 'repltr'"
        )

TABLES['grants'] = (
        "GRANT SELECT,RELOAD,SHOW DATABASES,REPLICATION SLAVE,REPLICATION CLIENT,LOCK TABLES ON *.* TO 'replicator' IDENTIFIED BY 'repltr'"
        )

TABLES['encrypt_connection'] = (
        "ALTER USER 'replicator'@'%' REQUIRE SSL"
        )

def setupTables(endpoint, user):

    session = boto3.Session();
    client = session.client('rds')
    token = client.generate_db_auth_token(DBHostname=endpoint, Port=3306, DBUsername=user, Region=session.region_name)

    print("Setting up Tables...")
    print("Password token...", token)
    
    ## We already expect the workshopDb to exist
    try:
        conn = mysql.connector.connect(user=user,password=token, host=endpoint)
        cursor = conn.cursor()
    except Exception as e:
        print("Database connection failed due to {}".format(e))   
        exit(1) 
        
    try:
        print("Creating the table structure...")
        cursor.execute("USE {}".format(DB_NAME))
    except mysql.connector.Error as err:
        print("Database {} does not exists.".format(DB_NAME))
        if err.errno == errorcode.ER_BAD_DB_ERROR:
            print(err)
            exit(1)
    try:
        print("Creating table {}".format("customeractivity"))
        cursor.execute(TABLES['customeractivity'])
        print("OK - tables created.")
        print("Creating replication user ...")
        cursor.execute(TABLES['debezium'])
        print("Granting required permissions to replicator user...")
        cursor.execute(TABLES['grants'])
        print("Setting up SSL for replicator user...")
        cursor.execute(TABLES['encrypt_connection'])
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
            print("already exists.")
        else:
            print(err.msg)
    finally:    
        cursor.close()
        conn.close()

#Initialize parser
parser = argparse.ArgumentParser()

#Add argument flag
parser.add_argument("-e","--endpoint",help = "DB end point")
parser.add_argument("-u","--user",help = "user name")

#Read arguments from commandline
args = parser.parse_args()

if args.endpoint:
    ep = args.endpoint
if args.user:
    user = args.user

setupTables(ep, user)
