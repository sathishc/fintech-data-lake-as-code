#!/usr/bin/env python3

import random 
import argparse
import logging
import boto3
from botocore.exceptions import ClientError


def generateaccnnos():
    '''Returns a list of account numbers'''
    return random.sample(range(100000,1000000),500000)

def generateData():
    print("Setting up accounts...")
    accnslist = generateaccnnos()
    with open('account_ids.txt', 'w') as f:
        for item in accnslist:
            f.write("%s\n" % item)
    
generateData()
