import pymssql
import time
import sys
import getopt
import datetime 
import urllib.parse 
import string
import random
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider 

# Config Values 
QUERY_INTERVAL_MS = 10

# parse command line args
try:
    opts, remainder = getopt.getopt(sys.argv[1:], 'hi:')
except:
    print('Usage: test.py -i [queryInterval]')
    sys.exit(1)

for opt, arg in opts:
    if (opt == '-h'):
        print('Usage: test.py -i [queryInterval]')
        sys.exit(1)
    elif (opt == '-i' and arg is not None):
        QUERY_INTERVAL_MS = int(arg)

# Setup SQL connection
DBConn = pymssql.connect(server='172.30.100.116', user='octopus', password='octopus', database='DEV1_FE')
DBConn.autocommit(True)
cursor = DBConn.cursor()

# Setup Cassandra connection
authentication = PlainTextAuthProvider(username='devadmin', password='Keys2TheK1ngd0m')
cluster = Cluster(['dev-cassandra.ksg.int'], port=9042, auth_provider=authentication)
session = cluster.connect('CassandraPractice')

def random_string(size=10, chars=string.ascii_lowercase + string.digits + string.ascii_uppercase):
    return ''.join(random.choice(chars) for _ in range(size))

def cassandra_insert(id):
    query = 'INSERT INTO \"CassandraPractice\".\"Zach_SnapLogic_Demo\" (\"id\", \"Int1\", \"String1\", \"updateTime\") VALUES (' + str(id) + ', ' + str(random.randint(0, 1000) * 10) + ', \'' + random_string() + 'CAT\', toTimestamp(now()));'
    session.execute(query)

def sql_insert(id):
    query = 'INSERT INTO DEV1_FE.dbo.Zach_SnapLogic_Demo (id, Int1, String1) VALUES (' + str(id) + ', ' + str(random.randint(0, 1000)) + ', \'' + random_string() + '\');'
    cursor.execute(query)

# Start of execution
print("Starting Execution.")
curId = 0
while(curId < 100000):
    if (random.randint(0, 2) == 0):
        cassandra_insert(curId)
    else:
        sql_insert(curId)
    curId = curId + 1
    time.sleep(0.001 * QUERY_INTERVAL_MS)
