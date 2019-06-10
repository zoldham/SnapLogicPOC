# import libraries
import requests
import pymssql
import time
import sys
import getopt
import datetime
import urllib.parse
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

# Config Values
CONNECTION_RESET_INTERVAL = 600
CHANGE_THRESHOLD = 100
AUTO_UPDATE_INTERVAL = 60
LOG_INTERVAL = 10
LOG_FILE_NAME = None
LOG_FILE = None
FILE_MODE = 'a+'

# Parse command line args
try:
    opts, remainder = getopt.getopt(sys.argv[1:], 'hc:u:l:f:m:')
except:
    print('Usage: syncronizer.py -c [changeThreshold] -u [autoUpdateInterval] -l [logInterval] -f [logFile] -m [fileMode]')
    sys.exit(1)

for opt, arg in opts:
    if (opt == '-h'):
        print('Usage: syncronizer.py -c [changeThreshold] -u [autoUpdateInterval] -l [logInterval] -f [logFile] -m [fileMode]')
        sys.exit(1)
    elif (opt == '-c' and arg is not None):
        CHANGE_THRESHOLD = int(arg)
    elif (opt == '-u' and arg is not None):
        AUTO_UPDATE_INTERVAL = int(arg)
    elif (opt == '-l' and arg is not None):
        LOG_INTERVAL = int(arg)
    elif (opt == '-f' and arg is not None):
        LOG_FILE_NAME = arg
    elif (opt == '-m' and arg is not None):
        FILE_MODE = arg

# setup log
if (LOG_FILE_NAME is not None):
    LOG_FILE = open(LOG_FILE_NAME, FILE_MODE)

# Setup SQL connection
DBConn = pymssql.connect(server='172.30.100.116', user='octopus', password='octopus', database='DEV1_FE')
DBConn.autocommit(True)
cursor = DBConn.cursor()

# Setup Cassandra Connection
authentication = PlainTextAuthProvider(username='devadmin', password='Keys2TheK1ngd0m')
cluster = Cluster(['dev-cassandra.ksg.int'], port=9042, auth_provider=authentication)
session = cluster.connect('CassandraPractice')

def get_current_time_sql():
    cursor.execute('SELECT (sysutcdatetime());')
    row = cursor.fetchone()
    return row[0]

def get_current_time_cassandra():
    row = session.execute("SELECT toTimestamp(now()) FROM system.local;")
    return row[0][0]

def log(line):
    timestamp = str(get_current_time_sql()).split('.')[0]
    if (LOG_FILE is not None):
        LOG_FILE.write(timestamp + ': ' + line + '\n')
    print(line)

def change_count_sql(timestamp):
    query = 'SELECT COUNT(*) FROM DEV1_FE.dbo.Zach_SnapLogic_Demo WHERE updateTime >= CAST(\'' + timestamp.strftime("%Y-%m-%d %H:%M:%S").split('.')[0] + '\' AS datetime2(7));'
    cursor.execute(query)
    row = cursor.fetchone()
    return int(row[0])

def change_count_cassandra(timestamp):
    # print(timestamp.strftime("%Y-%m-%d %H:%M:%S").split('.')[0] + ".000+0000")
    query = "SELECT COUNT(*) FROM \"CassandraPractice\".\"Zach_SnapLogic_Demo\" WHERE \"updateTime\" > \'" + timestamp.strftime("%Y-%m-%d %H:%M:%S").split('.')[0] + "+0000\' ALLOW FILTERING;"
    row = session.execute(query)
    return int(row[0][0])

def do_update(timestampSQL, timestampCassandra):
    # API url
    url = 'https://elastic.snaplogic.com:443/api/1/rest/slsched/feed/WylessTest/projects/Zach%20Oldham/SQL%20Demo%20Task?bearer_token=oYd6JngZ9PQw0YZdzs4IRWDkd7E0Qnir&updateTimestampSQL=' + urllib.parse.quote(timestampSQL.strftime("%Y-%m-%d %H:%M:%S").split('.')[0], safe='') + '&updateTimestampCassandra=' + urllib.parse.quote(timestampCassandra.strftime("%Y-%m-%d %H:%M:%S").split('.')[0], safe='')
    # params
    params = {}

    # send request
    try:
        r = requests.get(url = url, params = params)
    except requests.exceptions.RequestException as e:
        # an error occurred
        log('Sync request returned an error:')
        log(e)
        log('A new sync will be attempted in ' + str(AUTO_UPDATE_INTERVAL) + ' seconds or after ' + str(CHANGE_THRESHOLD) + ' additional changes.')
        return

    # success
    log('Sync request successful.')
    return

# start of execution
if (LOG_FILE is not None):
    print('All logging is being duplicated in ' + LOG_FILE_NAME + '.')

timeSinceUpdate = 0
updateTimestampSQL = get_current_time_sql()
updateTimestampCassandra = get_current_time_cassandra()
numChangesSQL = 0
numChangesCassandra = 0

while(True):

    numChangesSQL = change_count_sql(updateTimestampSQL)
    numChangesCassandra = change_count_cassandra(updateTimestampCassandra)
    # print(numChangesCassandra)

    if ((numChangesSQL + numChangesCassandra) >= CHANGE_THRESHOLD or timeSinceUpdate >= AUTO_UPDATE_INTERVAL):
        # Sync request needed
        # Print logging info
        if ((numChangesSQL + numChangesCassandra) >= CHANGE_THRESHOLD):
            log('Change threshold reached (' + str(numChangesSQL + numChangesCassandra) + ' changes), starting sync.')
        else:
            log('Time threshold reached (' + str(timeSinceUpdate) + ' seconds), starting sync.')
        
        # Issue sync requset
        tempUpdateTimestampSQL = get_current_time_sql()
        tempUpdateTimestampCassandra = get_current_time_cassandra()
        do_update(updateTimestampSQL, updateTimestampCassandra)
        updateTimestampSQL = tempUpdateTimestampSQL
        updateTimestampCassandra = tempUpdateTimestampCassandra
        timeSinceUpdate = 0
    else:
        # Continue to wait
        # Print logging info
        if (timeSinceUpdate % LOG_INTERVAL == 0):
            log(str(timeSinceUpdate) + ' seconds since last sync, ' + str(numChangesSQL + numChangesCassandra) + ' changes so far.')
            log('Waiting for ' + str(AUTO_UPDATE_INTERVAL - timeSinceUpdate) + ' more seconds or for ' + str(CHANGE_THRESHOLD - numChangesSQL - numChangesCassandra) + ' more changes.')
        
        # Sleep
        time.sleep(1)
        timeSinceUpdate = timeSinceUpdate + 1