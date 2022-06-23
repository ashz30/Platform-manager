import datetime
from datetime import datetime,timezone
import pyodbc

pyodbc.drivers()
INSERT_FILE_LOCATION = 'sql/scripts/insertRRProvisioningStatusSQL.txt'
SELECT_FILE_LOCATION = 'sql/scripts/selectRRProvisioningStatusSQL.txt'
UPDATE_FILE_LOCATION = 'sql/scripts/updateRRProvisioningStatusSQL.txt'


def getConn():
    # ConDB
    password = 'admin123$$'
    server = 'tcp:ashishsqlserver1.database.windows.net,1433'
    database = 'blueprism1'
    username = 'saadmin'

    cnxn = pyodbc.connect(
        'DRIVER={SQL Server Native Client 11.0};SERVER=' + server + ';DATABASE=' + database + ';UID=' + username + ';PWD=' + password)
    return cnxn



def readSQLFile(fileLocation):

    with open(fileLocation) as f:
        contents = f.read()
        return contents

def selectFromTable(object):
    cnxn = getConn()
    cursor = cnxn.cursor()
    selectSQLContent = readSQLFile(SELECT_FILE_LOCATION)
    selectSql = selectSQLContent + "'" + str(object[1]) + "'"
    print(selectSql)
    cursor.execute(selectSql)
    oneRRStatusRow = cursor.fetchall()
    if (len(oneRRStatusRow) == 0):
        return "NO_ROWS"
    else:
        return oneRRStatusRow[0]


def insertInTable(object):
    RR_DB_STATUS = 'PROVISIONING'
    cnxn = getConn()
    cursor = cnxn.cursor()
    currentdate = datetime.now(timezone.utc)
    format = '%Y-%m-%d %H:%M:%S'
    insertSQL = readSQLFile(INSERT_FILE_LOCATION) + " ('" + str(object[1]) + "','" + str(object[0].strftime(format)) + "','" + RR_DB_STATUS + "','" + str(currentdate.strftime(format)) + "')"
    print (insertSQL)
    cursor.execute(insertSQL)
    cnxn.commit()
    return RR_DB_STATUS

def updateInTable(object, ip_address, status):
    RR_DB_STATUS = status
    cnxn = getConn()
    cursor = cnxn.cursor()
    currentdate = datetime.now(timezone.utc)
    format = '%Y-%m-%d %H:%M:%S'
    updateSQL = readSQLFile(UPDATE_FILE_LOCATION)
    sql_parameter_data = (f'{str(object[0].strftime(format))}', f'{status}',f'{ip_address}', f'{str(currentdate.strftime(format))}', f'{str(object[1])}')
    print (updateSQL , sql_parameter_data)
    cursor.execute(updateSQL, sql_parameter_data)
    cnxn.commit()
    return RR_DB_STATUS