import datetime
from datetime import datetime,timezone, timedelta
import pyodbc

pyodbc.drivers()
INSERT_FILE_LOCATION = 'sql/scripts/insertRRProvisioningStatusSQL.txt'
SELECT_FILE_LOCATION = 'sql/scripts/selectRRProvisioningStatusSQL.txt'
UPDATE_FILE_LOCATION = 'sql/scripts/updateRRProvisioningStatusSQL.txt'
SELECT_RR_UP_FILE_LOCATION = 'sql/scripts/selectRRUpProvisioningStatusSQL.txt'
MARK_FILE_LOCATION = 'sql/scripts/markRRStatusSQL.txt'
DELETE_FILE_LOCATION = 'sql/scripts/deleteRRStatusSQL.txt'


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

def selectOneRRFromTable(object):
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

def selectRRUpFromTable():
    cnxn = getConn()
    cursor = cnxn.cursor()
    selectSQLContent = readSQLFile(SELECT_RR_UP_FILE_LOCATION)
    selectSql = selectSQLContent
    print(selectSql)
    cursor.execute(selectSql)
    RRStatusRows = cursor.fetchall()
    if (len(RRStatusRows) == 0):
        return "NO_ROWS"
    else:
        return RRStatusRows


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


def markStatus(vmname, status):
    RR_DB_STATUS = status
    cnxn = getConn()
    cursor = cnxn.cursor()
    currentdate = datetime.now(timezone.utc)
    format = '%Y-%m-%d %H:%M:%S'
    monthbacktime = currentdate + timedelta(days=30)
    updateSQL = readSQLFile(MARK_FILE_LOCATION)
    sql_parameter_data = (f'{RR_DB_STATUS}', f'{str(currentdate.strftime(format))}', f'{vmname}')
    print (updateSQL , sql_parameter_data)
    cursor.execute(updateSQL, sql_parameter_data)
    cnxn.commit()
    return RR_DB_STATUS

def deleteRecord(vmname):
    RR_DB_STATUS = 'DELETED'
    cnxn = getConn()
    cursor = cnxn.cursor()
    deleteSQL = readSQLFile(DELETE_FILE_LOCATION)
    sql_parameter_data = (f'{vmname}')
    print (deleteSQL , sql_parameter_data)
    cursor.execute(deleteSQL, sql_parameter_data)
    cnxn.commit()
    return RR_DB_STATUS