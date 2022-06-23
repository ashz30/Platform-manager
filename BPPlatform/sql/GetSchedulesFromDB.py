import pyodbc

pyodbc.drivers()
# ConDB

SCHEDULE_FILE_LOCATION = 'sql/scripts/ScheduleSQL.txt'


def getConn():
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


def runSQL(cnxn, sql):
   cursor = cnxn.cursor()
   cursor.execute(sql)
   allrows = cursor.fetchall()
   #print (allrows)
   return allrows




#sql = readSQLFile(fileLocation)
#print("SQL:" , sql)

#rows = runSQL(cnxn, sql) #to execute
#for row in rows:
    #print(f'row = {row}')
    #print(f"Schedules Time: {row[0]} on RR :{row[1]}")

