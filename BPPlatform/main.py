# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
import datetime
from datetime import datetime,timezone, timedelta

from sql import GetSchedulesFromDB, RunSQLOnRRStatusTable
from VM import provisionVM
import threading
from VM import checkRRStatus

# Press the green button in the gutter to run the script.


GAP_BEFORE_RUN = 30 #minutes

def provisionVmThread(scheduleRow):
    print(f"{scheduleRow[1]} not scheduled. Scheduling...")
    db_status = RunSQLOnRRStatusTable.insertInTable(scheduleRow)
    print(f"DB Status {scheduleRow[1]}:", db_status)
    vm_name, ip_address = provisionVM.createVM(scheduleRow[1])
    print(f"{scheduleRow[1]} {db_status} complete")
    db_status = RunSQLOnRRStatusTable.updateInTable(scheduleRow, ip_address, 'PROVISIONED')
    print(f"DB Status {scheduleRow[1]}:", db_status)


if __name__ == '__main__':
    dBScheduleSQL = GetSchedulesFromDB.readSQLFile(GetSchedulesFromDB.SCHEDULE_FILE_LOCATION)
    scheduleDBConn = GetSchedulesFromDB.getConn()
    scheduleRows = GetSchedulesFromDB.runSQL(scheduleDBConn, dBScheduleSQL) #to execute

    for scheduleRow in scheduleRows:
        db_status = None
        print(f"Scheduled Time: {scheduleRow[0]} on RR :{scheduleRow[1]}")
        #check status
        RR_db_status = RunSQLOnRRStatusTable.selectFromTable(scheduleRow)
        if (RR_db_status == "NO_ROWS"):
            print(f"No records found for {scheduleRow[1]}. Starting intelligent Provisioning thread..")
            vmThread = threading.Thread(target=provisionVmThread, name=scheduleRow[1]+'Thread', args=(scheduleRow,))
            vmThread.start()
            #provisionVmThread(scheduleRow)

        elif(RR_db_status[2] == 'PROVISIONING'):
            db_status = RR_db_status[2]
            print(f"DB Status {scheduleRow[1]}:", db_status)
            # check how long till next run
            currentdate = datetime.now(timezone.utc)
            schedule_run_time = RR_db_status[1].replace(tzinfo=timezone.utc)
            print("Current Date time :", currentdate)
            print("Schedule Date time :", schedule_run_time)
            if (currentdate + timedelta(minutes=GAP_BEFORE_RUN) > schedule_run_time):
                print(f"{scheduleRow[1]} is behind calling Human..")
                db_status = RunSQLOnRRStatusTable.updateInTable(scheduleRow, RR_db_status[3], 'HUMAN_NEEDED')

        elif(RR_db_status[2] == 'PROVISIONED'):
            db_status = RR_db_status[2]
            print(f"DB Status {scheduleRow[1]}:", db_status)
            RR_runtime_Status = checkRRStatus.getRRStatus(RR_db_status[3], "8181")
            print("RR Runtime Status for ",scheduleRow[1] , " is :" , RR_runtime_Status)
            if (RR_runtime_Status == 'RR_DOWN'):
                #check how long timm next run
                currentdate = datetime.now(timezone.utc)
                schedule_run_time = RR_db_status[1].replace(tzinfo=timezone.utc)
                print("Current Date time :" , currentdate)
                print("Schedule Date time :" , schedule_run_time)
                if (currentdate + timedelta(minutes = GAP_BEFORE_RUN) > schedule_run_time):
                    print(f"{scheduleRow[1]} is behind calling Human..")
                    db_status = RunSQLOnRRStatusTable.updateInTable(scheduleRow,RR_db_status[3], 'HUMAN_NEEDED')
            elif (RR_runtime_Status == 'RR_UP'):
                db_status = RunSQLOnRRStatusTable.updateInTable(scheduleRow,RR_db_status[3], 'RR_UP')
                print(f"DB Status {scheduleRow[1]}:", db_status)

        elif(RR_db_status[2] == 'RR_UP'):
            db_status = RR_db_status[2]
            print(f"{scheduleRow[1]} DB Status :", db_status)

        elif(RR_db_status[2] == 'HUMAN_NEEDED'):
            db_status = RR_db_status[2]
            print(f"{scheduleRow[1]} DB Status :", db_status)
            print(f"{scheduleRow[1]} is behind calling Human..")


