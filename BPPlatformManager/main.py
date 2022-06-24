# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
import datetime
from datetime import datetime,timezone, timedelta

from sql import GetSchedulesFromDB, RunSQLOnRRStatusTable
from VM import provisionVM, deleteVM
import threading
from VM import checkRRStatus

# Press the green button in the gutter to run the script.


GAP_BEFORE_RUN = 5 #minutes
RR_PORT = "8181"
ScheduledRRsGlobalVar = []

def provisionVmThread(scheduleRow):
    print(f"{scheduleRow[1]} not scheduled. Scheduling...")
    db_status = RunSQLOnRRStatusTable.insertInTable(scheduleRow)
    print(f"DB Status {scheduleRow[1]}:", db_status)
    vm_name, ip_address = provisionVM.createVM(scheduleRow[1])
    print(f"{scheduleRow[1]} {db_status} complete")
    db_status = RunSQLOnRRStatusTable.updateInTable(scheduleRow, ip_address, 'PROVISIONED')
    print(f"DB Status {scheduleRow[1]}:", db_status)


def calendarBasedProvisioning():
    # scheduling as per calendar
    print("Checking calendar")
    dBScheduleSQL = GetSchedulesFromDB.readSQLFile(GetSchedulesFromDB.SCHEDULE_FILE_LOCATION)
    scheduleDBConn = GetSchedulesFromDB.getConn()
    scheduleRows = GetSchedulesFromDB.runSQL(scheduleDBConn, dBScheduleSQL)  # to execute
    global ScheduledRRsGlobalVar

    for scheduleRow in scheduleRows:
        ScheduledRRsGlobalVar.append(scheduleRow[1])
        db_status = None
        print(f"Scheduled Time: {scheduleRow[0]} on RR :{scheduleRow[1]}")
        # check status
        RR_db_status = RunSQLOnRRStatusTable.selectOneRRFromTable(scheduleRow)
        if (RR_db_status == "NO_ROWS"):
            print(f"No records found for {scheduleRow[1]}. Starting intelligent Provisioning thread..")
            vmThread = threading.Thread(target=provisionVmThread, name=scheduleRow[1] + 'Thread', args=(scheduleRow,))
            vmThread.start()
            # provisionVmThread(scheduleRow)

        elif (RR_db_status[2] == 'PROVISIONING'):
            db_status = RR_db_status[2]
            print(f"DB Status {scheduleRow[1]}:", db_status)
            # check how long till next run
            currentdate = datetime.now(timezone.utc)
            schedule_run_time = RR_db_status[1].replace(tzinfo=timezone.utc)
            print("Current Date time :", currentdate)
            print("Schedule Date time :", schedule_run_time)
            if (currentdate + timedelta(minutes=GAP_BEFORE_RUN) > schedule_run_time):
                print(f"{scheduleRow[1]} is behind calling Human.. we can restart though..")
                db_status = RunSQLOnRRStatusTable.updateInTable(scheduleRow, RR_db_status[3], 'HUMAN_NEEDED')

        elif (RR_db_status[2] == 'PROVISIONED'):
            db_status = RR_db_status[2]
            print(f"DB Status {scheduleRow[1]}:", db_status)
            RR_runtime_Status = checkRRStatus.getRRStatus(RR_db_status[3], RR_PORT)
            print("RR Runtime Status for ", scheduleRow[1], " is :", RR_runtime_Status)
            if (RR_runtime_Status == 'RR_DOWN'):
                # check how long timm next run
                currentdate = datetime.now(timezone.utc)
                schedule_run_time = RR_db_status[1].replace(tzinfo=timezone.utc)
                print("Current Date time :", currentdate)
                print("Schedule Date time :", schedule_run_time)
                if (currentdate + timedelta(minutes=GAP_BEFORE_RUN) > schedule_run_time):
                    print(f"{scheduleRow[1]} is behind calling Human..")
                    db_status = RunSQLOnRRStatusTable.updateInTable(scheduleRow, RR_db_status[3], 'HUMAN_NEEDED')
            elif (RR_runtime_Status.__contains__('RR_UP')):
                db_status = RunSQLOnRRStatusTable.updateInTable(scheduleRow, RR_db_status[3], RR_runtime_Status)
                print(f"DB Status {scheduleRow[1]}:", db_status)

        elif (RR_db_status[2].__contains__('RR_UP')):
            db_status = RR_db_status[2]
            print(f"{scheduleRow[1]} DB Status :", db_status)

        elif (RR_db_status[2] == 'HUMAN_NEEDED'):
            db_status = RR_db_status[2]
            print(f"{scheduleRow[1]} DB Status :", db_status)
            print(f"{scheduleRow[1]} is behind calling Human..")


def calendarBasedDeprovisioning():
    print("Deleting resources not scheduled for work")
    RR_Up_db_rows = RunSQLOnRRStatusTable.selectRRUpFromTable()
    if(RR_Up_db_rows == "NO_ROWS"):
        print("No RR's UP without work in Database, Status check over")
    else:
        for RR_Up_db_row in RR_Up_db_rows:
            RR_runtime_Status = checkRRStatus.getRRStatus(RR_Up_db_row[3], RR_PORT)
            if (RR_runtime_Status == 'RR_UP_NOT_BUSY'):
                print(f"{RR_Up_db_row[0]} is up and not busy at runtime")
                db_status = RunSQLOnRRStatusTable.markStatus(RR_Up_db_row[0], RR_runtime_Status)
                print("Upcoming schedules :", ScheduledRRsGlobalVar)
                if(RR_Up_db_row[0] in ScheduledRRsGlobalVar):
                    print(f"{RR_Up_db_row[0]} present in upcoming schedule, not deleting")
                else:
                    print(f"{RR_Up_db_row[0]} does not have any upcoming schedule - Deleting resources")
                    db_status = RunSQLOnRRStatusTable.markStatus(RR_Up_db_row[0], 'DELETING')
                    deleteVM.deleteVM(RR_Up_db_row[0])
                    print(f"deletion of {RR_Up_db_row[0]} complete")
                    db_status = RunSQLOnRRStatusTable.deleteRecord(RR_Up_db_row[0])
                    print(f"DB Status {RR_Up_db_row[0]}:", db_status)

            elif (RR_runtime_Status == 'RR_UP_BUSY'):
                print(f"{RR_Up_db_row[0]} is up at runtime and busy")
                db_status = RunSQLOnRRStatusTable.markStatus(RR_Up_db_row[0], RR_runtime_Status)




if __name__ == '__main__':
    calendarBasedProvisioning()
    calendarBasedDeprovisioning()



