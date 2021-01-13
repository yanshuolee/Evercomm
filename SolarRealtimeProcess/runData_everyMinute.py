import datetime
import calendar
import os
import sys, traceback
import pathlib
import sqlalchemy as sql
import solarDataProcess
import time

pwd = str(pathlib.Path("__file__").parent.absolute())

try:
    systemStart = time.time()
    # currentTS = datetime.datetime(2020, 6, 18, 11, 3)
    currentTS = datetime.datetime.now() - datetime.timedelta(minutes=10)
    if (currentTS < datetime.datetime.now().replace(hour=5, minute=2, second=0)) or (currentTS > datetime.datetime.now().replace(hour=19, minute=0, second=0)):
        print("Processing will be run btw 05:00 - 19:00")
        sys.exit()

    flag_1 = solarDataProcess.main(currentTS, debug=False, insert=True)

    print(f"System Total Run Time: {time.time() - systemStart} sec.")

    if flag_1 != 0:
        print(f"[ERROR] {currentTS}: {flag_1}")
except:
    traceback.print_exc(file=sys.stdout)
