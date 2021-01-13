import datetime
import calendar
import os
import sys, traceback
import pathlib
import subprocess
import solarDataProcess

pwd = str(pathlib.Path("__file__").parent.absolute())

# 一定要從5:02開始跑
start = datetime.datetime(2020, 7, 17, 5, 2)
end = datetime.datetime(2020, 7, 17, 19, 0)

while start < end:
    processStart = start - datetime.timedelta(minutes=10)
    if (processStart < datetime.datetime.now().replace(hour=5, minute=2, second=0)) or (processStart > datetime.datetime.now().replace(hour=19, minute=0, second=0)):
        print(f"[{processStart}] Processing will be run btw 05:00 - 19:00")
        start = start + datetime.timedelta(minutes=1)
        continue
    if datetime.datetime.now().replace(second=0, microsecond=0) == start:
        rc = subprocess.call("bash /home/ecoetl/crontab.process.sh", shell=True)
        print("Online...")
        break
    flag_1 = solarDataProcess.main(processStart, debug=False, insert=True)
    start = start + datetime.timedelta(minutes=1)
    if flag_1 != 0:
        print(f"[ERROR] {start}: {flag_1}")