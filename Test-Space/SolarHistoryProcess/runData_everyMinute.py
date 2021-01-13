import datetime
import calendar
import os
import sys, traceback
import pathlib
import sqlalchemy as sql
import solarDataProcess

pwd = str(pathlib.Path("__file__").parent.absolute())

with open(f"{pwd}/SolarRealtimeProcess/flag.txt") as f: 
    flag = f.read() 

if flag != '0':
    sys.exit()
else:
    with open(f"{pwd}/SolarRealtimeProcess/flag.txt", "w") as f: 
        f.write("1") 

try:
    # currentTS = datetime.datetime(2020, 6, 18, 11, 3)
    currentTS = datetime.datetime.now() - datetime.timedelta(minutes=10)
    if (currentTS < datetime.datetime.now().replace(hour=5, minute=2, second=0)) or (currentTS > datetime.datetime.now().replace(hour=19, minute=0, second=0)):
        print("Processing will be run btw 05:00 - 19:00")
        sys.exit()

    flag_1 = solarDataProcess.main(currentTS, debug=False, insert=True)

    if flag_1 != 0:
        print(f"[ERROR] {currentTS}: {flag_1}")

except SystemExit:
    with open(f"{pwd}/SolarRealtimeProcess/flag.txt", "w") as f: 
        f.write("0")
except:
    with open(f"{pwd}/SolarRealtimeProcess/error_everyMinute.log", "a") as f: 
        traceback.print_exc(file=sys.stdout)
finally:
    with open(f"{pwd}/SolarRealtimeProcess/flag.txt", "w") as f: 
        f.write("0")
