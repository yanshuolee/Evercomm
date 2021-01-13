# import pandas as pd
# import sqlalchemy as sql
# import json

# #IP
# host = "localhost"
# user = "admin"
# pwd = "Admin99"
# #DB name
# db = "dataplatform"

# engine = sql.create_engine(f"mysql+mysqldb://{user}:{pwd}@{host}/{db}?charset=utf8", pool_recycle=3600)
# tb = pd.read_sql_query("SELECT * FROM dataplatform.solarInverter where receivedSync = '2020-09-30 05:14:00' and ieee='00124b000be4e977'", engine)
# vtg = json.loads(tb["dcVoltage"][0])
# print()

import subprocess

# rc = subprocess.call("speedtest > speed.log", shell=True)
rc = subprocess.call("echo admin99 | sudo -S cat /var/log/auth.log", shell=True)

with open("speed.log") as f: 
    logFile = f.read() 

logList = logFile.split("\n")

print(logList[1])
print(logList[6])
print(logList[8])
