import sqlalchemy as sql
import pandas as pd
import numpy as np
import copy
import time
import datetime

def update(currentTS, engine, insert=False):
    conn = engine.connect()
    year = currentTS.year
    month_start = f"{(currentTS.month):02}"
    month_end = f"{(currentTS.month)+1:02}"
    if insert:
        
        # 算歷史的
        update_sql = f"\
            replace into reportplatform.monthlySolarPowerGeneration \
            SELECT \
                operationDate,\
                siteId,\
                groupId,\
                inverterId,\
                inverterDescription,\
                round(avg(realPowerGeneration), 3) as realPowerGeneration,\
                round(avg(budgetPowerGeneration), 3) as budgetPowerGeneration,\
                round(avg(referencePowerGeneration), 3) as referencePowerGeneration,\
                round(avg(predictPowerGeneration), 3) as predictPowerGeneration,\
                round(avg(stationPowerGeneration), 3) as predictPowerGeneration,\
                round(avg(realIrradiation), 3) as realIrradiation,\
                round(avg(realPanelTemperature), 3) as realPanelTemperature\
            FROM\
                reportplatform.dailySolarPowerGeneration\
                where operationDate >= '{year}-{month_start}-01' and operationDate < '{year}-{month_end}-01' group by inverterId\
        "
        conn.execute(sql.text(update_sql))

if __name__ == "__main__":
    currentTS = datetime.datetime(2020, 7, 1, 11, 3)
    #IP
    host = "localhost"
    user = "admin"
    pwd = "Admin99"

    #DB name
    dbRPF = "reportplatform"
    reportplatform_engine = sql.create_engine(f'mysql+mysqldb://{user}:{pwd}@{host}/{dbRPF}', pool_recycle=3600*7)
    update(currentTS, reportplatform_engine, insert=True)
    print("Insert successfully.")