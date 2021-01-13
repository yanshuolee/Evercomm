import sqlalchemy as sql
import pandas as pd
import numpy as np
import copy
import time
import datetime
import calendar
import math
from multiprocessing import Process, Manager

def update(currentTS, engine, ncpu, insert=False):
    s = time.time()
    conn = engine.connect()
    year = currentTS.year
    day = currentTS.day
    month_start = f"{(currentTS.month):02}"
    lastDay = calendar.monthrange(currentTS.year, currentTS.month)[1]
    before = f"\
        SELECT \
            DATE_FORMAT(operationDate, '%Y-%m-01') as operationDate,\
            siteId,\
            groupId,\
            inverterId,\
            inverterDescription,\
            round(sum(realPowerGeneration), 3) as realPowerGeneration,\
            round(sum(budgetPowerGeneration), 3) as budgetPowerGeneration,\
            round(sum(referencePowerGeneration), 3) as referencePowerGeneration,\
            round(sum(predictPowerGeneration), 3) as predictPowerGeneration,\
            round(sum(stationPowerGeneration), 3) as stationPowerGeneration,\
            round(sum(realIrradiation), 3) as realIrradiation,\
            round(avg(realPanelTemperature), 3) as realPanelTemperature\
        FROM\
            (SELECT * FROM reportplatform.dailySolarPowerGeneration where operationDate < CURRENT_DATE()) as a\
            where operationDate >= '{year}-{month_start}-01' and operationDate <= '{year}-{month_start}-{lastDay}' group by inverterId;\
    "
    current = f"\
        SELECT \
            DATE_FORMAT(operationDate, '%Y-%m-01') as operationDate,\
            siteId,\
            groupId,\
            inverterId,\
            inverterDescription,\
            round(sum(realPowerGeneration), 3) as realPowerGeneration,\
            round(sum(budgetPowerGeneration), 3) as budgetPowerGeneration,\
            round(sum(referencePowerGeneration), 3) as referencePowerGeneration,\
            round(sum(predictPowerGeneration), 3) as predictPowerGeneration,\
            round(sum(stationPowerGeneration), 3) as stationPowerGeneration,\
            round(sum(realIrradiation), 3) as realIrradiation,\
            round(avg(realPanelTemperature), 3) as realPanelTemperature\
        FROM\
            (SELECT * FROM processplatform.dailySolarPowerGeneration) as a\
            where operationDate >= '{year}-{month_start}-01' and operationDate <= '{year}-{month_start}-{lastDay}' group by inverterId;\
    "

    beforeTbl = pd.read_sql(sql.text(before), con=engine)
    crtTbl = pd.read_sql(sql.text(current), con=engine)
    tbl = beforeTbl.merge(crtTbl, on=["operationDate", "siteId", "groupId", "inverterId", "inverterDescription"], how="outer")
    tbl = tbl.fillna(0)

    tbl = pd.eval("realPowerGeneration = tbl.realPowerGeneration_x + tbl.realPowerGeneration_y", target=tbl)
    tbl = pd.eval("budgetPowerGeneration = tbl.budgetPowerGeneration_x + tbl.budgetPowerGeneration_y", target=tbl)
    tbl = pd.eval("referencePowerGeneration = tbl.referencePowerGeneration_x + tbl.referencePowerGeneration_y", target=tbl)
    tbl = pd.eval("predictPowerGeneration = tbl.predictPowerGeneration_x + tbl.predictPowerGeneration_y", target=tbl)
    tbl = pd.eval("stationPowerGeneration = tbl.stationPowerGeneration_x + tbl.stationPowerGeneration_y", target=tbl)
    tbl = pd.eval("realIrradiation = tbl.realIrradiation_x + tbl.realIrradiation_y", target=tbl)
    tbl = pd.eval("realPanelTemperature = ((tbl.realPanelTemperature_x*(day-1)) + tbl.realPanelTemperature_y)/day", target=tbl)
    
    # start here
    def F(tbl):
        insertString = ""
        for ind, row in tbl.iterrows():
            tempString = ""
            tempString = f"'{row['operationDate']}', \
                           '{row['siteId']}', '{row['groupId']}', \
                           '{row['inverterId']}', '{row['inverterDescription']}',\
                           '{row['realPowerGeneration']}', \
                           '{row['budgetPowerGeneration']}',\
                           '{row['referencePowerGeneration']}', \
                           '{row['predictPowerGeneration']}', \
                           '{row['stationPowerGeneration']}', \
                           '{row['realIrradiation']}',\
                           '{row['realPanelTemperature']}'"
            insertString += f"({tempString}) , "
        insertString = insertString[:-2]
        return_df.append(insertString)
    
    if ncpu <= 0:
        ncpu = 1
    step = math.ceil(tbl.shape[0] / ncpu)
    tblSplit = [tbl.iloc[i:i+step] for i in range(0, tbl.shape[0], step)]

    manager = Manager()
    return_df = manager.list()
    # F(tbl.iloc[:3])
    processes = [Process(target=F, args=(processChild,)) for processChild in tblSplit]
    pStart = [p.start() for p in processes]
    pJoin = [p.join() for p in processes]

    values = ",".join(return_df)
    update_sql = f"\
            replace INTO `reportplatform`.`monthlySolarPowerGeneration`\
            (`operationDate`,\
            `siteId`,\
            `groupId`,\
            `inverterId`,\
            `inverterDescription`,\
            `realPowerGeneration`,\
            `budgetPowerGeneration`,\
            `referencePowerGeneration`,\
            `predictPowerGeneration`,\
            `stationPowerGeneration`,\
            `realIrradiation`,\
            `realPanelTemperature`)\
            VALUES {values}\
    "

    if insert:
        conn.execute(sql.text(update_sql))
        print(f"monthlySolarPowerGeneration insert successfully in {time.time()-s}.")

if __name__ == "__main__":
    currentTS = datetime.datetime(2020, 11, 4, 11, 3)
    #IP
    host = "localhost"
    user = "admin"
    pwd = "Admin99"

    #DB name
    dbRPF = "reportplatform"
    reportplatform_engine = sql.create_engine(f'mysql+mysqldb://{user}:{pwd}@{host}/{dbRPF}', pool_recycle=3600*7)
    update(currentTS, reportplatform_engine, ncpu=4, insert=True)