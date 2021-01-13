import sqlalchemy as sql
import pandas as pd
import numpy as np
import copy
import time
import datetime
import math
import json
import argparse
from multiprocessing import Process, Manager
try:
    import initialization
except:
    from . import initialization

def getOldDailySPG(dailySPG):
    dailySPG["realPowerGenerationPerHour"] = dailySPG["realPowerGenerationPerHour"].apply(json.loads)
    dailySPG["budgetPowerGenerationPerHour"] = dailySPG["budgetPowerGenerationPerHour"].apply(json.loads)
    dailySPG["referencePowerGenerationPerHour"] = dailySPG["referencePowerGenerationPerHour"].apply(json.loads)
    dailySPG["predictPowerGenerationPerHour"] = dailySPG["predictPowerGenerationPerHour"].apply(json.loads)
    dailySPG["stationPowerGenerationPerHour"] = dailySPG["stationPowerGenerationPerHour"].apply(json.loads)
    dailySPG["realIrradiationPerHour"] = dailySPG["realIrradiationPerHour"].apply(json.loads)
    dailySPG["realPanelTemperaturePerHour"] = dailySPG["realPanelTemperaturePerHour"].apply(json.loads)
    return dailySPG

def startHour(processStart, processEnd, TSiteInverter, solarInvPowerGeneration):    
    solarInvPowerGeneration = solarInvPowerGeneration[["ts", 
                                                       "siteId", 
                                                       "groupId", 
                                                       "inverterId", 
                                                       "inverterDescription"]]
    solarInvPowerGeneration_merged = TSiteInverter.merge(solarInvPowerGeneration, on=["inverterId"], how="left")[["ts", "siteId_x", "inverterId", "groupId", "inverterDescription"]]
    solarInvPowerGeneration_merged = solarInvPowerGeneration_merged.rename(columns={"siteId_x":"siteId", "ts":"operationDate"})

    rowCount = solarInvPowerGeneration_merged.shape[0]
    solarInvPowerGeneration_merged["operationDate"] = pd.Series([processStart.strftime('%Y-%m-%d')]*rowCount)
    solarInvPowerGeneration_merged["groupId"] = solarInvPowerGeneration_merged["groupId"].fillna(0)
    solarInvPowerGeneration_merged["inverterDescription"] = solarInvPowerGeneration_merged["inverterDescription"].fillna("-")
    jsonObj = initialization.initJsonObj(6, 19)

    dataDict = {"realPowerGeneration" : [0.0]*rowCount,
                "budgetPowerGeneration" : [0.0]*rowCount,
                "referencePowerGeneration" : [0.0]*rowCount,
                "stationPowerGeneration" : [0.0]*rowCount,
                "predictPowerGeneration" : [0.0]*rowCount,
                "realIrradiation" : [0.0]*rowCount,
                "realPanelTemperature" : [0.0]*rowCount,
                "realPowerGenerationPerHour" : [jsonObj["realPowerGeneration"]]*rowCount,
                "budgetPowerGenerationPerHour" : [jsonObj["budgetPowerGeneration"]]*rowCount,
                "referencePowerGenerationPerHour" : [jsonObj["referencePowerGeneration"]]*rowCount,
                "predictPowerGenerationPerHour" : [jsonObj["predictPowerGeneration"]]*rowCount,
                "stationPowerGenerationPerHour" : [jsonObj["stationPowerGeneration"]]*rowCount,
                "realIrradiationPerHour" : [jsonObj["realIrradiation"]]*rowCount,
                "realPanelTemperaturePerHour" : [jsonObj["realPanelTemperature"]]*rowCount}
    tmpTbl = pd.DataFrame(dataDict)

    dailySolarPowerGeneration = pd.concat([solarInvPowerGeneration_merged, tmpTbl], axis=1)
    return dailySolarPowerGeneration
    
def continueHour(processStart, processEnd, TSiteInverter, solarInvPowerGeneration, dailySPG, ncpu):
    TSiteInverter = TSiteInverter[["inverterId"]]
    solarInvPowerGeneration = solarInvPowerGeneration[["inverterId", 
                                                       "realPowerGeneration", 
                                                       "budgetPowerGeneration", 
                                                       "referencePowerGeneration", 
                                                       "stationPowerGeneration", 
                                                       "predictPowerGeneration", 
                                                       "realIrradiation", 
                                                       "realPanelTemperature"]]
    solarInvPowerGeneration = solarInvPowerGeneration.fillna(0)
    solarInvPowerGeneration_merged = TSiteInverter.merge(solarInvPowerGeneration, on=["inverterId"], how="left")
    dailySPG = getOldDailySPG(dailySPG)
    prev_count = processEnd.hour - 5
    current_count = prev_count + 1

    solarInvPowerGeneration_merged[["realPowerGeneration", "budgetPowerGeneration", "referencePowerGeneration", "stationPowerGeneration", "predictPowerGeneration", "realIrradiation"]] = \
    solarInvPowerGeneration_merged[["realPowerGeneration", "budgetPowerGeneration", "referencePowerGeneration", "stationPowerGeneration", "predictPowerGeneration", "realIrradiation"]].apply(lambda x: x/30)
    solarInvPowerGeneration_merged[["realPanelTemperature"]] = solarInvPowerGeneration_merged[["realPanelTemperature"]].apply(lambda x: x/current_count)

    processTbl = dailySPG.merge(solarInvPowerGeneration_merged, on=["inverterId"], suffixes=["_org", "_data"])

    processTbl = pd.eval("realPowerGeneration = processTbl.realPowerGeneration_org + processTbl.realPowerGeneration_data", target=processTbl)
    processTbl = pd.eval("budgetPowerGeneration = processTbl.budgetPowerGeneration_org + processTbl.budgetPowerGeneration_data", target=processTbl)
    processTbl = pd.eval("referencePowerGeneration = processTbl.referencePowerGeneration_org + processTbl.referencePowerGeneration_data", target=processTbl)
    processTbl = pd.eval("stationPowerGeneration = processTbl.stationPowerGeneration_org + processTbl.stationPowerGeneration_data", target=processTbl)
    processTbl = pd.eval("predictPowerGeneration = processTbl.predictPowerGeneration_org + processTbl.predictPowerGeneration_data", target=processTbl)
    processTbl = pd.eval("realIrradiation = processTbl.realIrradiation_org + processTbl.realIrradiation_data", target=processTbl)
    processTbl = pd.eval("realPanelTemperature = ((processTbl.realPanelTemperature_org * prev_count)/current_count) + processTbl.realPanelTemperature_data", target=processTbl)
    processTbl = processTbl.round(3)
    dt = (processEnd + datetime.timedelta(hours=1)).strftime("%H")+"H"

    def F(tbl, dt, columns):
        insertString = ""
        for ind, row in tbl.iterrows():
            tempString = ""
            row["realPowerGenerationPerHour"][dt]["data"] += round(row["realPowerGeneration_data"], 3)
            row["realPowerGenerationPerHour"] = json.dumps(row["realPowerGenerationPerHour"])
            row["budgetPowerGenerationPerHour"][dt]["data"] += round(row["budgetPowerGeneration_data"], 3)
            row["budgetPowerGenerationPerHour"] = json.dumps(row["budgetPowerGenerationPerHour"])
            row["referencePowerGenerationPerHour"][dt]["data"] += round(row["referencePowerGeneration_data"], 3)
            row["referencePowerGenerationPerHour"] = json.dumps(row["referencePowerGenerationPerHour"])
            row["stationPowerGenerationPerHour"][dt]["data"] += round(row["stationPowerGeneration_data"], 3)
            row["stationPowerGenerationPerHour"] = json.dumps(row["stationPowerGenerationPerHour"])
            row["predictPowerGenerationPerHour"][dt]["data"] += round(row["predictPowerGeneration_data"], 3)
            row["predictPowerGenerationPerHour"] = json.dumps(row["predictPowerGenerationPerHour"])
            row["realIrradiationPerHour"][dt]["data"] += round(row["realIrradiation_data"], 3)
            row["realIrradiationPerHour"] = json.dumps(row["realIrradiationPerHour"])
            row["realPanelTemperaturePerHour"][dt]["data"] += round(row["realPanelTemperature_data"], 3)
            row["realPanelTemperaturePerHour"] = json.dumps(row["realPanelTemperaturePerHour"])

            tempString = f"'{row['operationDate'].strftime('%Y-%m-%d')}', \
                           '{row['siteId']}', '{row['groupId']}', \
                           '{row['inverterId']}', '{row['inverterDescription']}',\
                           '{row['realPowerGeneration']}', '{row['realPowerGenerationPerHour']}', \
                           '{row['budgetPowerGeneration']}', '{row['budgetPowerGenerationPerHour']}', \
                           '{row['referencePowerGeneration']}', '{row['referencePowerGenerationPerHour']}', \
                           '{row['predictPowerGeneration']}', '{row['predictPowerGenerationPerHour']}', \
                           '{row['stationPowerGeneration']}', '{row['stationPowerGenerationPerHour']}', \
                           '{row['realIrradiation']}', '{row['realIrradiationPerHour']}', \
                           '{row['realPanelTemperature']}', '{row['realPanelTemperaturePerHour']}' "
            insertString += f"({tempString}) , "

        insertString = insertString[:-2]

        return_df.append(insertString)

    if ncpu <= 0:
        ncpu = 1
    step = math.ceil(processTbl.shape[0] / ncpu)
    processTblSplit = [processTbl.iloc[i:i+step] for i in range(0, processTbl.shape[0], step)]

    manager = Manager()
    return_df = manager.list()
    # F(processTbl.iloc[:3], dt, dailySPG.columns)
    processes = [Process(target=F, args=(processChild, dt, dailySPG.columns)) for processChild in processTblSplit]
    pStart = [p.start() for p in processes]
    pJoin = [p.join() for p in processes]

    return ",".join(return_df)

def update(processStart, processEnd, processplatform_engine, uiplatform_engine, reportplatform_engine, ncpu, insert=False):
    conn = processplatform_engine.connect()
    processStart_str = processStart.strftime('%Y-%m-%d %H:%M')
    processEnd_str = processEnd.strftime('%Y-%m-%d %H:%M')
    insertKey = ""
    
    stratDSPG = time.time()
    # Table
    TSiteInverter_sql = f"SELECT siteId, inverterId FROM uiplatform.TSiteInverter where siteId in (SELECT id FROM uiplatform.TSite where deleteFlag=0)"
    TSiteInverter = pd.read_sql(TSiteInverter_sql, con=uiplatform_engine)
    solarInvPowerGeneration_sql = f"SELECT * FROM processplatform.solarInvPowerGeneration where ts > '{processStart_str}' and ts < '{processEnd_str}' "
    solarInvPowerGeneration = pd.read_sql(sql.text(solarInvPowerGeneration_sql), con=processplatform_engine)
    dailySPG_sql = f"SELECT * FROM processplatform.dailySolarPowerGeneration where operationDate = '{processStart_str[:10]}';"
    dailySPG = pd.read_sql(sql.text(dailySPG_sql), con=reportplatform_engine)
    
    if dailySPG.shape[0] == 0:
        dailySolarPowerGeneration = startHour(processStart, processEnd, TSiteInverter, solarInvPowerGeneration)
        dailySolarPowerGeneration.to_sql("dailySolarPowerGeneration", con=processplatform_engine, if_exists='append', index=False)
        print("DailySolarPowerGeneration insert successfully.")
        print(f"DailySolarPowerGeneration executed in {time.time() - stratDSPG}.")
        return 0
    else:
        insertString = continueHour(processStart, processEnd, TSiteInverter, solarInvPowerGeneration, dailySPG, ncpu)
    
    for name in dailySPG.columns:
        insertKey += "`" + name + "`, "
    insertKey = insertKey[:-2]

    if insert:
        if insertString == "": return -1
        sqlstr = f"replace into processplatform.dailySolarPowerGeneration ({insertKey}) values {insertString}"
        conn.execute(sql.text(sqlstr))
        print("DailySolarPowerGeneration insert successfully.")

    print(f"DailySolarPowerGeneration executed in {time.time() - stratDSPG}.")

    return 0

if __name__ == "__main__":
    #IP
    host = "localhost"
    user = "admin"
    pwd = "Admin99"

    #DB name
    dbUi = "uiplatform"
    dbRPF = "reportplatform"
    dbProcessPF = "processplatform"
    
    currentTS = datetime.datetime(2020, 11, 3, 8, 43)
    processplatform_engine = sql.create_engine(f"mysql+mysqldb://{user}:{pwd}@{host}/{dbProcessPF}", pool_recycle=3600*7)
    uiplatform_engine = sql.create_engine(f"mysql+mysqldb://{user}:{pwd}@{host}/{dbUi}", pool_recycle=3600*7)
    reportplatform_engine = sql.create_engine(f"mysql+mysqldb://{user}:{pwd}@{host}/{dbRPF}", pool_recycle=3600*7)
    s = time.time()
    update(currentTS-datetime.timedelta(minutes=2), currentTS, processplatform_engine, uiplatform_engine, reportplatform_engine, ncpu=4, insert=True)
    print(f"Time elapsed {time.time()-s} sec.")
