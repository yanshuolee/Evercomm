import sqlalchemy as sql
import pandas as pd
import numpy as np
import copy
import time
import datetime
import math
import json
import argparse
# import initialization
from . import initialization

def getOldDailySPG(dailySPG, InvID):
    dailySPG_dict = dailySPG[dailySPG["inverterId"]==InvID].to_dict('records')[0]
    dailySPG_dict["realPowerGenerationPerHour"] = json.loads(dailySPG_dict["realPowerGenerationPerHour"])
    dailySPG_dict["budgetPowerGenerationPerHour"] = json.loads(dailySPG_dict["budgetPowerGenerationPerHour"])
    dailySPG_dict["referencePowerGenerationPerHour"] = json.loads(dailySPG_dict["referencePowerGenerationPerHour"])
    dailySPG_dict["predictPowerGenerationPerHour"] = json.loads(dailySPG_dict["predictPowerGenerationPerHour"])
    dailySPG_dict["stationPowerGenerationPerHour"] = json.loads(dailySPG_dict["stationPowerGenerationPerHour"])
    dailySPG_dict["realIrradiationPerHour"] = json.loads(dailySPG_dict["realIrradiationPerHour"])
    dailySPG_dict["realPanelTemperaturePerHour"] = json.loads(dailySPG_dict["realPanelTemperaturePerHour"])
    return dailySPG_dict

def startHour(processStart, processEnd, TSiteInverter, solarInvPowerGeneration):    
    insertString = ""
    for ind, row in TSiteInverter.iterrows():
        tempString = ""
        InvID = row["inverterId"]
        sId = row["siteId"]
        print(f"Initializing Inv {InvID}.")
        
        jsonObj = initialization.initJsonObj(6, 19)
        realPowerGeneration = 0.0
        budgetPowerGeneration = 0.0
        referencePowerGeneration = 0.0
        stationPowerGeneration = 0.0
        predictPowerGeneration = 0.0
        realIrradiation = 0.0
        realPanelTemperature = 0.0

        groupId = 0
        deviceDesc = "-"

        realPowerGenerationJson = json.dumps(jsonObj["realPowerGeneration"])
        budgetPowerGenerationJson = json.dumps(jsonObj["budgetPowerGeneration"])
        referencePowerGenerationJson = json.dumps(jsonObj["referencePowerGeneration"])
        predictPowerGenerationJson = json.dumps(jsonObj["predictPowerGeneration"])
        stationPowerGenerationJson = json.dumps(jsonObj["stationPowerGeneration"])
        realIrradiationJson = json.dumps(jsonObj["realIrradiation"])
        realPanelTemperatureJson = json.dumps(jsonObj["realPanelTemperature"])

        tempString = f"'{processStart.strftime('%Y-%m-%d')}', '{sId}', '{groupId}', '{InvID}', '{deviceDesc}',\
                        '{realPowerGeneration}', '{realPowerGenerationJson}', \
                        '{budgetPowerGeneration}', '{budgetPowerGenerationJson}', \
                        '{referencePowerGeneration}', '{referencePowerGenerationJson}', \
                        '{predictPowerGeneration}', '{predictPowerGenerationJson}', \
                        '{stationPowerGeneration}', '{stationPowerGenerationJson}', \
                        '{realIrradiation}', '{realIrradiationJson}', \
                        '{realPanelTemperature}', '{realPanelTemperatureJson}' "
        insertString += f"({tempString}) , "
    insertString = insertString[:-2]
    return insertString
    
def continueHour(processStart, processEnd, TSiteInverter, solarInvPowerGeneration, dailySPG):
    insertString = ""
    for ind, row in TSiteInverter.iterrows():
        tempString = ""
        InvID = row["inverterId"]
        sId = row["siteId"]
        print(f"Processing Inv {InvID}.")

        data = solarInvPowerGeneration[solarInvPowerGeneration["inverterId"]==InvID]
        if data.shape[0] == 0:
            print(f"Interter {InvID} has no data btw {processStart} - {processEnd}. Ignoring {InvID}.")
            continue
            # Do Nothing
        else:
            dailySolarPowerGeneration_old = getOldDailySPG(dailySPG, InvID)

            realPowerGeneration = round(data["realPowerGeneration"].values[0]/30, 3)
            if pd.isna(realPowerGeneration):
                realPowerGeneration = 0
            dailySolarPowerGeneration_old["realPowerGeneration"] += realPowerGeneration
            dailySolarPowerGeneration_old["realPowerGenerationPerHour"][(processEnd + datetime.timedelta(hours=1)).strftime("%H")+"H"]["data"] += realPowerGeneration
            
            budgetPowerGeneration = round(data["budgetPowerGeneration"].values[0]/30, 3)
            dailySolarPowerGeneration_old["budgetPowerGeneration"] += budgetPowerGeneration
            dailySolarPowerGeneration_old["budgetPowerGenerationPerHour"][(processEnd + datetime.timedelta(hours=1)).strftime("%H")+"H"]["data"] += budgetPowerGeneration
            
            referencePowerGeneration = round(data["referencePowerGeneration"].values[0]/30, 3)
            dailySolarPowerGeneration_old["referencePowerGeneration"] += referencePowerGeneration
            dailySolarPowerGeneration_old["referencePowerGenerationPerHour"][(processEnd + datetime.timedelta(hours=1)).strftime("%H")+"H"]["data"] += referencePowerGeneration
            
            stationPowerGeneration = round(data["stationPowerGeneration"].values[0]/30, 3)
            dailySolarPowerGeneration_old["stationPowerGeneration"] += stationPowerGeneration
            dailySolarPowerGeneration_old["stationPowerGenerationPerHour"][(processEnd + datetime.timedelta(hours=1)).strftime("%H")+"H"]["data"] += stationPowerGeneration
            
            predictPowerGeneration = round(data["predictPowerGeneration"].values[0]/30, 3)
            dailySolarPowerGeneration_old["predictPowerGeneration"] += predictPowerGeneration
            dailySolarPowerGeneration_old["predictPowerGenerationPerHour"][(processEnd + datetime.timedelta(hours=1)).strftime("%H")+"H"]["data"] += predictPowerGeneration
            
            realIrradiation = round(data["realIrradiation"].values[0]/30, 3)
            if pd.isna(realIrradiation):
                realIrradiation = 0
            dailySolarPowerGeneration_old["realIrradiation"] += realIrradiation
            dailySolarPowerGeneration_old["realIrradiationPerHour"][(processEnd + datetime.timedelta(hours=1)).strftime("%H")+"H"]["data"] += realIrradiation

            if not pd.isna(data["realPanelTemperature"].values[0]):
                realPanelTemperature = round(data["realPanelTemperature"].values[0]/30, 3)
            else:
                realPanelTemperature = 0
            
            prev_count = processEnd.hour - 5
            current_count = prev_count + 1
            dailySolarPowerGeneration_old["realPanelTemperature"] = ((dailySolarPowerGeneration_old["realPanelTemperature"] * prev_count) + realPanelTemperature) / current_count
            dailySolarPowerGeneration_old["realPanelTemperaturePerHour"][(processEnd + datetime.timedelta(hours=1)).strftime("%H")+"H"]["data"] += realPanelTemperature
        
        realPowerGenerationJson = json.dumps(dailySolarPowerGeneration_old["realPowerGenerationPerHour"])
        budgetPowerGenerationJson = json.dumps(dailySolarPowerGeneration_old["budgetPowerGenerationPerHour"])
        referencePowerGenerationJson = json.dumps(dailySolarPowerGeneration_old["referencePowerGenerationPerHour"])
        predictPowerGenerationJson = json.dumps(dailySolarPowerGeneration_old["predictPowerGenerationPerHour"])
        stationPowerGenerationJson = json.dumps(dailySolarPowerGeneration_old["stationPowerGenerationPerHour"])
        realIrradiationJson = json.dumps(dailySolarPowerGeneration_old["realIrradiationPerHour"])
        realPanelTemperatureJson = json.dumps(dailySolarPowerGeneration_old["realPanelTemperaturePerHour"])

        tempString = f"'{dailySolarPowerGeneration_old['operationDate'].strftime('%Y-%m-%d')}', \
                        '{dailySolarPowerGeneration_old['siteId']}', '{data['groupId'].to_numpy()[0]}', \
                        '{dailySolarPowerGeneration_old['inverterId']}', '{data['inverterDescription'].to_numpy()[0]}',\
                        '{realPowerGeneration}', '{realPowerGenerationJson}', \
                        '{budgetPowerGeneration}', '{budgetPowerGenerationJson}', \
                        '{referencePowerGeneration}', '{referencePowerGenerationJson}', \
                        '{predictPowerGeneration}', '{predictPowerGenerationJson}', \
                        '{stationPowerGeneration}', '{stationPowerGenerationJson}', \
                        '{realIrradiation}', '{realIrradiationJson}', \
                        '{realPanelTemperature}', '{realPanelTemperatureJson}' "
        insertString += f"({tempString}) , "
        
    insertString = insertString[:-2]
    return insertString

def update(processStart, processEnd, processplatform_engine, uiplatform_engine, reportplatform_engine, insert=False):
    conn = processplatform_engine.connect()
    processStart_str = processStart.strftime('%Y-%m-%d %H:%M')
    processEnd_str = processEnd.strftime('%Y-%m-%d %H:%M')
    insertKey = ""
    
    # Table
    TSiteInverter_sql = f"SELECT siteId, inverterId FROM uiplatform.TSiteInverter"
    TSiteInverter = pd.read_sql(TSiteInverter_sql, con=uiplatform_engine)
    solarInvPowerGeneration_sql = f"SELECT * FROM archiveplatform.solarInvPowerGeneration where ts > '{processStart_str}' and ts < '{processEnd_str}' "
    solarInvPowerGeneration = pd.read_sql(sql.text(solarInvPowerGeneration_sql), con=processplatform_engine)
    dailySPG_sql = f"SELECT * FROM reportplatform.dailySolarPowerGeneration where operationDate = '{processStart_str[:10]}';"
    dailySPG = pd.read_sql(sql.text(dailySPG_sql), con=reportplatform_engine)
    
    if dailySPG.shape[0] == 0:
        insertString = startHour(processStart, processEnd, TSiteInverter, solarInvPowerGeneration)
    else:
        insertString = continueHour(processStart, processEnd, TSiteInverter, solarInvPowerGeneration, dailySPG)
    
    for name in dailySPG.columns:
        insertKey += "`" + name + "`, "
    insertKey = insertKey[:-2]

    if insert:
        if insertString == "": return -1
        sqlstr = f"replace into reportplatform.dailySolarPowerGeneration ({insertKey}) values {insertString}"
        conn.execute(sql.text(sqlstr))
        print("DailySolarPowerGeneration insert successfully.")

    return 0

if __name__ == "__main__":
    #IP
    host = "localhost"
    user = "admin"
    pwd = "Admin99"
    # host = "192.168.1.85:3306"
    # user = "admin"
    # pwd = "Admin99"

    #DB name
    dbUi = "uiplatform"
    dbRPF = "reportplatform"
    dbProcessPF = "processplatform"
    
    currentTS = datetime.datetime(2020, 7, 13, 5, 3)
    processplatform_engine = sql.create_engine(f"mysql+mysqldb://{user}:{pwd}@{host}/{dbProcessPF}", pool_recycle=3600*7)
    uiplatform_engine = sql.create_engine(f"mysql+mysqldb://{user}:{pwd}@{host}/{dbUi}", pool_recycle=3600*7)
    reportplatform_engine = sql.create_engine(f"mysql+mysqldb://{user}:{pwd}@{host}/{dbRPF}", pool_recycle=3600*7)
    s = time.time()
    update(currentTS-datetime.timedelta(minutes=2), currentTS, processplatform_engine, uiplatform_engine, reportplatform_engine, insert=False)
    print(f"Time elapsed {time.time()-s} sec.")
