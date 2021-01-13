import sqlalchemy as sql
import pandas as pd
import numpy as np
import copy
import time
import datetime
import math
import json
import argparse

#IP
host = "localhost"
user = "ecoetl"
pwd = "ECO4etl"

#DB name
dbData = "dataplatform"
dbUi = "uiplatform"
dbRPF = "reportplatform"
dbARC = "archiveplatform"
dbProcessPF = "processplatform"
dbCWB = "historyDataCWB"

def main(sId, dataDate, insert=None):
    
    if sId is None:
        raise Exception("Please specify a siteId to continue.")
    if dataDate is None:
        raise Exception("Please specify date to continue.")

    todayStart_str = f"{dataDate} 05:00"
    todayStart = datetime.datetime.strptime(todayStart_str, "%Y-%m-%d %H:%M")
    todayEnd_str = f"{dataDate} 19:00"
    todayEnd = datetime.datetime.strptime(todayEnd_str, "%Y-%m-%d %H:%M")

    # Engine
    processplatform_engine = sql.create_engine(f"mysql+mysqldb://{user}:{pwd}@{host}/{dbProcessPF}", pool_recycle=3600*7)
    uiplatform_engine = sql.create_engine(f"mysql+mysqldb://{user}:{pwd}@{host}/{dbUi}", pool_recycle=3600*7)

    # Table
    print("Loading Tables.")
    TSiteInverter_sql = f"SELECT inverterId FROM uiplatform.TSiteInverter where siteId={sId}"
    TSiteInverter = pd.read_sql(TSiteInverter_sql, con=uiplatform_engine)
    solarInvPowerGeneration_sql = f"""SELECT * FROM processplatform.solarInvPowerGeneration where ts like '{dataDate}%' """
    solarInvPowerGeneration = pd.read_sql(sql.text(solarInvPowerGeneration_sql), con=processplatform_engine)
    print("Tables loaded.")

    # Inverter
    dailySolarPowerGeneration_dict = {"operationDate": [],"siteId": [],"groupId": [],"inverterId": [],"inverterDescription": [],"realPowerGeneration": [],"realPowerGenerationPerHour": [],"budgetPowerGeneration": [],"budgetPowerGenerationPerHour": [],"referencePowerGeneration": [],"referencePowerGenerationPerHour": [], "predictPowerGeneration": [],"predictPowerGenerationPerHour": [],"stationPowerGeneration":[], "stationPowerGenerationPerHour":[], "realIrradiation": [],"realIrradiationPerHour":[], "realPanelTemperature": [], "realPanelTemperaturePerHour":[]}
    for ind, _InvID in TSiteInverter.iterrows():
        InvID = _InvID[0]
        print(f"Processing Inv {InvID}.")
        jsonObj = {"realPowerGeneration":{}, "budgetPowerGeneration":{}, "referencePowerGeneration":{}, "stationPowerGeneration":{}, "predictPowerGeneration":{}, "realIrradiation":{}, "realPanelTemperature":{}}
        realPowerGenerationSum = 0; budgetPowerGenerationSum = 0; referencePowerGenerationSum = 0; stationPowerGenerationSum = 0; predictPowerGenerationSum = 0; realIrradiationSum = 0; realPanelTemperatureSum = 0
        
        processStart = todayStart
        processEnd = processStart + datetime.timedelta(hours=1)
        
        count = 0
        while processStart < todayEnd:
            # >= 會影響到下面 與shell不同
            data = solarInvPowerGeneration[(solarInvPowerGeneration["ts"]>=processStart) & (solarInvPowerGeneration["ts"]<processEnd) & (solarInvPowerGeneration["inverterId"]==InvID)]
            if data.shape[0] == 0:
                print(f"Interter {InvID} has no data btw {processStart} - {processEnd}. Stop processing {InvID}")
                break
            else:
                count += 1
                
                realPowerGeneration = round(data["realPowerGeneration"].mean(), 3)
                realPowerGenerationSum += realPowerGeneration
                jsonObj["realPowerGeneration"][(processStart + datetime.timedelta(hours=1)).strftime("%H")+"H"] = {"data": realPowerGeneration}
                
                budgetPowerGeneration = round(data["budgetPowerGeneration"].mean(), 3)
                budgetPowerGenerationSum += budgetPowerGeneration
                jsonObj["budgetPowerGeneration"][(processStart + datetime.timedelta(hours=1)).strftime("%H")+"H"] = {"data": budgetPowerGeneration}
                
                referencePowerGeneration = round(data["referencePowerGeneration"].mean(), 3)
                referencePowerGenerationSum += referencePowerGeneration
                jsonObj["referencePowerGeneration"][(processStart + datetime.timedelta(hours=1)).strftime("%H")+"H"] = {"data": referencePowerGeneration}
                
                stationPowerGeneration = round(data["stationPowerGeneration"].mean(), 3)
                stationPowerGenerationSum += stationPowerGeneration
                jsonObj["stationPowerGeneration"][(processStart + datetime.timedelta(hours=1)).strftime("%H")+"H"] = {"data": stationPowerGeneration}
                
                predictPowerGeneration = round(data["predictPowerGeneration"].mean(), 3)
                predictPowerGenerationSum += predictPowerGeneration
                jsonObj["predictPowerGeneration"][(processStart + datetime.timedelta(hours=1)).strftime("%H")+"H"] = {"data": predictPowerGeneration}
                
                realIrradiation = round(data["realIrradiation"].mean(), 3)
                realIrradiationSum += realIrradiation
                jsonObj["realIrradiation"][(processStart + datetime.timedelta(hours=1)).strftime("%H")+"H"] = {"data": realIrradiation}
                
                realPanelTemperature = round(data["realPanelTemperature"].mean(), 3)
                realPanelTemperatureSum += realPanelTemperature
                jsonObj["realPanelTemperature"][(processStart + datetime.timedelta(hours=1)).strftime("%H")+"H"] = {"data": realPanelTemperature}
                
                    
            processStart = processEnd
            processEnd = processStart + datetime.timedelta(hours=1)
        
        # per day
        try:
            groupId = solarInvPowerGeneration[solarInvPowerGeneration["inverterId"]==InvID]["groupId"].values[0]
            deviceDesc = solarInvPowerGeneration[solarInvPowerGeneration["inverterId"]==InvID]["inverterDescription"].values[0]
        except:
            continue
        
        dailySolarPowerGeneration_dict["operationDate"].append(dataDate)
        dailySolarPowerGeneration_dict["siteId"].append(sId)
        dailySolarPowerGeneration_dict["groupId"].append(groupId)
        dailySolarPowerGeneration_dict["inverterId"].append(InvID)
        dailySolarPowerGeneration_dict["inverterDescription"].append(deviceDesc)
        dailySolarPowerGeneration_dict["realPowerGeneration"].append(round(realPowerGenerationSum, 3))
        dailySolarPowerGeneration_dict["realPowerGenerationPerHour"].append(json.dumps(jsonObj["realPowerGeneration"]))
        dailySolarPowerGeneration_dict["budgetPowerGeneration"].append(round(budgetPowerGenerationSum, 3))
        dailySolarPowerGeneration_dict["budgetPowerGenerationPerHour"].append(json.dumps(jsonObj["budgetPowerGeneration"]))
        dailySolarPowerGeneration_dict["referencePowerGeneration"].append(round(referencePowerGenerationSum, 3))
        dailySolarPowerGeneration_dict["referencePowerGenerationPerHour"].append(json.dumps(jsonObj["referencePowerGeneration"]))
        dailySolarPowerGeneration_dict["predictPowerGeneration"].append(round(predictPowerGenerationSum, 3))
        dailySolarPowerGeneration_dict["predictPowerGenerationPerHour"].append(json.dumps(jsonObj["predictPowerGeneration"]))
        dailySolarPowerGeneration_dict["stationPowerGeneration"].append(round(stationPowerGenerationSum, 3))
        dailySolarPowerGeneration_dict["stationPowerGenerationPerHour"].append(json.dumps(jsonObj["stationPowerGeneration"]))
        dailySolarPowerGeneration_dict["realIrradiation"].append(round(realIrradiationSum, 3))
        dailySolarPowerGeneration_dict["realIrradiationPerHour"].append(json.dumps(jsonObj["realIrradiation"]))
        dailySolarPowerGeneration_dict["realPanelTemperature"].append(round(realPanelTemperatureSum/count, 3))
        dailySolarPowerGeneration_dict["realPanelTemperaturePerHour"].append(json.dumps(jsonObj["realPanelTemperature"]))

    df = pd.DataFrame(data=dailySolarPowerGeneration_dict)

    if insert:
        ui_85_engine = sql.create_engine(f'mysql+mysqldb://{user}:{pwd}@{host}/{dbRPF}', pool_recycle=3600*7)
        df.to_sql('dailySolarPowerGeneration', con=ui_85_engine, if_exists='append', index=False)

    print("DailySolarPowerGeneration insert successfully.")

    return 0

if __name__ == "__main__":
    sId = 1
    dataDate = "2020-06-30"
    main(sId, dataDate, insert=False)
