import sqlalchemy as sql
import pandas as pd
import numpy as np
import copy
import time
import datetime
import math
import json
import argparse
import dailySolarEnvironmentStatistic
import sys, traceback

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

def calRealPowerGen(energyProducedLifeTime, invId, instCapacity):
    energyProducedLifeTime = energyProducedLifeTime.sort_values("_hour")
    energyProducedLifeTimeMaxArr = energyProducedLifeTime["maxTime"].to_numpy()
    energyProducedLifeTimeMinArr = energyProducedLifeTime["minTime"].to_numpy()
    
    maxVal = energyProducedLifeTimeMaxArr[energyProducedLifeTimeMaxArr > 0][-1]
    minVal = energyProducedLifeTimeMinArr[energyProducedLifeTimeMinArr > 0][0]
    diff = maxVal - minVal

    if diff < 0:
        diffHourly = energyProducedLifeTimeMaxArr - energyProducedLifeTimeMinArr
        diffHourly = diffHourly[diffHourly >= 0]
        diff = diffHourly.sum()

    RealPowerGen = diff / (instCapacity/1000)

    return RealPowerGen

def main(dataDate, insert=None):
    
    if dataDate is None:
        raise Exception("Please specify date to continue.")

    todayStart_str = f"{dataDate} 05:00"
    todayStart = datetime.datetime.strptime(todayStart_str, "%Y-%m-%d %H:%M")
    todayEnd_str = f"{dataDate} 19:00"
    todayEnd = datetime.datetime.strptime(todayEnd_str, "%Y-%m-%d %H:%M")

    # Engine
    uiplatform_engine = sql.create_engine(f"mysql+mysqldb://{user}:{pwd}@{host}/{dbUi}?charset=utf8", pool_recycle=3600*7)
    archiveplatform_engine = sql.create_engine(f"mysql+mysqldb://{user}:{pwd}@{host}/{dbARC}?charset=utf8", pool_recycle=3600*7)

    # Table
    TSiteInverter_sql = f"\
        SELECT \
            a.siteId, a.inverterId, c.ieee, a.instCapacity\
        FROM\
            uiplatform.TSiteInverter AS a\
                INNER JOIN\
            (SELECT \
                *\
            FROM\
                dataplatform.TLogicDevice) AS b ON b.id = a.logicDeviceId\
                INNER JOIN\
            (SELECT \
                *\
            FROM\
                dataplatform.TDevice) AS c ON c.id = b.deviceId\
    "
    
    TSiteInverter = pd.read_sql(TSiteInverter_sql, con=uiplatform_engine)
    solarInvPowerGeneration_sql = f"""SELECT * FROM archiveplatform.solarInvPowerGeneration where ts like '{dataDate}%' """
    solarInvPowerGeneration = pd.read_sql(sql.text(solarInvPowerGeneration_sql), con=archiveplatform_engine)
    sql_query = f"\
        SELECT \
            receivedSync,\
            SUBSTRING(receivedSync, 12, 2) AS _hour,\
            ieee,\
            max(energyProducedLifeTime) as maxTime,\
            min(energyProducedLifeTime) as minTime\
        FROM\
            dataplatform.solarInverter\
        WHERE\
            receivedSync LIKE '{dataDate}%'\
        GROUP BY ieee , _hour\
    "
    s = time.time()
    energyProducedLifeTime = pd.read_sql_query(sql.text(sql_query), con=archiveplatform_engine)
    print(f"Loading {time.time()-s} s")

    # 發電曲線
    sql_query = f"\
        replace into uiplatform.RptSitePowerGen(RptDate,siteId,totalActivePower)\
        SELECT \
            CONCAT(hour1, ':','0', 2minute_chk * 2, ':','00') AS RptDate,\
            siteId,\
            SUM(totalActivePower) AS totalActivePower\
        FROM\
            (SELECT \
                DATE_FORMAT(receivedSync, '%Y-%m-%d %H') AS hour1,\
                    FLOOR((MINUTE(receivedSync)) / 2) AS 2minute_chk,\
                    c.Id AS siteId,\
                    ieee,\
                    totalActivePower,\
                    COUNT(*)\
            FROM\
                dataplatform.solarInverter AS a, dataplatform.TCustProj AS b, uiplatform.TSite AS c\
            WHERE\
                a.gatewayId = b.projCode\
                    AND b.id = c.projId\
                    AND DATE_FORMAT(receivedSync, '%H:%i') >= '05:02'\
                    AND DATE_FORMAT(receivedSync, '%H:%i') <= '19:00'\
                    AND MINUTE(receivedSync) < 10\
                    AND DATE(receivedSync) = '{dataDate}'\
            GROUP BY hour1 , 2minute_chk , siteId , ieee) AS a\
        GROUP BY hour1 , 2minute_chk , siteId \
        UNION ALL SELECT \
            CONCAT(hour1, ':', 2minute_chk * 2, ':','00') AS RptDate,\
            siteId,\
            ROUND(SUM(totalActivePower), 0)\
        FROM\
            (SELECT \
                DATE_FORMAT(receivedSync, '%Y-%m-%d %H') AS hour1,\
                    FLOOR((MINUTE(receivedSync)) / 2) AS 2minute_chk,\
                    c.Id AS siteId,\
                    ieee,\
                    totalActivePower,\
                    COUNT(*)\
            FROM\
                dataplatform.solarInverter AS a, dataplatform.TCustProj AS b, uiplatform.TSite AS c\
            WHERE\
                a.gatewayId = b.projCode\
                    AND b.id = c.projId\
                    AND DATE_FORMAT(receivedSync, '%H:%i') >= '05:02'\
                    AND DATE_FORMAT(receivedSync, '%H:%i') <= '19:00'\
                    AND MINUTE(receivedSync) >= 10\
                    AND DATE(receivedSync) = '{dataDate}'\
            GROUP BY hour1 , 2minute_chk , siteId , ieee) AS a\
        GROUP BY hour1 , 2minute_chk , siteId\
        ORDER BY siteId , RptDate\
    "
    
    if insert:
        try:
            conn = uiplatform_engine.connect()
            conn.execute(sql.text(sql_query))
        except:
            traceback.print_exc(file=sys.stdout)
    
    # Inverter
    dailySolarPowerGeneration_dict = {"operationDate": [],
                                      "siteId": [],
                                      "groupId": [],
                                      "inverterId": [],
                                      "inverterDescription": [],
                                      "realPowerGeneration": [],
                                      "realPowerGenerationPerHour": [],
                                      "budgetPowerGeneration": [],
                                      "budgetPowerGenerationPerHour": [],
                                      "referencePowerGeneration": [],
                                      "referencePowerGenerationPerHour": [],
                                      "predictPowerGeneration": [],
                                      "predictPowerGenerationPerHour": [],
                                      "stationPowerGeneration":[], 
                                      "stationPowerGenerationPerHour":[], 
                                      "realIrradiation": [],
                                      "realIrradiationPerHour":[], 
                                      "realPanelTemperature": [], 
                                      "realPanelTemperaturePerHour":[]}
    for ind, Inv in TSiteInverter.iterrows():
        InvID = Inv["inverterId"]
        ieee = Inv["ieee"]
        sId = Inv["siteId"]
        instCapacity = Inv["instCapacity"]
        print(f"Processing Inv {InvID}.")

        jsonObj = {"realPowerGeneration":{}, "budgetPowerGeneration":{}, "referencePowerGeneration":{}, "stationPowerGeneration":{}, "predictPowerGeneration":{}, "realIrradiation":{}, "realPanelTemperature":{}}
        realPowerGenerationSum = 0; budgetPowerGenerationSum = 0; referencePowerGenerationSum = 0; stationPowerGenerationSum = 0; predictPowerGenerationSum = 0; realIrradiationSum = 0; realPanelTemperatureSum = 0
        
        processStart = todayStart
        processEnd = processStart + datetime.timedelta(hours=1)
        
        count = 0
        while processStart < todayEnd:
            data = solarInvPowerGeneration[(solarInvPowerGeneration["ts"]>=processStart) & (solarInvPowerGeneration["ts"]<processEnd) & (solarInvPowerGeneration["inverterId"]==InvID)]
            if data.shape[0] == 0:
                print(f"Interter {InvID} has no data btw {processStart} - {processEnd}. Stop processing {InvID}")
                break
            else:
                count += 1
                
                realPowerGeneration = round((data["realPowerGeneration"].sum() / data["realPowerGeneration"].size), 3)
                if pd.isna(realPowerGeneration):
                    realPowerGeneration = None
                jsonObj["realPowerGeneration"][(processStart + datetime.timedelta(hours=1)).strftime("%H")+"H"] = {"data": realPowerGeneration}
                
                budgetPowerGeneration = round((data["budgetPowerGeneration"].sum() / data["budgetPowerGeneration"].size), 3)
                if pd.isna(budgetPowerGeneration):
                    budgetPowerGeneration = None
                else:
                    budgetPowerGenerationSum += budgetPowerGeneration
                jsonObj["budgetPowerGeneration"][(processStart + datetime.timedelta(hours=1)).strftime("%H")+"H"] = {"data": budgetPowerGeneration}
                
                referencePowerGeneration = round((data["referencePowerGeneration"].sum() / data["referencePowerGeneration"].size), 3)
                if pd.isna(referencePowerGeneration):
                    referencePowerGeneration = None
                else:
                    referencePowerGenerationSum += referencePowerGeneration
                jsonObj["referencePowerGeneration"][(processStart + datetime.timedelta(hours=1)).strftime("%H")+"H"] = {"data": referencePowerGeneration}
                
                stationPowerGeneration = round((data["stationPowerGeneration"].sum() / data["stationPowerGeneration"].size), 3)
                if pd.isna(stationPowerGeneration):
                    stationPowerGeneration = None
                else:
                    stationPowerGenerationSum += stationPowerGeneration
                jsonObj["stationPowerGeneration"][(processStart + datetime.timedelta(hours=1)).strftime("%H")+"H"] = {"data": stationPowerGeneration}
                
                predictPowerGeneration = round((data["predictPowerGeneration"].sum() / data["predictPowerGeneration"].size), 3)
                if pd.isna(predictPowerGeneration):
                    predictPowerGeneration = None
                else:
                    predictPowerGenerationSum += predictPowerGeneration
                jsonObj["predictPowerGeneration"][(processStart + datetime.timedelta(hours=1)).strftime("%H")+"H"] = {"data": predictPowerGeneration}
                
                realIrradiation = round((data["realIrradiation"].sum() / data["realIrradiation"].size), 3)
                if pd.isna(realIrradiation):
                    realIrradiation = None
                else:
                    realIrradiationSum += realIrradiation
                jsonObj["realIrradiation"][(processStart + datetime.timedelta(hours=1)).strftime("%H")+"H"] = {"data": realIrradiation}
                
                realPanelTemperature = round((data["realPanelTemperature"].sum() / data["realPanelTemperature"].size), 3)
                if pd.isna(realPanelTemperature):
                    realPanelTemperature = None
                else:
                    realPanelTemperatureSum += realPanelTemperature
                jsonObj["realPanelTemperature"][(processStart + datetime.timedelta(hours=1)).strftime("%H")+"H"] = {"data": realPanelTemperature}
                
                    
            processStart = processEnd
            processEnd = processStart + datetime.timedelta(hours=1)

        if count == 0:
            continue
        
        try:
            realPowerGenerationSum = calRealPowerGen(energyProducedLifeTime[energyProducedLifeTime["ieee"].str.contains(ieee, case=False)], InvID, instCapacity)
        except:
            realPowerGenerationSum = None

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
        dailySolarPowerGeneration_dict["realPowerGeneration"].append(round(realPowerGenerationSum, 3) if realPowerGenerationSum != None else realPowerGenerationSum)
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
        reportplatform_engine = sql.create_engine(f'mysql+mysqldb://{user}:{pwd}@{host}/{dbRPF}?charset=utf8', pool_recycle=3600*7)
        df.to_sql('dailySolarPowerGeneration', con=reportplatform_engine, if_exists='append', index=False)
        print("[Table Insert] dailySolarPowerGeneration")

        try:
            re = dailySolarEnvironmentStatistic.cal(reportplatform_engine, dataDate, insert=insert)
            if re == 0:
                print("[Table Insert] dailySolarEnvironmentStatistic")
        except:
            print("dailySolarEnvironmentStatistic has been inserted or there might be a bug.")
        
        reportplatform_engine.dispose()

    uiplatform_engine.dispose()
    archiveplatform_engine.dispose()

    return 0

if __name__ == "__main__":
    # sites = [1,2,3,4,5,6,7,15,16]
    # dataDate = "2020-07-14"
    # for sId in sites:
    #     main(sId, dataDate, insert=True)

    sId = 16
    dataDate = "2020-08-18"
    main(sId, dataDate, insert=False)
