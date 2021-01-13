import sqlalchemy as sql
import pandas as pd
import numpy as np
import copy
import time
import datetime
import math
import json
import argparse
from toolkits import information, connection, calculation
from toolkits import dailySolarPowerGeneration, dailySolarRevenue
from toolkits import monthlySolarPowerGeneration, monthlySolarRevenue

def convertTs(ts):
    return datetime.datetime.utcfromtimestamp(ts.tolist()/1e9)

def main(currentTS, debug=None, insert=None):
    totalTime = 0

    # Engine
    engine_dict = connection.getEngine()

    uiplatform_engine = engine_dict["uiplatform"]
    historyDataCWB_engine = engine_dict["historyDataCWB"]
    archiveplatform_engine = engine_dict["archiveplatform"]
    dataplatform_engine = engine_dict["dataplatform"]
    processplatform_engine = engine_dict["processplatform"]
    ui_85_engine = engine_dict["processplatform"]
    reportplatform_engine = engine_dict["reportplatform"]

    # Table
    env_sql = "SELECT receivedSync FROM dataplatform.environment order by receivedSync desc limit 1"
    environment_last_time = pd.read_sql_query(env_sql, con=dataplatform_engine)
    environment_last_minute = environment_last_time.receivedSync.dt.minute[0]
    environment_last_time = environment_last_time.receivedSync[0]

    solarInverter_sql = "SELECT receivedSync FROM dataplatform.solarInverter order by receivedSync desc limit 1"
    solarInverter_last_time = pd.read_sql(solarInverter_sql, con=dataplatform_engine)
    solarInverter_last_minute = solarInverter_last_time.receivedSync.dt.minute[0]
    solarInverter_last_time = solarInverter_last_time.receivedSync[0]

    currentTS_min = currentTS.minute

    if not debug:
        if currentTS_min % 2 == 0:
            return "Waiting..."
        if (currentTS - datetime.timedelta(minutes=2)) > environment_last_time:
            return f"currentTS is {currentTS}, envTS is {environment_last_time}"
        if (currentTS - datetime.timedelta(minutes=2)) > solarInverter_last_time:
            return f"currentTS is {currentTS}, solarTS is {solarInverter_last_time}"

    year = currentTS.strftime('%Y')
    month = currentTS.strftime('%m')
    day = currentTS.strftime('%d')

    dbARCNum = f"{year}{month}"
    print(f"dbARCNum = {dbARCNum}")

    dbCWBNum = f"{month}"
    dbCWBDate = f"{month}-{day}"
    dataDate = f"{year}-{month}-{day}"

    dbCWBStartTime = (currentTS.replace(second=0) - pd.Timedelta(minutes=2)).replace(year=2020)
    dbCWBEndTime = dbCWBStartTime + pd.Timedelta(minutes=2)

    processStart = currentTS.replace(second=0) - pd.Timedelta(minutes=2)
    processEnd = processStart + pd.Timedelta(minutes=2)

    TSite = information.getTSite(uiplatform_engine)
    TPanelModel = information.getTPanelModel(uiplatform_engine)
    historyDataCWB_dbCWBNum = information.getHistoryDataCWB(historyDataCWB_engine, dbCWBNum)
    VSiteDevice = information.getVSiteDevice(uiplatform_engine)
    environment_dbARCNum = information.getEnvironment(dataplatform_engine, str(processStart))
    solarInverter = information.getSolarInverter(dataplatform_engine, processStart)
    solarMpptPowerGeneration_tb = information.getSolarMpptPowerGeneration(processplatform_engine, processStart-datetime.timedelta(minutes=30), processStart-datetime.timedelta(minutes=16))

    # Inverter Info
    InverterMpptInfo = information.getInverterMpptInfo(uiplatform_engine)
    if isinstance(InverterMpptInfo, str):
        return InverterMpptInfo

    solarMpptPowerGeneration_dict = {"ts": [],"siteId": [],"groupId": [],"inverterId": [],"inverterDescription": [],"mpptId": [],"realPowerGeneration": [],"budgetPowerGeneration": [],"referencePowerGeneration": [],"stationPowerGeneration": [],"predictPowerGeneration": [],"realIrradiation": [],"realPanelTemperature": []}

    traceStart = time.time()
    sId = None
    for ind, InvInfo in InverterMpptInfo.iterrows():
        print(f"Processing Inverter [ {InvInfo['ieee']} ] ")

        s = time.time()

        if sId != InvInfo["siteId"]:
            # 太陽能板瓦特溫升負係數 panelTempSet=0.0044
            tmpTb = TSite.merge(TPanelModel, left_on="solarPanelModelId", right_on="id")
            _panelTempSet = tmpTb.loc[tmpTb["id_x"]==InvInfo["siteId"]].MaxPowerTemp.values[0]
            if _panelTempSet is None or "":
                return "panelTempSet is empty!"
                raise Exception(f"panelTempSet is empty!")
            else:
                print(f"panelTempSet = {_panelTempSet}")
            
            # 太陽能板衰減率
            _panelEfficiencySet = information.getPanelEfficiencySet(InvInfo["siteId"], dataDate, TSite, TPanelModel)
            if isinstance(_panelEfficiencySet, str):
                return _panelEfficiencySet
            
            # 日照
            _insolationIEEE = VSiteDevice[(VSiteDevice["deviceCategoryId"]==2) & (VSiteDevice["siteId"]==InvInfo["siteId"])]["ieee"].values[0]
            if _insolationIEEE is None or "":
                return f"insolationIEEE is empty!"
            else:
                print(f"insolationIEEE = {_insolationIEEE}")
            
            environment_insolationIEEE = environment_dbARCNum[(environment_dbARCNum["receivedSync"] >= processStart) & (processEnd >= environment_dbARCNum["receivedSync"]) & (environment_dbARCNum["ieee"]==_insolationIEEE)]
            print(processStart, processEnd, _insolationIEEE, environment_last_time)
            if environment_insolationIEEE.shape[0] == 0:
                print("No data in insolationIEEE = "+_insolationIEEE)
                # return "Env insolationIEEE = " + _insolationIEEE
            else:
                print(f"environment{dbARCNum} has data with length {environment_insolationIEEE.shape[0]}")

            _moduleTempIEEE = VSiteDevice[(VSiteDevice["deviceCategoryId"]==4) & (VSiteDevice["siteId"]==InvInfo["siteId"])]["ieee"].values[0]
            if _moduleTempIEEE is None or "":
                return "moduleTempIEEE is empty!"
            else:
                print(f"moduleTempIEEE = {_moduleTempIEEE}")

            environment_moduleTempIEEE = environment_dbARCNum[(environment_dbARCNum["receivedSync"] >= processStart) & (processEnd >= environment_dbARCNum["receivedSync"]) & (environment_dbARCNum["ieee"]==_moduleTempIEEE)]
            if environment_moduleTempIEEE.shape[0] == 0:
                print("No data in moduleTempIEEE = "+_moduleTempIEEE)
                # return "Env moduleTempIEEE " + _moduleTempIEEE
            else:
                print(f"environment{dbARCNum} has data with length {environment_moduleTempIEEE.shape[0]}")

            sId = InvInfo["siteId"]

        print(f"[{ind}] Inv Info: groupID:{InvInfo['groupId']} Inv:{InvInfo['inverterId']} MPPT:{InvInfo['mpptId']} Capacity:{InvInfo['mpptInstCapacity']} azimuth:{InvInfo['azimuth']} inclination:{InvInfo['inclination']} Inverter ieee:{InvInfo['ieee']} deviceDesc:{InvInfo['deviceDesc']}")
        
        ## ======================== start processing [part 1] ======================== ##
        calculator = calculation.Cal(processStart, processEnd, environment_dbARCNum, historyDataCWB_dbCWBNum, _insolationIEEE, _moduleTempIEEE, InvInfo, _panelEfficiencySet, _panelTempSet, solarInverter, solarMpptPowerGeneration_tb)
        
        # print(f"Stage 1 {time.time()-s} s")

        # ======= 日照 realIrradiation ======= #
        s = time.time()
        realIrradiation = calculator.calRealIrradiation()
        # print(f"Stage 2 {time.time()-s} s")
                    
        # ======= 實際溫度 realPanelTemperature ======= #
        s = time.time()
        realPanelTemperature = calculator.calRealPanelTemperature()
        # print(f"Stage 3 {time.time()-s} s")
        
        # ======= 預算發電量 budgetPowerGeneration ======= #
        s = time.time()
        budgetPowerGeneration = calculator.calBudgetPowerGeneration()
        # print(f"Stage 4 {time.time()-s} s")
        
        # ======= 實際發電量 realPowerGeneration ======= #
        s = time.time()
        realPowerGeneration = calculator.calRealPowerGeneration()
        # print(f"Stage 6 {time.time()-s} s")

        # ======= 案場參考發電量 referencePowerGeneration ======= #
        s = time.time()
        if (realIrradiation == None) or (realPanelTemperature == None): 
            if realPowerGeneration == None:
                realPowerGeneration = 0
                referencePowerGeneration = 0
            else:
                referencePowerGeneration = realPowerGeneration
        else:
            referencePowerGeneration = calculator.calReferencePowerGeneration(realIrradiation, realPanelTemperature)
        # print(f"Stage 5 {time.time()-s} s")
        
        # ======= 觀測站發電量 stationPowerGeneration ======= #
        s = time.time()
        stationPowerGeneration = calculator.calStationPowerGeneration()
        # print(f"Stage 7 {time.time()-s} s")

        # ======= 預測發電量 predictPowerGeneration ======= #
        s = time.time()
        predictPowerGeneration = calculator.calPredictPowerGeneration()
        # print(f"Stage 8 {time.time()-s} s")
        
        # insert to dataframe
        solarMpptPowerGeneration_dict["ts"].append(processEnd - datetime.timedelta(minutes=1))
        solarMpptPowerGeneration_dict["siteId"].append(InvInfo["siteId"])
        solarMpptPowerGeneration_dict["groupId"].append(InvInfo['groupId'])
        solarMpptPowerGeneration_dict["inverterId"].append(InvInfo['inverterId'])
        solarMpptPowerGeneration_dict["inverterDescription"].append(InvInfo['deviceDesc'])
        solarMpptPowerGeneration_dict["mpptId"].append(InvInfo['mpptId'])
        solarMpptPowerGeneration_dict["realPowerGeneration"].append(realPowerGeneration)
        solarMpptPowerGeneration_dict["budgetPowerGeneration"].append(budgetPowerGeneration)
        solarMpptPowerGeneration_dict["referencePowerGeneration"].append(referencePowerGeneration)
        solarMpptPowerGeneration_dict["stationPowerGeneration"].append(stationPowerGeneration)
        solarMpptPowerGeneration_dict["predictPowerGeneration"].append(predictPowerGeneration)
        solarMpptPowerGeneration_dict["realIrradiation"].append(realIrradiation)
        solarMpptPowerGeneration_dict["realPanelTemperature"].append(realPanelTemperature)
    
    df = pd.DataFrame(data=solarMpptPowerGeneration_dict)
    success = -1
    if insert:
        df.to_sql('solarMpptPowerGeneration', con=archiveplatform_engine, if_exists='append', index=False)
        success = 0

    tranceEnd = time.time()
    timeDiff = tranceEnd-traceStart
    totalTime += timeDiff
    print(f"Time in calculating inverters: {timeDiff} sec.")

    ## ======================== end processing ======================== ##

    ## ======================== start processing [part 2] ======================== ##
    if success == 0:
        print("Start processing.")
        sites = TSite[TSite["deleteFlag"]==0]["id"].to_numpy()
        
        #insert into Inverter
        for sId in sites:
            # insert into solarInvPowerGeneration
            solarInv_sql = f"SELECT ts,   a.siteId,   a.groupId,   a.inverterId,   inverterDescription,   round(sum(realPowerGeneration*mpptInstCapacity)/sum(mpptInstCapacity),3) as realPowerGeneration,   round(sum(budgetPowerGeneration*mpptInstCapacity)/sum(mpptInstCapacity),3) as budgetPowerGeneration,   round(sum(referencePowerGeneration*mpptInstCapacity)/sum(mpptInstCapacity),3)  as referencePowerGeneration,   round(sum(stationPowerGeneration*mpptInstCapacity)/sum(mpptInstCapacity),3) as stationPowerGeneration,   round(sum(predictPowerGeneration*mpptInstCapacity)/sum(mpptInstCapacity),3) as predictPowerGeneration,   realIrradiation,   realPanelTemperature FROM     archiveplatform.solarMpptPowerGeneration as a,     uiplatform.TInverterMppt as b where     a.inverterId=b.inverterId and     a.mpptId=b.mpptId and     a.siteId={sId} and     ts>='{processStart}' and     ts<'{processEnd}' group by a.inverterId, ts"
            solarInvPowerGeneration = pd.read_sql(solarInv_sql, con=archiveplatform_engine)
            if insert:
                solarInvPowerGeneration.to_sql('solarInvPowerGeneration', con=archiveplatform_engine, if_exists='append', index=False)
                
            #insert into group
            # insert into solarGroupPowerGeneration
            solarGroup_sql = f"SELECT ts,     a.siteId,     a.groupId,     round(sum(realPowerGeneration*instCapacity)/sum(instCapacity),3) as realPowerGeneration,     round(sum(budgetPowerGeneration*instCapacity)/sum(instCapacity),3) as budgetPowerGeneration,     round(sum(referencePowerGeneration*instCapacity)/sum(instCapacity),3)  as referencePowerGeneration,     round(sum(stationPowerGeneration*instCapacity)/sum(instCapacity),3) as stationPowerGeneration,     round(sum(predictPowerGeneration*instCapacity)/sum(instCapacity),3) as predictPowerGeneration,     realIrradiation,     realPanelTemperature FROM     archiveplatform.solarInvPowerGeneration as a,     uiplatform.TSiteInverter as b where     a.siteId=b.siteId and     a.groupId=b.groupId and     a.inverterId=b.inverterId and     a.siteId={sId} and     ts>='{processStart}' and     ts<'{processEnd}' group by a.groupId, ts"
            solarGroupPowerGeneration = pd.read_sql(solarGroup_sql, con=archiveplatform_engine)
            if insert:
                solarGroupPowerGeneration.to_sql('solarGroupPowerGeneration', con=archiveplatform_engine, if_exists='append', index=False)

            #insert into site
            # insert into solarSitePowerGeneration
            solarSite_sql = f"SELECT ts,     a.siteId,     round(sum(realPowerGeneration*instCapacity)/sum(instCapacity),3) as realPowerGeneration,     round(sum(budgetPowerGeneration*instCapacity)/sum(instCapacity),3) as budgetPowerGeneration,     round(sum(referencePowerGeneration*instCapacity)/sum(instCapacity),3)  as referencePowerGeneration,     round(sum(stationPowerGeneration*instCapacity)/sum(instCapacity),3) as stationPowerGeneration,     round(sum(predictPowerGeneration*instCapacity)/sum(instCapacity),3) as predictPowerGeneration,     realIrradiation,     realPanelTemperature FROM     archiveplatform.solarInvPowerGeneration as a,     uiplatform.TSiteInverter as b WHERE     a.siteId=b.siteId and     a.groupId=b.groupId and     a.inverterId=b.inverterId and     a.siteId={sId} and     ts>='{processStart}' and     ts<'{processEnd}' group by a.siteId,ts"
            solarSitePowerGeneration = pd.read_sql(solarSite_sql, con=archiveplatform_engine)
            if insert:
                solarSitePowerGeneration.to_sql('solarSitePowerGeneration', con=archiveplatform_engine, if_exists='append', index=False)

        print("solarInvPowerGeneration / solarGroupPowerGeneration / solarSitePowerGeneration insert successfully.")
        success = 0
    else:
        return "solarMpptPowerGeneration inserts unsuccessfully."
    ## ======================== end processing ======================== ##

    ## ======================== start processing [part 3] ======================== ##
    if success == 0:
        success = dailySolarPowerGeneration.update(processStart, processEnd, archiveplatform_engine, uiplatform_engine, reportplatform_engine, insert=insert)
    else:
        return "solarInvPowerGeneration, solarGroupPowerGeneration and solarSitePowerGeneration insert unsuccessfully."
    
    if success == 0:
        monthlySolarPowerGeneration.update(currentTS, reportplatform_engine, insert=insert)

    ## ======================== end processing ======================== ##

    ## ======================== start processing [part 4] ======================== ##
    if success == 0:
        success = dailySolarRevenue.update(currentTS, archiveplatform_engine, reportplatform_engine, insert=insert)
    else:
        return "dailySolarPowerGeneration inserts unsuccessfully."
    
    if success == 0:
        monthlySolarRevenue.update(currentTS, reportplatform_engine, insert=insert)
    ## ======================== end processing ======================== ##

    return 0

if __name__ == "__main__":
    # currentTS = datetime.datetime.now() - datetime.timedelta(minutes=10)
    currentTS = datetime.datetime(2020, 7, 13, 9, 3)
    re = main(currentTS, debug=True, insert=False)
    print(re)
