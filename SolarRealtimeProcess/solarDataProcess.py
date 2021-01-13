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
from toolkits import information, connection, calculation
from toolkits import dailySolarPowerGeneration, dailySolarRevenue
from toolkits import monthlySolarPowerGeneration, monthlySolarRevenue

def convertTs(ts):
    return datetime.datetime.utcfromtimestamp(ts.tolist()/1e9)

def main(currentTS, debug=None, insert=None, ncpu=4):
    try:
        totalTime = 0

        currentTS_min = currentTS.minute
        # if currentTS_min % 2 == 0:
        #     return "Waiting..."

        # Engine
        engine_dict = connection.getEngine()

        uiplatform_engine = engine_dict["uiplatform"]
        historyDataCWB_engine = engine_dict["historyDataCWB"]
        archiveplatform_engine = engine_dict["archiveplatform"]
        dataplatform_engine = engine_dict["dataplatform"]
        processplatform_engine = engine_dict["processplatform"]
        ui_85_engine = engine_dict["processplatform"]
        reportplatform_engine = engine_dict["reportplatform"]

        # update spGenRptWebWeather
        RptWebWeather = pd.read_sql_query("call uiplatform.spGenRptWebWeather()", uiplatform_engine)
        RptWebWeather.to_sql('RptWebWeather', con=uiplatform_engine, if_exists='append', index=False)

        # Table
        env_sql = "SELECT receivedSync FROM dataplatform.environment order by receivedSync desc limit 1"
        environment_last_time = pd.read_sql_query(env_sql, con=dataplatform_engine)
        environment_last_minute = environment_last_time.receivedSync.dt.minute[0]
        environment_last_time = environment_last_time.receivedSync[0]

        solarInverter_sql = "SELECT receivedSync FROM dataplatform.solarInverter order by receivedSync desc limit 1"
        solarInverter_last_time = pd.read_sql(solarInverter_sql, con=dataplatform_engine)
        solarInverter_last_minute = solarInverter_last_time.receivedSync.dt.minute[0]
        solarInverter_last_time = solarInverter_last_time.receivedSync[0]

        if not debug:
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

        traceDBStart = time.time()
        s_6 = time.time()
        TSite = information.getTSite(uiplatform_engine)
        print(f"Loading TSite in {time.time() - s_6}")
        s_5 = time.time()
        TPanelModel = information.getTPanelModel(uiplatform_engine)
        print(f"Loading TPanelModel in {time.time() - s_5}")
        s_4 = time.time()
        historyDataCWB_dbCWBNum = information.getHistoryDataCWB(historyDataCWB_engine, dbCWBNum)
        print(f"Loading historyDataCWB_dbCWBNum in {time.time() - s_4}")
        s_3 = time.time()
        VSiteDevice = information.getVSiteDevice(uiplatform_engine)
        print(f"Loading VSiteDevice in {time.time() - s_3}")
        s_2 = time.time()
        environment_dbARCNum = information.getEnvironment(dataplatform_engine, str(processStart)) # watch
        print(f"Loading env in {time.time() - s_2}")
        s_1 = time.time()
        solarInverter = information.getSolarInverter(dataplatform_engine, processStart) # watch
        print(f"Loading solarInv in {time.time() - s_1}")
        solarMpptPowerGeneration_tb = information.getSolarMpptPowerGeneration(processplatform_engine, processStart-datetime.timedelta(minutes=30), processStart-datetime.timedelta(minutes=16))
        tranceDBEnd = time.time()

        # Inverter Info
        InverterMpptInfo = information.getInverterMpptInfo(uiplatform_engine)
        if isinstance(InverterMpptInfo, str):
            return InverterMpptInfo

        # Test
        # InverterMpptInfo = InverterMpptInfo.append(InverterMpptInfo, ignore_index=True)
        # InverterMpptInfo = pd.concat([InverterMpptInfo]*18, ignore_index=True)
        if ncpu <= 0:
            ncpu = 1
        step = math.ceil(InverterMpptInfo.shape[0] / ncpu)
        InverterMpptInfoSplit = [InverterMpptInfo.iloc[i:i+step] for i in range(0, InverterMpptInfo.shape[0], step)]

        def F(InverterMpptInfo, 
              TSite, 
              TPanelModel, 
              VSiteDevice, 
              environment_dbARCNum, 
              processStart, 
              processEnd, 
              dbARCNum, 
              historyDataCWB_dbCWBNum, 
              solarInverter, 
              solarMpptPowerGeneration_tb, 
              return_df):
            sId = None
            solarMpptPowerGeneration_dict = {"ts": [],
                                            "siteId": [],
                                            "groupId": [],
                                            "inverterId": [],
                                            "inverterDescription": [],
                                            "mpptId": [],
                                            "realPowerGeneration": [],
                                            "budgetPowerGeneration": [],
                                            "referencePowerGeneration": [],
                                            "stationPowerGeneration": [],
                                            "predictPowerGeneration": [],
                                            "realIrradiation": [],
                                            "realPanelTemperature": []}
            
            for ind, InvInfo in InverterMpptInfo.iterrows():
                print(f"Processing Inverter [ {InvInfo['ieee']} ] ")
                if InvInfo["siteId"] == 17: break # 暫時更改 
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
                    referencePowerGeneration = None
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
            return_df.append(df)

        traceStart = time.time()
        manager = Manager()
        return_df = manager.list()

        processes = [Process(target=F, args=(InverterMpptInfo, TSite, TPanelModel, VSiteDevice, environment_dbARCNum, processStart, processEnd, dbARCNum, historyDataCWB_dbCWBNum, solarInverter, solarMpptPowerGeneration_tb, return_df)) for InverterMpptInfo in InverterMpptInfoSplit]
        pStart = [p.start() for p in processes]
        pJoin = [p.join() for p in processes]
        
        df = pd.concat(return_df)

        success = -1
        if insert:
            df.to_sql('solarMpptPowerGeneration', con=ui_85_engine, if_exists='append', index=False)
            success = 0

        tranceEnd = time.time()
        timeDiff = tranceEnd-traceStart
        totalTime += timeDiff
        print(f"Time in loading DB: {tranceDBEnd-traceDBStart} sec.")
        print(f"Time in calculating mppts: {timeDiff} sec.")

        ## ======================== end processing ======================== ##

        ## ======================== start processing [part 2] ======================== ##
        if success == 0:
            print("Start processing.")
            sites = TSite[TSite["deleteFlag"]==0]["id"].to_numpy()
            
            startP2 = time.time()
            # insert into solarInvPowerGeneration
            solarInv_sql = f"\
                SELECT ts, \
                    a.siteid, \
                    a.groupid, \
                    a.inverterid, \
                    inverterdescription, \
                    Round(Sum(realpowergeneration * mpptinstcapacity) / Sum(mpptinstcapacity) \
                    , 3) AS \
                    realPowerGeneration, \
                    Round(Sum(budgetpowergeneration * mpptinstcapacity) / Sum( \
                            mpptinstcapacity), 3) \
                                                AS budgetPowerGeneration, \
                    Round(Sum(referencepowergeneration * mpptinstcapacity) / Sum( \
                            mpptinstcapacity), \
                    3) \
                    AS referencePowerGeneration, \
                    Round(Sum(stationpowergeneration * mpptinstcapacity) / Sum( \
                            mpptinstcapacity), 3) \
                                                AS stationPowerGeneration, \
                    Round(Sum(predictpowergeneration * mpptinstcapacity) / Sum( \
                            mpptinstcapacity), 3) \
                                                AS predictPowerGeneration, \
                    realirradiation, \
                    realpaneltemperature \
                FROM   processplatform.solarMpptPowerGeneration AS a, \
                    uiplatform.TInverterMppt AS b \
                WHERE  a.inverterid = b.inverterid \
                    AND a.mpptid = b.mpptid \
                    AND ts >= '{processStart}' \
                    AND ts < '{processEnd}' \
                GROUP  BY a.inverterid, ts \
            "
            solarInvPowerGeneration = pd.read_sql(solarInv_sql, con=processplatform_engine)
            if insert:
                solarInvPowerGeneration.to_sql('solarInvPowerGeneration', con=ui_85_engine, if_exists='append', index=False)
            print(f"Time in solarInvPowerGeneration {time.time() - startP2}")
            
            startP2_1 = time.time()
            #insert into group
            # insert into solarGroupPowerGeneration
            solarGroup_sql = f"\
                SELECT \
                    ts,\
                    a.siteId,\
                    a.groupId,\
                    ROUND(SUM(realPowerGeneration * instCapacity) / SUM(instCapacity),\
                            3) AS realPowerGeneration,\
                    ROUND(SUM(budgetPowerGeneration * instCapacity) / SUM(instCapacity),\
                            3) AS budgetPowerGeneration,\
                    ROUND(SUM(referencePowerGeneration * instCapacity) / SUM(instCapacity),\
                            3) AS referencePowerGeneration,\
                    ROUND(SUM(stationPowerGeneration * instCapacity) / SUM(instCapacity),\
                            3) AS stationPowerGeneration,\
                    ROUND(SUM(predictPowerGeneration * instCapacity) / SUM(instCapacity),\
                            3) AS predictPowerGeneration,\
                    realIrradiation,\
                    realPanelTemperature\
                FROM\
                    processplatform.solarInvPowerGeneration AS a,\
                    uiplatform.TSiteInverter AS b\
                WHERE\
                    a.siteId = b.siteId\
                        AND a.groupId = b.groupId\
                        AND a.inverterId = b.inverterId\
                        AND ts >= '{processStart}'\
                        AND ts < '{processEnd}'\
                GROUP BY a.groupId , a.siteId , ts\
            "
            solarGroupPowerGeneration = pd.read_sql(solarGroup_sql, con=processplatform_engine)
            if insert:
                solarGroupPowerGeneration.to_sql('solarGroupPowerGeneration', con=ui_85_engine, if_exists='append', index=False)
            print(f"Time in solarGroupPowerGeneration {time.time() - startP2_1}")

            startP2_2 = time.time()
            #insert into site
            # insert into solarSitePowerGeneration
            solarSite_sql = f"\
                SELECT \
                    ts,\
                    a.siteId,\
                    ROUND(SUM(realPowerGeneration * instCapacity) / SUM(instCapacity),\
                            3) AS realPowerGeneration,\
                    ROUND(SUM(budgetPowerGeneration * instCapacity) / SUM(instCapacity),\
                            3) AS budgetPowerGeneration,\
                    ROUND(SUM(referencePowerGeneration * instCapacity) / SUM(instCapacity),\
                            3) AS referencePowerGeneration,\
                    ROUND(SUM(stationPowerGeneration * instCapacity) / SUM(instCapacity),\
                            3) AS stationPowerGeneration,\
                    ROUND(SUM(predictPowerGeneration * instCapacity) / SUM(instCapacity),\
                            3) AS predictPowerGeneration,\
                    realIrradiation,\
                    realPanelTemperature\
                FROM\
                    processplatform.solarInvPowerGeneration AS a,\
                    uiplatform.TSiteInverter AS b\
                WHERE\
                    a.siteId = b.siteId\
                        AND a.groupId = b.groupId\
                        AND a.inverterId = b.inverterId\
                        AND ts >= '{processStart}'\
                        AND ts < '{processEnd}'\
                GROUP BY a.siteId , ts\
            "
            solarSitePowerGeneration = pd.read_sql(solarSite_sql, con=processplatform_engine)
            if insert:
                solarSitePowerGeneration.to_sql('solarSitePowerGeneration', con=ui_85_engine, if_exists='append', index=False)
            print(f"Time in solarSitePowerGeneration {time.time() - startP2_2}")

            print("solarInvPowerGeneration / solarGroupPowerGeneration / solarSitePowerGeneration insert successfully.")
            success = 0
        else:
            return "solarMpptPowerGeneration inserts unsuccessfully."
        ## ======================== end processing ======================== ##

        ## ======================== start processing [part 3] ======================== ##
        if success == 0:
            success = dailySolarPowerGeneration.update(processStart, processEnd, processplatform_engine, uiplatform_engine, reportplatform_engine, ncpu=ncpu, insert=insert)
        else:
            return "solarInvPowerGeneration, solarGroupPowerGeneration and solarSitePowerGeneration insert unsuccessfully."
        
        if success == 0:
            monthlySolarPowerGeneration.update(currentTS, reportplatform_engine, ncpu=ncpu, insert=insert)

        ## ======================== end processing ======================== ##

        ## ======================== start processing [part 4] ======================== ##
        if success == 0:
            success = dailySolarRevenue.update(currentTS, processplatform_engine, reportplatform_engine, ncpu=ncpu, insert=insert)
        else:
            return "dailySolarPowerGeneration inserts unsuccessfully."
        
        if success == 0:
            monthlySolarRevenue.update(currentTS, reportplatform_engine, insert=insert)
        ## ======================== end processing ======================== ##

        return 0
    
    finally:
        try:
            uiplatform_engine.dispose()
            historyDataCWB_engine.dispose()
            archiveplatform_engine.dispose()
            dataplatform_engine.dispose()
            processplatform_engine.dispose()
            ui_85_engine.dispose()
            reportplatform_engine.dispose()
        except:
            pass

if __name__ == "__main__":
    # currentTS = datetime.datetime.now() - datetime.timedelta(minutes=10)
    currentTS = datetime.datetime(2020, 7, 13, 9, 3)
    re = main(currentTS, debug=True, insert=False)
    print(re)
