import sqlalchemy as sql
import pandas as pd
import numpy as np
import copy
import time
import datetime
import math
import json
import argparse

def getMPPTNum(_id):
    if _id == "A":
        mpptNum = 1
        mpptCurrentNum1 = 1
        mpptCurrentNum2 = 2
    elif _id == "B":
        mpptNum = 2
        mpptCurrentNum1 = 3
        mpptCurrentNum2 = 4
    elif _id == "C":
        mpptNum = 3
        mpptCurrentNum1 = 5
        mpptCurrentNum2 = 6
    elif _id == "D":
        mpptNum = 4
        mpptCurrentNum1 = 7
        mpptCurrentNum2 = 8
    
    return mpptNum, mpptCurrentNum1, mpptCurrentNum2

def main(currentTS, debug=None, insert=None):
    siteID = [15, 16]
    totalTime = 0

    #IP
    host = "localhost"
    user = "admin"
    pwd = "Admin99"

    #DB name
    dbData = "dataplatform"
    dbUi = "uiplatform"
    dbRPF = "reportplatform"
    dbARC = "archiveplatform"
    dbProcessPF = "processplatform"
    dbCWB = "historyDataCWB"

    # Engine
    uiplatform_engine = sql.create_engine(f"mysql+mysqldb://{user}:{pwd}@{host}/{dbUi}?charset=utf8", pool_recycle=3600*7)
    historyDataCWB_engine = sql.create_engine(f"mysql+mysqldb://{user}:{pwd}@{host}/{dbCWB}?charset=utf8", pool_recycle=3600*7)
    archiveplatform_engine = sql.create_engine(f"mysql+mysqldb://{user}:{pwd}@{host}/{dbARC}?charset=utf8", pool_recycle=3600*7)
    dataplatform_engine = sql.create_engine(f"mysql+mysqldb://{user}:{pwd}@{host}/{dbData}?charset=utf8", pool_recycle=3600*7)
    processplatform_engine = sql.create_engine(f"mysql+mysqldb://{user}:{pwd}@{host}/{dbProcessPF}", pool_recycle=3600*7)
    ui_85_engine = sql.create_engine(f'mysql+mysqldb://{user}:{pwd}@{host}/{dbProcessPF}?charset=utf8', pool_recycle=3600*7)

    # Table
    env_sql = "SELECT receivedSync FROM dataplatform.environment order by receivedSync desc limit 1"
    environment_last_time = pd.read_sql_query(env_sql, con=dataplatform_engine)
    environment_last_minute = environment_last_time.receivedSync.dt.minute[0]
    environment_last_time = environment_last_time.receivedSync[0]
    solarInverter_sql = "SELECT receivedSync FROM dataplatform.solarInverter order by receivedSync desc limit 1"
    solarInverter_last_time = pd.read_sql(solarInverter_sql, con=dataplatform_engine)
    solarInverter_last_minute = solarInverter_last_time.receivedSync.dt.minute[0]
    solarInverter_last_time = solarInverter_last_time.receivedSync[0]

    # currentTS = datetime.datetime.now()
    currentTS_min = currentTS.minute

    if not debug:
        if currentTS_min % 2 != 0:
            return 1
    
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

    todayStart = currentTS.replace(second=0) - pd.Timedelta(minutes=2)
    todayEnd = todayStart + pd.Timedelta(minutes=2)

    TSite = pd.read_sql_table("TSite", con=uiplatform_engine)
    TPanelModel = pd.read_sql_table("TPanelModel", con=uiplatform_engine)
    historyDataCWB_dbCWBNum = pd.read_sql_table(f"historyDataCWB{dbCWBNum}", con=historyDataCWB_engine)
    VSiteDevice = pd.read_sql_table("VSiteDevice", con=uiplatform_engine)
    # environment_dbARCNum = pd.read_sql_table(f"environment{dbARCNum}", con=archiveplatform_engine)
    env_sql = f"""SELECT * FROM dataplatform.environment where receivedSync like '{dataDate}%' """
    environment_dbARCNum = pd.read_sql_query(sql.text(env_sql), con=dataplatform_engine)
    TSiteInverter = pd.read_sql_table("TSiteInverter", con=uiplatform_engine)
    TInverterMppt = pd.read_sql_table("TInverterMppt", con=uiplatform_engine)
    TSiteBuildList = pd.read_sql_table("TSiteBuildList", con=uiplatform_engine)
    TInverterModel = pd.read_sql_table("TInverterModel", con=uiplatform_engine)
    TLogicDevice = pd.read_sql_table("TLogicDevice", con=dataplatform_engine)
    TDevice = pd.read_sql_table("TDevice", con=dataplatform_engine)

    # loop through all sites
    for sId in siteID:
        traceStart = time.time()
        print(f"Processing site {sId}")

        _cityId = TSite[TSite["id"]==sId]["cityId"].values[0]
        if _cityId is None or "":
            return f"CityID {_cityId} is empty!"
            raise Exception(f"CityID is empty!") 
            
        # 太陽能板瓦特溫升負係數 panelTempSet=0.0044
        tmpTb = TSite.merge(TPanelModel, left_on="solarPanelModelId", right_on="id")
        _panelTempSet = tmpTb.loc[tmpTb["id_x"]==sId].MaxPowerTemp.values[0]
        if _panelTempSet is None or "":
            return "panelTempSet is empty!"
            raise Exception(f"panelTempSet is empty!")
        else:
            print(f"panelTempSet = {_panelTempSet}")
        
        # 取案場建置完成時間
        siteDate = TSite.loc[TSite["id"]==sId].instDate.values[0]

        # 未滿一年以1年計算
        del_day = math.ceil((datetime.datetime.strptime(dataDate, "%Y-%m-%d") - datetime.datetime.utcfromtimestamp(siteDate.tolist()/1e9)).days / 365)

        # 太陽能板衰減率
        if del_day <=0:
            return "del_day <= 0"
        tmpTb = TSite.merge(TPanelModel, left_on="solarPanelModelId", right_on="id")
        _panelEfficiencySet = (tmpTb.loc[tmpTb["id_x"]==sId].efficiencyPerYear.values[0])[str(del_day)]["data"]
        if _panelEfficiencySet is None or "":
            return "panelEfficiencySet is empty!"
            raise Exception(f"panelEfficiencySet is empty!")
        else:
            print(f"panelEfficiencySet = {_panelEfficiencySet}")
            
        historyDataCWB_data = historyDataCWB_dbCWBNum.loc[historyDataCWB_dbCWBNum["ts"] >= dbCWBStartTime].loc[dbCWBEndTime >= historyDataCWB_dbCWBNum["ts"]]
        historyDataCWB_data = historyDataCWB_data.loc[historyDataCWB_data["cityId"]==_cityId]
        if historyDataCWB_data.shape[0] == 0:
            return f"No data in historyDataCWB{dbCWBNum}"
            raise Exception(f"No data in historyDataCWB{dbCWBNum}")
        else:
            print(f"historyDataCWB{dbCWBNum} has data with length {historyDataCWB_data.shape[0]}")

        # 日照
        _insolationIEEE = VSiteDevice.loc[VSiteDevice["deviceCategoryId"]==2].loc[VSiteDevice["siteId"]==sId]["ieee"].values[0]
        if _insolationIEEE is None or "":
            return f"insolationIEEE is empty!"
            raise Exception(f"insolationIEEE is empty!")
        else:
            print(f"insolationIEEE = {_insolationIEEE}")

        environment = environment_dbARCNum.loc[environment_dbARCNum["receivedSync"] >= todayStart].loc[todayEnd >= environment_dbARCNum["receivedSync"]]
        environment_insolationIEEE = environment.loc[environment_dbARCNum["ieee"]==_insolationIEEE]
        if environment_insolationIEEE.shape[0] == 0:
            return env_sql+" IEEE = "+_insolationIEEE
            raise Exception(f"No data in environment{dbARCNum}: insolationIEEE")
        else:
            print(f"environment{dbARCNum} has data with length {environment_insolationIEEE.shape[0]}")

        # 面板溫度
        _moduleTempIEEE = VSiteDevice.loc[VSiteDevice["deviceCategoryId"]==4].loc[VSiteDevice["siteId"]==sId]["ieee"].values[0]
        if _moduleTempIEEE is None or "":
            return "moduleTempIEEE is empty!"
            raise Exception(f"moduleTempIEEE is empty!")
        else:
            print(f"moduleTempIEEE = {_moduleTempIEEE}")


        environment_moduleTempIEEE = environment.loc[environment_dbARCNum["ieee"]==_moduleTempIEEE]
        if environment_moduleTempIEEE.shape[0] == 0:
            return env_sql+" IEEE "+_moduleTempIEEE
            raise Exception(f"No data in environment{dbARCNum}: moduleTempIEEE")
        else:
            print(f"environment{dbARCNum} has data with length {environment_moduleTempIEEE.shape[0]}")

        # Inverter Info
        sql_query = f" SELECT a.siteId, a.groupId, a.inverterId, b.mpptId, b.mpptInstCapacity, c.azimuth, c.inclination, d.efficiency, f.ieee, f.deviceDesc FROM  uiplatform.TSiteInverter as a, uiplatform.TInverterMppt as b, uiplatform.TSiteBuildList as c, uiplatform.TInverterModel as d, dataplatform.TLogicDevice as e, dataplatform.TDevice as f where a.inverterId=b.inverterId and b.mpptPanelAzimuthId=c.azimuthId and b.mpptPanelInclinationId=c.inclinationId and a.invModelId=d.id and a.logicDeviceId=e.id and e.deviceId=f.id and a.siteId={sId}"

        InverterMpptInfo = pd.read_sql_query(sql_query, con=uiplatform_engine)
        if InverterMpptInfo.shape[0] == 0:
            return "Inverter Mppt Info is no NULL [SQL ]"+sql_query
            raise Exception(f"Inverter Mppt Info is no NULL")


        solarMpptPowerGeneration_dict = {"ts": [],"siteId": [],"groupId": [],"inverterId": [],"inverterDescription": [],"mpptId": [],"realPowerGeneration": [],"budgetPowerGeneration": [],"referencePowerGeneration": [],"stationPowerGeneration": [],"predictPowerGeneration": [],"realIrradiation": [],"realPanelTemperature": []}

        traceStart = time.time()
        for ind, InvInfo in InverterMpptInfo.iterrows():
            mpptNum, mpptCurrentNum1, mpptCurrentNum2 = getMPPTNum(InvInfo["mpptId"])
            print(f"[{ind}] Inv Info: groupID:{InvInfo['groupId']} Inv:{InvInfo['inverterId']} MPPT:{InvInfo['mpptId']}({mpptNum}) Capacity:{InvInfo['mpptInstCapacity']} azimuth:{InvInfo['azimuth']} inclination:{InvInfo['inclination']} Inverter ieee:{InvInfo['ieee']} deviceDesc:{InvInfo['deviceDesc']}")
            
            # 確認inverter data
            solarInverter_sql = f"SELECT * FROM solarInverter WHERE ieee='{InvInfo['ieee']}'"
            solarInverter_dbARCNum = pd.read_sql(solarInverter_sql, con=dataplatform_engine) # Table容量大
            if solarInverter_dbARCNum.shape[0] == 0:
                return "No data in solarInverter!"+" [SQL] "+solarInverter_sql
                raise Exception(f"No data in solarInverter!")
            else:
                print(f"solarInverter{dbARCNum}({InvInfo['ieee']}) has data with length {solarInverter_dbARCNum.shape[0]}")
            
            realIrradiation = 0
            realPanelTemperature = 0

            ## ======================== start processing ======================== ##
            processStart = todayStart
            processEnd = todayEnd
            print(processStart)

            # ====日照 realIrradiation ==== #
            realIrradiation_list = environment_dbARCNum[(environment_dbARCNum["receivedSync"] >= processStart) & (processEnd >= environment_dbARCNum["receivedSync"]) & (environment_dbARCNum["ieee"]==_insolationIEEE)].groupby(["receivedSync"]).mean()["value5"].values
            if realIrradiation_list.size == 0:
                # realIrradiation = 0
                pass
            else:
                realIrradiation = round(np.average(realIrradiation_list)/1000,3)
                
            # ==== 實際溫度 realPanelTemperature ==== #
            realPanelTemperature_list = environment_dbARCNum[(environment_dbARCNum["receivedSync"] >= processStart) & (processEnd >= environment_dbARCNum["receivedSync"]) & (environment_dbARCNum["ieee"]==_moduleTempIEEE)].groupby(["receivedSync"]).mean()["value3"].values
            if realPanelTemperature_list.size == 0:
                pass
                # realPanelTemperature = 0
            else:
                realPanelTemperature = round(np.average(realPanelTemperature_list), 3)

            # ==== 預算發電量 budgetPowerGeneration ==== #
            tmp = historyDataCWB_dbCWBNum[(historyDataCWB_dbCWBNum["ts"] >= processStart.replace(year=2020)) & (processEnd.replace(year=2020) >= historyDataCWB_dbCWBNum["ts"]) & (historyDataCWB_dbCWBNum["cityId"] == _cityId)]
            
            _azimuth = tmp["azimuth"].values[0]
            _inclination = tmp["inclination"].values[0]
            _budtTemp = tmp["budtTemp"].values[0]
            _budtDirectInsolation = tmp["budtDirectInsolation"].values[0]
            _azimuthInsolation = round(_budtDirectInsolation*math.cos((_azimuth-InvInfo['azimuth'])*0.0175),3)

            if _azimuthInsolation > 0:
                _budgetAzimuthInclinationInsolation =             math.sqrt(
                math.pow(_azimuthInsolation*math.sin((_inclination+InvInfo['inclination'])*0.0175),2)
                +
                math.pow(_budtDirectInsolation*math.cos((_azimuth-InvInfo['azimuth']-90)*0.0175)*math.sin(_inclination*0.0175)*math.cos(InvInfo['inclination']*0.0175),2)
                )
            else:
                _budgetAzimuthInclinationInsolation =             math.sqrt(
                math.pow(_azimuthInsolation*math.sin((_inclination-InvInfo['inclination'])*0.0175),2)
                +
                math.pow(_budtDirectInsolation*math.cos((_azimuth-InvInfo['azimuth']-90)*0.0175)*math.sin(_inclination*0.0175)*math.cos(InvInfo['inclination']*0.0175),2)
                )

            budgetPowerGeneration = round((_budgetAzimuthInclinationInsolation*_panelEfficiencySet)*(1-(_budgetAzimuthInclinationInsolation*25.3/0.8+_budtTemp-25)*_panelTempSet)*0.9835*InvInfo['efficiency'], 3)

            # ==== 案場參考發電量 referencePowerGeneration ==== #
            tmp = historyDataCWB_dbCWBNum[(historyDataCWB_dbCWBNum["ts"] >= processStart.replace(year=2020)) & (processEnd.replace(year=2020) >= historyDataCWB_dbCWBNum["ts"]) & (historyDataCWB_dbCWBNum["cityId"] == _cityId)]
            
            _refDirectInsolation = round(tmp["oneSIN"]*realIrradiation, 3).values[0]
            _referenceAzimuthIrradiation = round((tmp["oneSIN"].values[0]*realIrradiation)*math.cos((tmp["azimuth"].values[0]-InvInfo["azimuth"])*0.0175),3)
            
            if _referenceAzimuthIrradiation > 0:
                _refAzimuthInclinationInsolation =             math.sqrt(
                math.pow(_referenceAzimuthIrradiation*math.sin((tmp["inclination"].values[0]+InvInfo["inclination"])*0.0175), 2)
                +\
                math.pow(_refDirectInsolation*math.cos((tmp["azimuth"].values[0]-InvInfo["azimuth"]-90)*0.0175)*math.sin(tmp["inclination"].values[0]*0.0175)*math.cos(InvInfo["inclination"]*0.0175), 2)
                )
            else:
                _refAzimuthInclinationInsolation =             math.sqrt(
                math.pow(_referenceAzimuthIrradiation*math.sin((tmp["inclination"].values[0]-InvInfo["inclination"])*0.0175), 2)
                +\
                math.pow(_refDirectInsolation*math.cos((tmp["azimuth"].values[0]-InvInfo["azimuth"]-90)*0.0175)*math.sin(tmp["inclination"].values[0]*0.0175)*math.cos(InvInfo["inclination"]*0.0175), 2)
                )

            referencePowerGeneration = round((_refAzimuthInclinationInsolation*_panelEfficiencySet)*(1-(realPanelTemperature-25)*_panelTempSet)*0.9835*InvInfo['efficiency'], 3)
            
            # ==== 實際發電量 realPowerGeneration ==== #
            tmp = solarInverter_dbARCNum[(solarInverter_dbARCNum["receivedSync"] >= processStart) & (processEnd >= solarInverter_dbARCNum["receivedSync"]) & (solarInverter_dbARCNum["ieee"]==InvInfo['ieee'])]
            if tmp.shape[0] == 0:
                realPowerGeneration = 0
            else:
                dc_list = []
                realKw = []
                for crt in tmp["dcCurrent"]:
                    if isinstance(crt, str):
                        crt = json.loads(crt)
                    dc_list.append(crt[f"current{mpptCurrentNum1}"] + crt[f"current{mpptCurrentNum2}"])
                avg_dc = sum(dc_list)/len(dc_list)

                for vtg in tmp["dcVoltage"]:
                    if isinstance(vtg, str):
                        vtg = json.loads(vtg)
                    realKw.append(vtg[f"voltage{mpptNum}"] * avg_dc)
                avg_realKw = sum(realKw)/len(realKw)
                realPowerGeneration = round(avg_realKw / InvInfo['mpptInstCapacity'], 3)

            #觀測站發電量 stationPowerGeneration
            tmp = historyDataCWB_dbCWBNum[(historyDataCWB_dbCWBNum["ts"] >= processStart.replace(year=2020)) & (processEnd.replace(year=2020) >= historyDataCWB_dbCWBNum["ts"]) & (historyDataCWB_dbCWBNum["cityId"] == _cityId)]
            
            _stationAzimuthIrradiation = round(tmp["refDirectInsolation"].values[0]*math.cos((tmp["azimuth"].values[0]-InvInfo["azimuth"])*0.0175), 3)
            
            if _stationAzimuthIrradiation > 0:
                _stationAzimuthInclinationInsolation =             math.sqrt(
                math.pow(_stationAzimuthIrradiation*math.sin((tmp["inclination"].values[0]+InvInfo['inclination'])*0.0175), 2)
                +
                math.pow(tmp["refDirectInsolation"].values[0]*math.cos((tmp["azimuth"].values[0]-InvInfo['azimuth']-90)*0.0175)*math.sin(tmp["inclination"].values[0]*0.0175)*math.cos(InvInfo['inclination']*0.0175), 2)
                )
            else:
                _stationAzimuthInclinationInsolation =             math.sqrt(
                math.pow(_stationAzimuthIrradiation*math.sin((tmp["inclination"].values[0]-InvInfo['inclination'])*0.0175), 2)
                +
                math.pow(tmp["refDirectInsolation"].values[0]*math.cos((tmp["azimuth"].values[0]-InvInfo['azimuth']-90)*0.0175)*math.sin(tmp["inclination"].values[0]*0.0175)*math.cos(InvInfo['inclination']*0.0175), 2)
                )

            stationPowerGeneration = round((_stationAzimuthInclinationInsolation*_panelEfficiencySet)*(1-(_stationAzimuthInclinationInsolation*25.3/0.8+tmp["refTemp"].values[0]-25)*_panelTempSet)*0.9835*InvInfo['efficiency'], 3)
            
            # ==== 預測發電量 predictPowerGeneration ==== #
            # realKW
            tmp = solarInverter_dbARCNum[(solarInverter_dbARCNum["receivedSync"] >= processStart-datetime.timedelta(minutes=30)) & (processStart-datetime.timedelta(minutes=16) > solarInverter_dbARCNum["receivedSync"])]
            if tmp.shape[0] == 0:
                _realKW = 0.0
            else:
                dc_list = []
                realKw = []
                for crt in tmp["dcCurrent"]:
                    if isinstance(crt, str):
                        crt = json.loads(crt)
                    dc_list.append(crt[f"current{mpptCurrentNum1}"] + crt[f"current{mpptCurrentNum2}"])
                avg_dc = sum(dc_list)/len(dc_list)

                for vtg in tmp["dcVoltage"]:
                    if isinstance(vtg, str):
                        vtg = json.loads(vtg)
                    realKw.append(vtg[f"voltage{mpptNum}"] * avg_dc)
                avg_realKw = sum(realKw)/len(realKw)
                _realKW = round(avg_realKw / InvInfo['mpptInstCapacity'], 3)
            
            # preOneSIN
            tmp = historyDataCWB_dbCWBNum[(historyDataCWB_dbCWBNum["ts"] >= (processStart-datetime.timedelta(minutes=30)).replace(year=2020)) & ((processStart-datetime.timedelta(minutes=16)).replace(year=2020) > historyDataCWB_dbCWBNum["ts"]) & (historyDataCWB_dbCWBNum["cityId"] == _cityId)]
            if tmp.shape[0] == 0:
                _preOneSIN = 0.0
            else:
                _preOneSIN = np.average(tmp["oneSIN"].values)
            
            # postOneSIN
            tmp = historyDataCWB_dbCWBNum[(historyDataCWB_dbCWBNum["ts"] >= processStart.replace(year=2020)) & ((processStart+datetime.timedelta(minutes=16)).replace(year=2020) > historyDataCWB_dbCWBNum["ts"]) & (historyDataCWB_dbCWBNum["cityId"] == _cityId)]
            if tmp.shape[0] == 0:
                _postOneSIN = 0.0
            else:
                _postOneSIN = np.average(tmp["oneSIN"].values)
            
            # final
            if _postOneSIN == 0:
                predictPowerGeneration = 0.0
            else:
                predictPowerGeneration = round((_realKW * _preOneSIN) / _postOneSIN, 3)

            # insert to dataframe
            solarMpptPowerGeneration_dict["ts"].append(processEnd - datetime.timedelta(minutes=1))
            solarMpptPowerGeneration_dict["siteId"].append(sId)
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
        
        # 先跑一個inv
        #break
        df = pd.DataFrame(data=solarMpptPowerGeneration_dict)

        success = -1
        if insert:
            df.to_sql('solarMpptPowerGeneration', con=ui_85_engine, if_exists='append', index=False)
            success = 0

        tranceEnd = time.time()
        timeDiff = tranceEnd-traceStart
        totalTime += timeDiff
        print(f"Time in Looping [site {sId}]: {timeDiff} sec.")

    ## ======================== end processing ======================== ##

    if success == 0:
        print("Start processing.")
        #insert into Inverter

        for sId in siteID:
            # insert into solarInvPowerGeneration
            solarInv_sql = f"SELECT ts,   a.siteId,   a.groupId,   a.inverterId,   inverterDescription,   round(sum(realPowerGeneration*mpptInstCapacity)/sum(mpptInstCapacity),3) as realPowerGeneration,   round(sum(budgetPowerGeneration*mpptInstCapacity)/sum(mpptInstCapacity),3) as budgetPowerGeneration,   round(sum(referencePowerGeneration*mpptInstCapacity)/sum(mpptInstCapacity),3)  as referencePowerGeneration,   round(sum(stationPowerGeneration*mpptInstCapacity)/sum(mpptInstCapacity),3) as stationPowerGeneration,   round(sum(predictPowerGeneration*mpptInstCapacity)/sum(mpptInstCapacity),3) as predictPowerGeneration,   realIrradiation,   realPanelTemperature FROM     processplatform.solarMpptPowerGeneration as a,     uiplatform.TInverterMppt as b where     a.inverterId=b.inverterId and     a.mpptId=b.mpptId and     a.siteId={sId} and     ts>='{todayStart}' and     ts<'{todayEnd}' group by a.inverterId, ts"
            solarInvPowerGeneration = pd.read_sql(solarInv_sql, con=processplatform_engine)
            if insert:
                solarInvPowerGeneration.to_sql('solarInvPowerGeneration', con=ui_85_engine, if_exists='append', index=False)
                
            #insert into group
            # insert into solarGroupPowerGeneration
            solarGroup_sql = f"SELECT ts,     a.siteId,     a.groupId,     round(sum(realPowerGeneration*instCapacity)/sum(instCapacity),3) as realPowerGeneration,     round(sum(budgetPowerGeneration*instCapacity)/sum(instCapacity),3) as budgetPowerGeneration,     round(sum(referencePowerGeneration*instCapacity)/sum(instCapacity),3)  as referencePowerGeneration,     round(sum(stationPowerGeneration*instCapacity)/sum(instCapacity),3) as stationPowerGeneration,     round(sum(predictPowerGeneration*instCapacity)/sum(instCapacity),3) as predictPowerGeneration,     realIrradiation,     realPanelTemperature FROM     processplatform.solarInvPowerGeneration as a,     uiplatform.TSiteInverter as b where     a.siteId=b.siteId and     a.groupId=b.groupId and     a.inverterId=b.inverterId and     a.siteId={sId} and     ts>='{todayStart}' and     ts<'{todayEnd}' group by a.groupId, ts"
            solarGroupPowerGeneration = pd.read_sql(solarGroup_sql, con=processplatform_engine)
            if insert:
                solarGroupPowerGeneration.to_sql('solarGroupPowerGeneration', con=ui_85_engine, if_exists='append', index=False)

            #insert into site
            # insert into solarSitePowerGeneration
            solarSite_sql = f"SELECT ts,     a.siteId,     round(sum(realPowerGeneration*instCapacity)/sum(instCapacity),3) as realPowerGeneration,     round(sum(budgetPowerGeneration*instCapacity)/sum(instCapacity),3) as budgetPowerGeneration,     round(sum(referencePowerGeneration*instCapacity)/sum(instCapacity),3)  as referencePowerGeneration,     round(sum(stationPowerGeneration*instCapacity)/sum(instCapacity),3) as stationPowerGeneration,     round(sum(predictPowerGeneration*instCapacity)/sum(instCapacity),3) as predictPowerGeneration,     realIrradiation,     realPanelTemperature FROM     processplatform.solarInvPowerGeneration as a,     uiplatform.TSiteInverter as b WHERE     a.siteId=b.siteId and     a.groupId=b.groupId and     a.inverterId=b.inverterId and     a.siteId={sId} and     ts>='{todayStart}' and     ts<'{todayEnd}' group by a.siteId,ts"
            solarSitePowerGeneration = pd.read_sql(solarSite_sql, con=processplatform_engine)
            if insert:
                solarSitePowerGeneration.to_sql('solarSitePowerGeneration', con=ui_85_engine, if_exists='append', index=False)

        print("solarInvPowerGeneration / solarGroupPowerGeneration / solarSitePowerGeneration insert successfully.")
    
    else:
        return "solarMpptPowerGeneration inserted, but not solarInvPowerGeneration, solarGroupPowerGeneration, solarSitePowerGeneration"
    
    return 0

if __name__ == "__main__":
    currentTS = datetime.datetime(2020, 6, 17, 18, 17)
    flag = main(currentTS, debug=True, insert=False)
    print(f"Flag: {flag}")
