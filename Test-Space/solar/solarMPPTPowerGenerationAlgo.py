import sqlalchemy as sql
import pandas as pd
import numpy as np
import copy
import time
import datetime
import math
import json
import argparse
import sys, traceback
import os

def getMPPTNum(_id):
    if _id == "A":
        mpptNum = 1
    elif _id == "B":
        mpptNum = 2
    elif _id == "C":
        mpptNum = 3
    elif _id == "D":
        mpptNum = 4
    return mpptNum

def seriesToDict(series, mpptNum):
    def getValue(arr, ind):
        return arr[ind]
    x = series.apply(json.loads)
    y = x.apply(dict.values)
    z = y.apply(list)
    i = z.apply(np.array)
    return i.apply(getValue, args=(mpptNum-1,))

def main(date, insert=None):
    d = date.split("-")
    year = d[0]
    month = d[1]
    day = d[2]

    if year is None:
        raise Exception("Please specify year to continue.")
    if month is None:
        raise Exception("Please specify month to continue.")
    if day is None:
        raise Exception("Please specify day to continue.")

    dbARCNum = f"{year}{month}"

    dbCWBNum = f"{month}"
    dbCWBDate = f"{month}-{day}"

    dbCWBStartTime = f"{year}-{month}-{day} 05:00"
    dbCWBStartTime = datetime.datetime.strptime(dbCWBStartTime, "%Y-%m-%d %H:%M")
    dbCWBEndTime = f"{year}-{month}-{day} 19:00"
    dbCWBEndTime = datetime.datetime.strptime(dbCWBEndTime, "%Y-%m-%d %H:%M")

    dataDate = f"{year}-{month}-{day}"
    todayStart_str = f"{dataDate} 05:00"
    todayStart = datetime.datetime.strptime(todayStart_str, "%Y-%m-%d %H:%M")
    todayEnd_str = f"{dataDate} 19:00"
    todayEnd = datetime.datetime.strptime(todayEnd_str, "%Y-%m-%d %H:%M")

    print(f"Running MPPT data on {dataDate}")

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

    # Engine
    uiplatform_engine = sql.create_engine(f"mysql+mysqldb://{user}:{pwd}@{host}/{dbUi}?charset=utf8", pool_recycle=3600*7)
    historyDataCWB_engine = sql.create_engine(f"mysql+mysqldb://{user}:{pwd}@{host}/{dbCWB}?charset=utf8", pool_recycle=3600*7)
    archiveplatform_engine = sql.create_engine(f"mysql+mysqldb://{user}:{pwd}@{host}/{dbARC}?charset=utf8", pool_recycle=3600*7)
    dataplatform_engine = sql.create_engine(f"mysql+mysqldb://{user}:{pwd}@{host}/{dbData}?charset=utf8", pool_recycle=3600*7)

    #暫時engine
    temp_engine = sql.create_engine(f"mysql+mysqldb://{user}:{pwd}@202.73.49.62:44406/{dbUi}?charset=utf8", pool_recycle=3600*7)

    # Table
    TSite = pd.read_sql_table("TSite", con=temp_engine) # TSite = pd.read_sql_table("TSite", con=uiplatform_engine)
    TPanelModel = pd.read_sql_table("TPanelModel", con=uiplatform_engine)
    # historyDataCWB_dbCWBNum = pd.read_sql_table(f"historyDataCWB{dbCWBNum}", con=historyDataCWB_engine)
    historyDataCWB_dbCWBNum = pd.read_sql_query(sql.text(f"SELECT * FROM historyDataCWB.historyDataCWB{dbCWBNum} where ts like '2020-{dbCWBDate}%'"), con=historyDataCWB_engine)
    VSiteDevice = pd.read_sql_table("VSiteDevice", con=uiplatform_engine)
    # environment_dbARCNum = pd.read_sql_table(f"environment{dbARCNum}", con=archiveplatform_engine)
    environment_dbARCNum = pd.read_sql_query(sql.text(f"select * from environment where receivedSync >= '{dataDate} 05:02' and receivedSync <= '{dataDate} 19:00'"), con=dataplatform_engine)
    # env_sql = f"""SELECT * FROM archiveplatform.environment where receivedSync like '{dataDate}%' """
    # environment_dbARCNum = pd.read_sql_query(sql.text(env_sql), con=archiveplatform_engine)
    TSiteInverter = pd.read_sql_table("TSiteInverter", con=uiplatform_engine)
    TInverterMppt = pd.read_sql_table("TInverterMppt", con=uiplatform_engine)
    TSiteBuildList = pd.read_sql_table("TSiteBuildList", con=uiplatform_engine)
    TInverterModel = pd.read_sql_table("TInverterModel", con=uiplatform_engine)
    TLogicDevice = pd.read_sql_table("TLogicDevice", con=dataplatform_engine)
    TDevice = pd.read_sql_table("TDevice", con=dataplatform_engine)

    # 測試 site 5,6,7,15,16
    # Inverter Info
    sql_query = \
        "\
            SELECT \
                a.siteId,\
                a.groupId,\
                a.inverterId,\
                b.mpptId,\
                b.mpptInstCapacity,\
                c.azimuth,\
                c.inclination,\
                d.efficiency,\
                f.ieee,\
                f.deviceDesc,\
                d.id AS invTypeId,\
                g.cityId\
            FROM\
                uiplatform.TSiteInverter AS a,\
                uiplatform.TInverterMppt AS b,\
                uiplatform.TSiteBuildList AS c,\
                uiplatform.TInverterModel AS d,\
                dataplatform.TLogicDevice AS e,\
                dataplatform.TDevice AS f,\
                uiplatform.TSite AS g\
            WHERE\
                a.inverterId = b.inverterId\
                    AND b.mpptPanelAzimuthId = c.azimuthId\
                    AND b.mpptPanelInclinationId = c.inclinationId\
                    AND a.invModelId = d.id\
                    AND a.logicDeviceId = e.id\
                    AND e.deviceId = f.id\
                    AND g.id = a.siteId\
                    and a.siteId in (5,6,7,15,16)\
            ORDER BY siteId\
        "

    InverterMpptInfo = pd.read_sql_query(sql_query, con=uiplatform_engine)
    if InverterMpptInfo.shape[0] == 0:
        return "Inverter Mppt Info is no NULL [SQL ]"+sql_query
        raise Exception(f"Inverter Mppt Info is no NULL")
    
    ## TODO
    # 可測試放這邊會不會比較快
    #
    # 確認inverter data
    # solarInverter_sql = f"SELECT * FROM solarInverter WHERE receivedSync like '{dataDate}%'"
    # s = time.time()
    # solarInverter_dbARCNum = pd.read_sql(sql.text(solarInverter_sql), con=dataplatform_engine) # Table容量大
    # print(f"solarInverter_dbARCNum load time {time.time()-s} sec")
    # if solarInverter_dbARCNum.shape[0] == 0:
    #     raise Exception("No data in solarInverter!"+" [SQL] "+solarInverter_sql)
    # else:
    #     print(f"solarInverter{dbARCNum} has data with length {solarInverter_dbARCNum.shape[0]}")

    solarMpptPowerGeneration_dict = {"ts": [],"siteId": [],"groupId": [],"inverterId": [],"inverterDescription": [],
                                     "mpptId": [],"realPowerGeneration": [],"budgetPowerGeneration": [],"referencePowerGeneration": [],
                                     "stationPowerGeneration": [],"predictPowerGeneration": [],"realIrradiation": [],"realPanelTemperature": []}

    traceStart = time.time()
    sId = None
    solarMpptPowerGenerationDF = pd.DataFrame(columns=[
                                    "ts",
                                    "siteId",
                                    "groupId",
                                    "inverterId",
                                    "inverterDescription",
                                    "mpptId",
                                    "realPowerGeneration",
                                    "budgetPowerGeneration",
                                    "referencePowerGeneration",
                                    "stationPowerGeneration",
                                    "predictPowerGeneration",
                                    "realIrradiation",
                                    "realPanelTemperature"
                                ])
    for ind, InvInfo in InverterMpptInfo.iterrows():
        print(f"Processing Inverter [ {InvInfo['ieee']} ] ")

        if sId != InvInfo["siteId"]:
            sId = InvInfo["siteId"]

            _cityId = TSite[TSite["id"]==sId]["cityId"].values[0]
            if _cityId is None or "":
                return f"CityID {_cityId} is empty!"
                raise Exception(f"CityID is empty!") 

            # CWB ID
            CWBId = TSite[TSite["id"]==sId]["CWBId"].values[0]
            # CWBData = pd.read_sql_query(sql.text(f"SELECT stationId, receivedSync, date_format(receivedSync, '%k') as _hour, temperature, radiationKW FROM dataplatform.CWBStationWeather where receivedSync like '{dataDate}%' and stationId = {CWBId}"), con=dataplatform_engine)
            CWBData = pd.read_sql_query(sql.text(f"SELECT stationId, receivedSync, date_format(receivedSync, '%k') as _hour, temperature, radiationKW FROM dataplatform.CWBStationWeather where receivedSync like '{dataDate}%' and stationId = {CWBId} and date_format(receivedSync, '%k') between 5 and 19"), con=temp_engine)
            
            # 太陽能板瓦特溫升負係數
            tmpTb = TSite.merge(TPanelModel, left_on="solarPanelModelId", right_on="id")
            _panelTempSet = tmpTb.loc[tmpTb["id_x"]==sId].MaxPowerTemp.values[0]
            if _panelTempSet is None or "":
                return "panelTempSet is empty!"
                raise Exception(f"panelTempSet is empty!")
            else:
                print(f"panelTempSet = {_panelTempSet}")
                
            siteDate = TSite.loc[TSite["id"]==sId].instDate.values[0]

            # 未滿一年以1年計算
            del_day = math.ceil((datetime.datetime.strptime(dataDate, "%Y-%m-%d") - datetime.datetime.utcfromtimestamp(siteDate.tolist()/1e9)).days / 365)

            # 太陽能板衰減率
            tmpTb = TSite.merge(TPanelModel, left_on="solarPanelModelId", right_on="id")
            _panelEfficiencySet = (tmpTb.loc[tmpTb["id_x"]==sId].efficiencyPerYear.values[0])[str(del_day)]["data"]
            if _panelEfficiencySet is None or "":
                return "panelEfficiencySet is empty!"
                raise Exception(f"panelEfficiencySet is empty!")
            else:
                print(f"panelEfficiencySet = {_panelEfficiencySet}")
            
            historyDataCWB_data = historyDataCWB_dbCWBNum.loc[historyDataCWB_dbCWBNum["cityId"]==_cityId]
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
                print("No data in environment IEEE [_insolationIEEE] = "+_insolationIEEE)
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
                print(f"IEEE [_moduleTempIEEE] {_moduleTempIEEE} is null")
            else:
                print(f"environment{dbARCNum} has data with length {environment_moduleTempIEEE.shape[0]}")

        solarInverter_sql = f"SELECT * FROM solarInverter WHERE receivedSync >= '{dataDate} 05:02' and receivedSync <= '{dataDate} 19:00' and ieee='{InvInfo['ieee']}'"
        s = time.time()
        solarInverter_dbARCNum = pd.read_sql(sql.text(solarInverter_sql), con=dataplatform_engine) # Table容量大
        print(f"solarInverter_dbARCNum load time {time.time()-s} sec")
        if solarInverter_dbARCNum.shape[0] == 0:
            print("No data in solarInverter!"+" [SQL] "+solarInverter_sql)
            continue
        else:
            print(f"solarInverter{dbARCNum} has data with length {solarInverter_dbARCNum.shape[0]}")

        mpptNum = getMPPTNum(InvInfo["mpptId"])
        print(f"[{ind}] Inv Info: groupID:{InvInfo['groupId']} Inv:{InvInfo['inverterId']} MPPT:{InvInfo['mpptId']}({mpptNum}) Capacity:{InvInfo['mpptInstCapacity']} azimuth:{InvInfo['azimuth']} inclination:{InvInfo['inclination']} Inverter ieee:{InvInfo['ieee']} deviceDesc:{InvInfo['deviceDesc']}")

        # ================== New Code ==================== #
        # ==================日照 realIrradiation ================== #
        realIrradiationArr = environment_dbARCNum[(environment_dbARCNum["ieee"]==_insolationIEEE)][["receivedSync","value5"]].reset_index()
        realIrradiationArr["value5"] = realIrradiationArr["value5"] / 1000

        # ================== 實際溫度 realPanelTemperature ================== #
        realPanelTemperatureArr = environment_dbARCNum[(environment_dbARCNum["ieee"]==_moduleTempIEEE)][["receivedSync","value3"]].reset_index()

        # ================== 預算發電量 budgetPowerGeneration ================== #
        historyCWBArr = historyDataCWB_dbCWBNum[(historyDataCWB_dbCWBNum["cityId"] == _cityId)][["ts", "azimuth", "inclination", "budtTemp", "budtDirectInsolation"]]
        _azimuthInsolationArr = historyCWBArr["budtDirectInsolation"] * ((historyCWBArr["azimuth"]-InvInfo['azimuth'])*0.0175).apply(np.cos)

        greaterInd = _azimuthInsolationArr[_azimuthInsolationArr>0].index
        lessInd = _azimuthInsolationArr[_azimuthInsolationArr<=0].index

        x1 = historyCWBArr.loc[greaterInd, "inclination"] + InvInfo['inclination']
        x2 = historyCWBArr.loc[lessInd, "inclination"] - InvInfo['inclination']
        x = pd.concat([x1,x2]).sort_index()

        y1 = (((x*0.0175).apply(np.sin))*_azimuthInsolationArr).apply(np.square)
        y2_1 = ((historyCWBArr["azimuth"]-InvInfo['azimuth']-90)*0.0175).apply(np.cos)
        y2_2 = (historyCWBArr["inclination"]*0.0175).apply(np.sin)
        y2_3 = math.cos(InvInfo['inclination']*0.0175)
        y2 = (historyCWBArr["budtDirectInsolation"] * y2_1 * y2_2 * y2_3).apply(np.square)
        _budgetAzimuthInclinationInsolation = (y1+y2).apply(np.sqrt)

        budgetPowerGeneration = round((_budgetAzimuthInclinationInsolation*_panelEfficiencySet)*(1-(_budgetAzimuthInclinationInsolation*25.3/0.8+historyCWBArr["budtTemp"]-25)*_panelTempSet)*0.9835*InvInfo['efficiency'], 3)
        budgetPowerGeneration = pd.DataFrame(data={"ts":historyCWBArr["ts"], "budgetPowerGeneration":budgetPowerGeneration}).reset_index()

        # ================== 實際發電量 realPowerGeneration ================== #
        # solarArr = solarInverter_dbARCNum[(solarInverter_dbARCNum["ieee"]==InvInfo['ieee'])][["receivedSync", "dcVoltage", "dcCurrent"]]
        vtg_crt = (solarInverter_dbARCNum[(solarInverter_dbARCNum["ieee"]==InvInfo['ieee'])][["dcVoltage", "dcCurrent"]])
        vtg_crt_x = vtg_crt[~ pd.isna(vtg_crt["dcVoltage"])]
        if vtg_crt_x.shape[0] != 0:
            vtg_crt_x = vtg_crt_x.apply(seriesToDict, args=(mpptNum,))       
            dcPower = pd.eval("power = ((vtg_crt_x.dcVoltage * vtg_crt_x.dcCurrent) / InvInfo['mpptInstCapacity']) * InvInfo['efficiency']", target=vtg_crt_x)
            vtg_crt_x = dcPower.replace((dcPower[dcPower>1.5]), value=np.nan)["power"]

            vtg_crt_y = pd.Series(data=np.nan, index=vtg_crt[pd.isna(vtg_crt["dcVoltage"])].index)
            realPowerGeneration = pd.concat([vtg_crt_x, vtg_crt_y]).sort_index()
        else:
            realPowerGeneration = pd.Series.fillna(vtg_crt["dcVoltage"], np.nan)

        realPowerGeneration = pd.DataFrame(data={"receivedSync":solarInverter_dbARCNum[(solarInverter_dbARCNum["ieee"]==InvInfo['ieee'])]["receivedSync"],
                                                   "realPowerGeneration":realPowerGeneration}).reset_index()

        # ================== 案場參考發電量 referencePowerGeneration ================== #
        historyCWBArr = historyDataCWB_dbCWBNum[(historyDataCWB_dbCWBNum["cityId"] == _cityId)][["ts", "azimuth", "inclination", "oneSIN"]]
        assert historyCWBArr.shape[0] == realIrradiationArr.shape[0]
        _refDirectInsolationArr = historyCWBArr["oneSIN"].to_numpy()*realIrradiationArr["value5"].to_numpy()
        _refDirectInsolation = pd.DataFrame(data={"ts":historyCWBArr["ts"], "refDirectInsolation": _refDirectInsolationArr}) # 為了後面index
        _referenceAzimuthIrradiation = _refDirectInsolation["refDirectInsolation"] * math.cos((historyCWBArr["azimuth"].values[0]-InvInfo["azimuth"])*0.0175)

        greaterInd = _referenceAzimuthIrradiation[_referenceAzimuthIrradiation>0].index
        lessInd = _referenceAzimuthIrradiation[_referenceAzimuthIrradiation<=0].index

        x1 = historyCWBArr.loc[greaterInd, "inclination"] + InvInfo['inclination']
        x2 = historyCWBArr.loc[lessInd, "inclination"] - InvInfo['inclination']
        x = pd.concat([x1,x2]).sort_index()

        y1 = (((x*0.0175).apply(np.sin))*_referenceAzimuthIrradiation).apply(np.square)
        y2_1 = ((historyCWBArr["azimuth"]-InvInfo['azimuth']-90)*0.0175).apply(np.cos)
        y2_2 = (historyCWBArr["inclination"]*0.0175).apply(np.sin)
        y2_3 = math.cos(InvInfo['inclination']*0.0175)
        y2 = (_refDirectInsolation["refDirectInsolation"] * y2_1 * y2_2 * y2_3).apply(np.square)
        _refAzimuthInclinationInsolation = (y1+y2).apply(np.sqrt)

        referencePowerGeneration = (_refAzimuthInclinationInsolation*_panelEfficiencySet).to_numpy()*(1-(realPanelTemperatureArr["value3"]-25).to_numpy()*_panelTempSet)*0.9835*InvInfo['efficiency']
        referencePowerGeneration = pd.DataFrame({"ts":historyCWBArr["ts"], "referencePowerGeneration": referencePowerGeneration}).reset_index(drop=True)

        # ================== 觀測站發電量 stationPowerGeneration ================== #
        historyCWBArr = historyDataCWB_dbCWBNum[(historyDataCWB_dbCWBNum["cityId"] == _cityId)]

        # 暫時氣象補資料
        if CWBData.shape[0] != 15:
            DTArr = pd.date_range(start=f'{date} 05', end=f'{date} 19', freq='H')
            DT = pd.DataFrame({"receivedSync":DTArr})
            CWBData = DT.merge(CWBData, on="receivedSync", how="left")

        globRad = CWBData["radiationKW"]
        temperature = CWBData["temperature"]

        x = np.identity(15)
        y = np.repeat(x, 30, axis=0)
        mask = y[:30*14, :] # Shape = (420, 15)
        globRad_temp = globRad.to_numpy().reshape(len(globRad), 1)
        globRad = np.dot(mask, globRad_temp)
        globRad = globRad.flatten()

        temperature_temp = temperature.to_numpy().reshape(len(temperature), 1)
        temperature = np.dot(mask, temperature_temp)
        temperature = temperature.flatten()

        directInsolation = historyCWBArr["oneSIN"].to_numpy() * globRad
        _stationAzimuthIrradiation = directInsolation * ((historyCWBArr["azimuth"]-InvInfo["azimuth"])*0.0175).apply(np.cos).to_numpy()
        _stationAzimuthIrradiation = pd.DataFrame({"ts":historyCWBArr["ts"], "stationAzimuthIrradiation": _stationAzimuthIrradiation})

        greaterInd = _stationAzimuthIrradiation[_stationAzimuthIrradiation["stationAzimuthIrradiation"]>0].index
        lessInd = _stationAzimuthIrradiation[_stationAzimuthIrradiation["stationAzimuthIrradiation"]<=0].index

        x1 = historyCWBArr.loc[greaterInd, "inclination"] + InvInfo['inclination']
        x2 = historyCWBArr.loc[lessInd, "inclination"] - InvInfo['inclination']
        x = pd.concat([x1,x2]).sort_index()

        y1 = (((x*0.0175).apply(np.sin))*_stationAzimuthIrradiation["stationAzimuthIrradiation"]).apply(np.square)
        y2_1 = ((historyCWBArr["azimuth"]-InvInfo['azimuth']-90)*0.0175).apply(np.cos)
        y2_2 = (historyCWBArr["inclination"]*0.0175).apply(np.sin)
        y2_3 = math.cos(InvInfo['inclination']*0.0175)
        y2 = (directInsolation * y2_1 * y2_2 * y2_3).apply(np.square)
        _stationAzimuthInclinationInsolation = (y1+y2).apply(np.sqrt)

        stationPowerGeneration = round((_stationAzimuthInclinationInsolation*_panelEfficiencySet)*(1-(_stationAzimuthInclinationInsolation*25.3/0.8+temperature-25)*_panelTempSet)*0.9835*InvInfo['efficiency'], 3)
        oneSin = historyCWBArr["oneSIN"].to_numpy()
        stationPowerGeneration = stationPowerGeneration.to_numpy()
        stationPowerGeneration[np.where(oneSin<=0)[0]] = np.nan
        stationPowerGeneration = pd.DataFrame({"ts": historyCWBArr["ts"], "stationPowerGeneration":stationPowerGeneration}).reset_index(drop=True)

        # ================== 預測發電量 predictPowerGeneration ================== #
        # real kw
        KERNEL_SIZE = 8
        LAST_IDX = 15 # before 15 minute
        _realKW = np.array([], dtype=float)

        solarMPPTPG = realPowerGeneration["realPowerGeneration"].to_numpy()
        solarMPPTPG = np.append(np.zeros(LAST_IDX), solarMPPTPG)

        # 可以用numba加速
        for i in range(solarMPPTPG.shape[0]-LAST_IDX):
            _realKW = np.append(_realKW, np.mean(solarMPPTPG[i:i+8]))
        
        # preOneSIN
        historyCWBArr = historyDataCWB_dbCWBNum[(historyDataCWB_dbCWBNum["cityId"] == _cityId)]["oneSIN"]

        KERNEL_SIZE = 8
        LAST_IDX = 15 # before 15 minute
        _preOneSIN = np.array([], dtype=float)

        historyCWBArr_preOneSIN = historyCWBArr.to_numpy()
        historyCWBArr_preOneSIN = np.append(np.zeros(LAST_IDX), historyCWBArr_preOneSIN)

        # 可以用numba加速
        for i in range(historyCWBArr_preOneSIN.shape[0]-LAST_IDX):
            _preOneSIN = np.append(_preOneSIN, np.mean(historyCWBArr_preOneSIN[i:i+8]))

        # postOneSIN
        KERNEL_SIZE = 8
        LAST_IDX = 15 # before 15 minute
        _postOneSIN = np.array([], dtype=float)

        historyCWBArr_postOneSIN = historyCWBArr.to_numpy()
        historyCWBArr_postOneSIN = np.append(historyCWBArr_postOneSIN, np.zeros(LAST_IDX))

        # 可以用numba加速
        for i in range(historyCWBArr_postOneSIN.shape[0]-LAST_IDX):
            _postOneSIN = np.append(_postOneSIN, np.mean(historyCWBArr_postOneSIN[i:i+8]))
        _postOneSIN = np.where(_postOneSIN==0, np.nan, _postOneSIN)

        # final
        predictPowerGeneration = (_realKW * _preOneSIN) / _postOneSIN
        predictPowerGeneration = pd.DataFrame({"receivedSync": realPowerGeneration["receivedSync"], "predictPowerGeneration":predictPowerGeneration})

        # insert to dataframe
        insertDict = {
            "ts" : predictPowerGeneration["receivedSync"],
            "siteId" : InvInfo['siteId'],
            "groupId" : InvInfo['groupId'],
            "inverterId" : InvInfo['inverterId'],
            "inverterDescription" : InvInfo['deviceDesc'],
            "mpptId" : InvInfo['mpptId'],
            "realPowerGeneration" : realPowerGeneration["realPowerGeneration"],
            "budgetPowerGeneration" : budgetPowerGeneration["budgetPowerGeneration"],
            "referencePowerGeneration" : referencePowerGeneration["referencePowerGeneration"],
            "stationPowerGeneration" : stationPowerGeneration["stationPowerGeneration"],
            "predictPowerGeneration" : predictPowerGeneration["predictPowerGeneration"],
            "realIrradiation" : realIrradiationArr["value5"],
            "realPanelTemperature" : realPanelTemperatureArr["value3"]
        }

        insertDataFrame = pd.DataFrame(insertDict)
        solarMpptPowerGenerationDF = solarMpptPowerGenerationDF.append(insertDataFrame, ignore_index=True)

        # ================== New Code ==================== #
        
    print(f"Time in Looping: {time.time()-traceStart} sec.")

    df = pd.DataFrame(data=solarMpptPowerGeneration_dict)
    
    if insert:
        try:
            ui_85_engine = sql.create_engine(f'mysql+mysqldb://{user}:{pwd}@{host}/{dbARC}?charset=utf8', pool_recycle=3600*7)
            df.to_sql('solarMpptPowerGeneration', con=ui_85_engine, if_exists='append', index=False)
            print("[Table Insert] solarMpptPowerGeneration")
        except:
            currentDirectory = __file__.split(os.path.basename(__file__))[0]
            df.to_pickle(f"{currentDirectory}logs/{date}_site{sId}.pkl")
            print("solarMpptPowerGeneration to pickle")
            traceback.print_exc(file=sys.stdout)

    return 0

if __name__ == "__main__":
    date = "2020-08-26"
    main(date, insert=False)
