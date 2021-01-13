import pandas as pd
import datetime
import sqlalchemy as sql
import math

def getInverterMpptInfo(engine):
    assert engine is not None
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
                and g.deleteFlag=0\
        ORDER BY siteId\
        "
    InverterMpptInfo = pd.read_sql_query(sql.text(sql_query), con=engine)
    
    if InverterMpptInfo.shape[0] == 0:
        return "Inverter Mppt Info is NULL [SQL] " + sql_query
    else:
        return InverterMpptInfo

def getTSite(engine):
    assert engine is not None
    tbl = pd.read_sql_table("TSite", con=engine)
    return tbl

def getTPanelModel(engine):
    assert engine is not None
    tbl = pd.read_sql_table("TPanelModel", con=engine)
    return tbl

def getHistoryDataCWB(engine, dbCWBNum):
    assert engine is not None
    tbl = pd.read_sql_table(f"historyDataCWB{dbCWBNum}", con=engine)
    return tbl

def getVSiteDevice(engine):
    assert engine is not None
    tbl = pd.read_sql_table("TCategory", con=engine)
    return tbl

def getEnvironment(engine, dataDate):
    assert engine is not None
    env_sql = f"SELECT * FROM dataplatform.environment where receivedSync >= '{dataDate}' order by receivedSync desc"
    tbl = pd.read_sql_query(sql.text(env_sql), con=engine)
    return tbl

def getPanelEfficiencySet(sId, dataDate, TSite, TPanelModel):
    # 取案場建置完成時間
    siteDate = TSite.loc[TSite["id"]==sId].instDate.values[0]

    # 未滿一年以1年計算
    del_day = math.ceil((datetime.datetime.strptime(dataDate, "%Y-%m-%d") - datetime.datetime.utcfromtimestamp(siteDate.tolist()/1e9)).days / 365)

    if del_day <=0:
        return "del_day <= 0"
    tmpTb = TSite.merge(TPanelModel, left_on="solarPanelModelId", right_on="id")
    _panelEfficiencySet = (tmpTb.loc[tmpTb["id_x"]==sId].efficiencyPerYear.values[0])[str(del_day)]["data"]
    if _panelEfficiencySet is None or "":
        return "panelEfficiencySet is empty!"
    else:
        print(f"panelEfficiencySet = {_panelEfficiencySet}")
        return _panelEfficiencySet

def getSolarInverter(engine, processStart):
    start = processStart - datetime.timedelta(minutes=31)
    solarInverter_sql = f"SELECT * FROM dataplatform.solarInverter WHERE receivedSync >= '{start}'"
    solarInverter = pd.read_sql_query(solarInverter_sql, con=engine)
    if solarInverter.shape[0] == 0:
        return "solarInverter is NULL [SQL] " + solarInverter_sql
    else:
        return solarInverter

def getSolarMpptPowerGeneration(engine, processStart, processEnd):
    sql = f"SELECT * FROM processplatform.solarMpptPowerGeneration where ts >= '{processStart}' and ts <= '{processEnd}'"
    solarMpptPowerGeneration = pd.read_sql_query(sql, con=engine)
    return solarMpptPowerGeneration

def getvSiteDailyPowerGeneration_RealtimeSQL():
    return f"\
        SELECT \
            `a`.`operationDate` AS `operationDate`,\
            `a`.`siteId` AS `siteId`,\
            `c`.`instCapacity` AS `siteInstCapacity`,\
            ROUND(SUM((`a`.`realPowerGeneration` * `b`.`instCapacity`)),\
                    3) AS `sumRealPowerGeneration`,\
            ROUND(SUM((`a`.`budgetPowerGeneration` * `b`.`instCapacity`)),\
                    3) AS `sumBudgetPowerGeneration`,\
            ROUND(SUM((`a`.`referencePowerGeneration` * `b`.`instCapacity`)),\
                    3) AS `sumReferencePowerGeneration`,\
            ROUND(SUM((`a`.`predictPowerGeneration` * `b`.`instCapacity`)),\
                    3) AS `sumPredictPowerGeneration`,\
            ROUND(SUM((`a`.`stationPowerGeneration` * `b`.`instCapacity`)),\
                    3) AS `sumStationPowerGeneration`,\
            `a`.`realPanelTemperature` AS `realPanelTemperature`,\
            `a`.`realIrradiation` AS `realIrradiation`\
        FROM\
            (((SELECT \
                `processplatform`.`dailySolarPowerGeneration`.`operationDate` AS `operationDate`,\
                    `processplatform`.`dailySolarPowerGeneration`.`siteId` AS `siteId`,\
                    `processplatform`.`dailySolarPowerGeneration`.`groupId` AS `groupId`,\
                    `processplatform`.`dailySolarPowerGeneration`.`inverterId` AS `inverterId`,\
                    `processplatform`.`dailySolarPowerGeneration`.`inverterDescription` AS `inverterDescription`,\
                    `processplatform`.`dailySolarPowerGeneration`.`realPowerGeneration` AS `realPowerGeneration`,\
                    `processplatform`.`dailySolarPowerGeneration`.`realPowerGenerationPerHour` AS `realPowerGenerationPerHour`,\
                    `processplatform`.`dailySolarPowerGeneration`.`budgetPowerGeneration` AS `budgetPowerGeneration`,\
                    `processplatform`.`dailySolarPowerGeneration`.`budgetPowerGenerationPerHour` AS `budgetPowerGenerationPerHour`,\
                    `processplatform`.`dailySolarPowerGeneration`.`referencePowerGeneration` AS `referencePowerGeneration`,\
                    `processplatform`.`dailySolarPowerGeneration`.`referencePowerGenerationPerHour` AS `referencePowerGenerationPerHour`,\
                    `processplatform`.`dailySolarPowerGeneration`.`predictPowerGeneration` AS `predictPowerGeneration`,\
                    `processplatform`.`dailySolarPowerGeneration`.`predictPowerGenerationPerHour` AS `predictPowerGenerationPerHour`,\
                    `processplatform`.`dailySolarPowerGeneration`.`stationPowerGeneration` AS `stationPowerGeneration`,\
                    `processplatform`.`dailySolarPowerGeneration`.`stationPowerGenerationPerHour` AS `stationPowerGenerationPerHour`,\
                    `processplatform`.`dailySolarPowerGeneration`.`realIrradiation` AS `realIrradiation`,\
                    `processplatform`.`dailySolarPowerGeneration`.`realIrradiationPerHour` AS `realIrradiationPerHour`,\
                    `processplatform`.`dailySolarPowerGeneration`.`realPanelTemperature` AS `realPanelTemperature`,\
                    `processplatform`.`dailySolarPowerGeneration`.`realPanelTemperaturePerHour` AS `realPanelTemperaturePerHour`\
            FROM\
                `processplatform`.`dailySolarPowerGeneration`) `a`\
            JOIN `uiplatform`.`TSiteInverter` `b`)\
            JOIN `uiplatform`.`TSite` `c`)\
        WHERE\
            ((`a`.`inverterId` = `b`.`inverterId`)\
                AND (`a`.`siteId` = `c`.`id`)\
                AND (`b`.`siteId` = `c`.`id`))\
        GROUP BY `a`.`siteId` , `a`.`operationDate`;\
    "