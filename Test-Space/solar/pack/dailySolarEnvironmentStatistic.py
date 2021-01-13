import sqlalchemy as sql
import pandas as pd
import numpy as np
import copy
import time
import datetime
import json
import sys, traceback

def cal(uiplatform_engine, dataDate, insert=None):
    TSite = pd.read_sql_query(sql.text("SELECT id, siteName, CWBId FROM uiplatform.TSite where deleteFlag = 0"), con=uiplatform_engine)
    CWBStationWeather = pd.read_sql_query(sql.text(f"SELECT receivedSync, stationId, date_format(receivedSync, '%k')+1 as _hour, radiationKW, precipitation, temperature, windDirection, windSpeed FROM dataplatform.CWBStationWeather where receivedSync like '{dataDate}%'"), con=uiplatform_engine)
    
    stationIdList = np.unique(TSite["CWBId"].to_numpy())
    dailySolarEnvironmentStatistic_dict = {"operationDate": [],"siteId": [],"predictIrradiation": [],"predictIrradiationPerHour": [],
                                           "stationIrradiation": [],"stationIrradiationPerHour": [],"predictTemperture": [],"predictTemperturePerHour": [],
                                           "stationTemperature": [],"stationTemperaturePerHour": [],"rainfall": [],"rainfallPerHour": [],
                                           "windSpeedPerHour": [], "windDirectionPerHour": []}

    for stationId in stationIdList:
        jsonObj = {"stationIrradiationPerHour":{}, "rainfallPerHour":{}, "stationTemperaturePerHour":{}, "windDirectionPerHour":{}, "windSpeedPerHour":{}}
        tbl = CWBStationWeather[(CWBStationWeather["stationId"] == stationId) & (CWBStationWeather["_hour"] >= 6) & (CWBStationWeather["_hour"] <= 19)].sort_values(by=["_hour"])

        for ind, data in tbl.iterrows():
            hour_str = f"{int(data['_hour']):02}H"
            jsonObj["stationIrradiationPerHour"][hour_str] = {"data": data['radiationKW'] if not pd.isna(data['radiationKW']) else None}
            jsonObj["rainfallPerHour"][hour_str] = {"data": data['precipitation'] if not pd.isna(data['precipitation']) else None}
            jsonObj["stationTemperaturePerHour"][hour_str] = {"data": data['temperature'] if not pd.isna(data['temperature']) else None}
            jsonObj["windDirectionPerHour"][hour_str] = {"data": data['windDirection'] if not pd.isna(data['windDirection']) else None}
            jsonObj["windSpeedPerHour"][hour_str] = {"data": data['windSpeed'] if not pd.isna(data['windSpeed']) else None}

        for ind, data in TSite[TSite["CWBId"] == stationId].iterrows():
            dailySolarEnvironmentStatistic_dict["operationDate"].append(dataDate)
            dailySolarEnvironmentStatistic_dict["siteId"].append(data["id"])
            dailySolarEnvironmentStatistic_dict["predictIrradiation"].append(None)
            dailySolarEnvironmentStatistic_dict["predictIrradiationPerHour"].append(None)
            dailySolarEnvironmentStatistic_dict["stationIrradiation"].append(None)
            dailySolarEnvironmentStatistic_dict["stationIrradiationPerHour"].append(json.dumps(jsonObj["stationIrradiationPerHour"]))
            dailySolarEnvironmentStatistic_dict["predictTemperture"].append(None)
            dailySolarEnvironmentStatistic_dict["predictTemperturePerHour"].append(None)
            dailySolarEnvironmentStatistic_dict["stationTemperature"].append(None)
            dailySolarEnvironmentStatistic_dict["stationTemperaturePerHour"].append(json.dumps(jsonObj["stationTemperaturePerHour"]))
            dailySolarEnvironmentStatistic_dict["rainfall"].append(None)
            dailySolarEnvironmentStatistic_dict["rainfallPerHour"].append(json.dumps(jsonObj["rainfallPerHour"]))
            dailySolarEnvironmentStatistic_dict["windSpeedPerHour"].append(json.dumps(jsonObj["windSpeedPerHour"]))
            dailySolarEnvironmentStatistic_dict["windDirectionPerHour"].append(json.dumps(jsonObj["windDirectionPerHour"]))
    
    df = pd.DataFrame(data=dailySolarEnvironmentStatistic_dict)

    if insert:
        try:
            df.to_sql('dailySolarEnvironmentStatistic', con=uiplatform_engine, if_exists='append', index=False)
            print("[Table Insert] dailySolarEnvironmentStatistic")
        except:
            traceback.print_exc(file=sys.stdout)
    
    return 0

if __name__ == "__main__":
    dataDate = "2020-08-13"
    #IP
    host = "localhost"
    user = "ecoetl"
    pwd = "ECO4etl"

    #DB name
    dbUi = "reportplatform"

    uiplatform_engine = sql.create_engine(f"mysql+mysqldb://{user}:{pwd}@{host}/{dbUi}?charset=utf8", pool_recycle=3600*7)
    for i in range(1, 18):
        cal(uiplatform_engine, f"2020-08-{i:02}", insert=True)
