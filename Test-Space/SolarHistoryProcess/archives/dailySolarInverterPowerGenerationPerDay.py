import sqlalchemy as sql
import pandas as pd
import numpy as np
import copy
import time
import datetime
import math
import json
import argparse
import sys

def main(sId, dataDate, insert=None):
    
    if sId is None:
        raise Exception("Please specify a siteId to continue.")
    if len(dataDate) != 7:
        raise Exception("Wrong format. Example: 2020-05")

    #IP
    host = "202.73.49.62:33236"
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
    processplatform_engine = sql.create_engine(f"mysql+mysqldb://{user}:{pwd}@{host}/{dbProcessPF}", pool_recycle=3600*7)
    uiplatform_engine = sql.create_engine(f"mysql+mysqldb://{user}:{pwd}@{host}/{dbUi}", pool_recycle=3600*7)
    reportplatform_engine = sql.create_engine(f'mysql+mysqldb://{user}:{pwd}@{host}/{dbRPF}', pool_recycle=3600*7)

    # Table
    print("Loading Tables.")
    # TSiteInverter_sql = f"SELECT inverterId FROM uiplatform.TSiteInverter where siteId={sId}"
    TSiteInverter_sql = f"SELECT inverterId FROM uiplatform.TSiteInverter"
    TSiteInverter = pd.read_sql(TSiteInverter_sql, con=uiplatform_engine)
    dailySolarPowerGeneration_sql = f"""SELECT * FROM reportplatform.dailySolarPowerGeneration where operationDate like '{dataDate}%'"""
    dailySolarPowerGeneration = pd.read_sql(sql.text(dailySolarPowerGeneration_sql), con=reportplatform_engine)
    
    # Inverter
    monthlySolarPowerGeneration_dict = {"operationDate": [],"siteId": [],"groupId": [],"inverterId": [],"inverterDescription": [],"realPowerGeneration": [],"budgetPowerGeneration": [],"referencePowerGeneration": [], "predictPowerGeneration": [],"stationPowerGeneration":[], "realIrradiation": [], "realPanelTemperature": []}
    for ind, _InvID in TSiteInverter.iterrows():
        InvID = _InvID[0]
        print(f"Processing Inv {InvID}.")

        tbl = dailySolarPowerGeneration[dailySolarPowerGeneration["inverterId"]==InvID]
        if tbl.size == 0:
            print(f"Inverter {InvID} has no data.")

        realPowerGeneration = tbl["realPowerGeneration"].mean()
        budgetPowerGeneration = tbl["budgetPowerGeneration"].mean()
        referencePowerGeneration = tbl["referencePowerGeneration"].mean()
        predictPowerGeneration = tbl["predictPowerGeneration"].mean()
        stationPowerGeneration = tbl["stationPowerGeneration"].mean()
        realIrradiation = tbl["realIrradiation"].mean()
        realPanelTemperature = tbl["realPanelTemperature"].mean()
       
        monthlySolarPowerGeneration_dict["operationDate"].append(dataDate+"-01")
        monthlySolarPowerGeneration_dict["siteId"].append(sId)
        monthlySolarPowerGeneration_dict["groupId"].append(tbl["groupId"].values[0])
        monthlySolarPowerGeneration_dict["inverterId"].append(InvID)
        monthlySolarPowerGeneration_dict["inverterDescription"].append(tbl["inverterDescription"].values[0])
        monthlySolarPowerGeneration_dict["realPowerGeneration"].append(round(realPowerGeneration, 3))
        monthlySolarPowerGeneration_dict["budgetPowerGeneration"].append(round(budgetPowerGeneration, 3))
        monthlySolarPowerGeneration_dict["referencePowerGeneration"].append(round(referencePowerGeneration, 3))
        monthlySolarPowerGeneration_dict["predictPowerGeneration"].append(round(predictPowerGeneration, 3))
        monthlySolarPowerGeneration_dict["stationPowerGeneration"].append(round(stationPowerGeneration, 3))
        monthlySolarPowerGeneration_dict["realIrradiation"].append(round(realIrradiation, 3))
        monthlySolarPowerGeneration_dict["realPanelTemperature"].append(round(realPanelTemperature, 3))

    df = pd.DataFrame(data=monthlySolarPowerGeneration_dict)

    if insert:
        ui_85_engine = sql.create_engine(f'mysql+mysqldb://{user}:{pwd}@{host}/{dbRPF}', pool_recycle=3600*7)
        df.to_sql('monthlySolarPowerGeneration', con=ui_85_engine, if_exists='append', index=False)

    print("monthlySolarPowerGeneration insert successfully.")

    return 0

if __name__ == "__main__":
    sId = 5
    dataDate = "2020-06"
    main(sId, dataDate, insert=False)
