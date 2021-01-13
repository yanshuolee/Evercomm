import sqlalchemy as sql
import pandas as pd
import numpy as np
import copy
import time
import datetime
import math
import json
import argparse

def main(sId, dataDate, insert=None):

    if sId is None:
        raise Exception("Please specify a siteId to continue.")
    if dataDate is None:
        raise Exception("Please specify date to continue.")

    todayStart = f"{dataDate} 05:00"
    todayEnd = f"{dataDate} 19:00"

    #IP
    host = "localhost"
    user = "ecoetl"
    pwd = "ECO4etl"

    #DB name
    dbUi = "uiplatform"
    dbProcessPF = "processplatform"
    dbRPF = "reportplatform"

    # Engine
    print("Loading tables.")
    processplatform_engine = sql.create_engine(f"mysql+mysqldb://{user}:{pwd}@{host}/{dbProcessPF}", pool_recycle=3600*7)
    uiplatform_engine = sql.create_engine(f"mysql+mysqldb://{user}:{pwd}@{host}/{dbUi}", pool_recycle=3600*7)
    reportplatform_engine = sql.create_engine(f"mysql+mysqldb://{user}:{pwd}@{host}/{dbRPF}", pool_recycle=3600*7)
    sellToGridPrice = pd.read_sql(f"SELECT sellToGridPrice FROM uiplatform.TSite where id={sId}", con=processplatform_engine).values[0][0]
    vRealPowerGenerationPerHour = pd.read_sql_table("vRealPowerGenerationPerHour", con=reportplatform_engine)
    vSiteDailyPowerGeneration = pd.read_sql_table("vSiteDailyPowerGeneration", con=reportplatform_engine)

    realPowerGenerationData = vRealPowerGenerationPerHour[(vRealPowerGenerationPerHour["siteId"]==sId) & (vRealPowerGenerationPerHour["operationDate"]==dataDate)]

    jsonObj = {"06H":{}, "07H":{}, "08H":{}, "09H":{}, "10H":{}, "11H":{}, "12H":{}, "13H":{}, "14H":{}, "15H":{}, "16H":{}, "17H":{}, "18H":{}, "19H":{}}
    realRevenuePerHour = copy.deepcopy(jsonObj)

    for key in realRevenuePerHour.keys():
        realRevenuePerHour[key] = {"data": round(((realPowerGenerationData[key]/1000)*sellToGridPrice).values[0], 3) }
    realRevenuePerHour = json.dumps(realRevenuePerHour)

    budgetRevenuePerHour = copy.deepcopy(realRevenuePerHour)
    referenceRevenuePerHour = copy.deepcopy(realRevenuePerHour)

    revenueData = vSiteDailyPowerGeneration[(vSiteDailyPowerGeneration["siteId"]==sId) & (vSiteDailyPowerGeneration["operationDate"]==dataDate)]

    realRevenue = int(round(((revenueData["sumRealPowerGeneration"]/1000)*sellToGridPrice).values[0]))
    budgetRevenue = int(round(((revenueData["sumBudgetPowerGeneration"]/1000)*sellToGridPrice).values[0]))
    referenceRevenue = int(round(((revenueData["sumReferencePowerGeneration"]/1000)*sellToGridPrice).values[0]))

    dailySolarRevenue_dict = {"operationDate": [],"siteId": [],"realRevenue": [],"realRevenuePerHour": [],"budgetRevenue": [],"budgetRevenuePerHour": [],"referenceRevenue": [],"referenceRevenuePerHour": []}

    dailySolarRevenue_dict["operationDate"].append(dataDate)
    dailySolarRevenue_dict["siteId"].append(sId)
    dailySolarRevenue_dict["realRevenue"].append(realRevenue)
    dailySolarRevenue_dict["realRevenuePerHour"].append(realRevenuePerHour)
    dailySolarRevenue_dict["budgetRevenue"].append(budgetRevenue)
    dailySolarRevenue_dict["budgetRevenuePerHour"].append(budgetRevenuePerHour)
    dailySolarRevenue_dict["referenceRevenue"].append(referenceRevenue)
    dailySolarRevenue_dict["referenceRevenuePerHour"].append(referenceRevenuePerHour)

    df = pd.DataFrame(data=dailySolarRevenue_dict)

    if insert:
        ui_85_engine = sql.create_engine(f'mysql+mysqldb://{user}:{pwd}@{host}/{dbRPF}', pool_recycle=3600*7)
        df.to_sql('dailySolarRevenue', con=ui_85_engine, if_exists='append', index=False)

    print("dailySolarRevenue insert successfully.")

    return 0

if __name__ == "__main__":
    sId = 5
    dataDate = "2020-06-07"
    main(sId, dataDate, insert=False)
