import sqlalchemy as sql
import pandas as pd
import numpy as np
import copy
import time
import datetime
import math
import json
import argparse

def main(dataDate, insert=None):

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
    processplatform_engine = sql.create_engine(f"mysql+mysqldb://{user}:{pwd}@{host}/{dbProcessPF}?charset=utf8", pool_recycle=3600*7)
    uiplatform_engine = sql.create_engine(f"mysql+mysqldb://{user}:{pwd}@{host}/{dbUi}?charset=utf8", pool_recycle=3600*7)
    reportplatform_engine = sql.create_engine(f"mysql+mysqldb://{user}:{pwd}@{host}/{dbRPF}?charset=utf8", pool_recycle=3600*7)
    prices = pd.read_sql(f"SELECT id, sellToGridPrice FROM uiplatform.TSite where deleteFlag = 0", con=processplatform_engine)
    vRealPowerGenerationPerHour = pd.read_sql_table("vRealPowerGenerationPerHour", con=reportplatform_engine)
    vReferencePowerGenerationPerHour = pd.read_sql_table("vReferencePowerGenerationPerHour", con=reportplatform_engine)
    vBudgetPowerGenerationPerHour = pd.read_sql_table("vBudgetPowerGenerationPerHour", con=reportplatform_engine)
    vSiteDailyPowerGeneration = pd.read_sql(f"call spSiteDailyPowerGeneration_Date('{dataDate}')", con=reportplatform_engine)

    dailySolarRevenue_dict = {"operationDate": [],"siteId": [],"realRevenue": [],"realRevenuePerHour": [],"budgetRevenue": [],"budgetRevenuePerHour": [],"referenceRevenue": [],"referenceRevenuePerHour": []}
    
    for ind, (sId, sellToGridPrice) in prices.iterrows():
        sId = int(sId)
        print(f"Processing site {sId}.")
    
        realPowerGenerationData = vRealPowerGenerationPerHour[(vRealPowerGenerationPerHour["siteId"]==sId) & (vRealPowerGenerationPerHour["operationDate"]==dataDate)]
        refPowerGenerationData = vReferencePowerGenerationPerHour[(vReferencePowerGenerationPerHour["siteId"]==sId) & (vReferencePowerGenerationPerHour["operationDate"]==dataDate)]
        budgetPowerGenerationData = vBudgetPowerGenerationPerHour[(vBudgetPowerGenerationPerHour["siteId"]==sId) & (vBudgetPowerGenerationPerHour["operationDate"]==dataDate)]

        jsonObj = {"06H":{}, "07H":{}, "08H":{}, "09H":{}, "10H":{}, "11H":{}, "12H":{}, "13H":{}, "14H":{}, "15H":{}, "16H":{}, "17H":{}, "18H":{}, "19H":{}}
        realRevenuePerHour = copy.deepcopy(jsonObj)
        budgetRevenuePerHour = copy.deepcopy(jsonObj)
        referenceRevenuePerHour = copy.deepcopy(jsonObj)

        for key in realRevenuePerHour.keys():
            realRevenuePerHour[key] = {"data": round(((realPowerGenerationData[key]/1000)*sellToGridPrice).values[0], 3) }
        realRevenuePerHour = json.dumps(realRevenuePerHour)

        for key in budgetRevenuePerHour.keys():
            budgetRevenuePerHour[key] = {"data": round(((budgetPowerGenerationData[key]/1000)*sellToGridPrice).values[0], 3) }
        budgetRevenuePerHour = json.dumps(budgetRevenuePerHour)

        for key in referenceRevenuePerHour.keys():
            referenceRevenuePerHour[key] = {"data": round(((refPowerGenerationData[key]/1000)*sellToGridPrice).values[0], 3) }
        referenceRevenuePerHour = json.dumps(referenceRevenuePerHour)

        revenueData = vSiteDailyPowerGeneration[(vSiteDailyPowerGeneration["siteId"]==sId)]

        realRevenue = 0 if pd.isna(revenueData["sumRealPowerGeneration"].values[0]) else int(round(((revenueData["sumRealPowerGeneration"]/1000)*sellToGridPrice).values[0]))
        budgetRevenue = int(round(((revenueData["sumBudgetPowerGeneration"]/1000)*sellToGridPrice).values[0]))
        referenceRevenue = int(round(((revenueData["sumReferencePowerGeneration"]/1000)*sellToGridPrice).values[0]))

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
        df.to_sql('dailySolarRevenue', con=reportplatform_engine, if_exists='append', index=False)

        print("dailySolarRevenue insert successfully.")

    processplatform_engine.dispose()
    uiplatform_engine.dispose()
    reportplatform_engine.dispose()

    return 0

if __name__ == "__main__":
    dataDate = "2020-08-15"
    main(dataDate, insert=True)

    # sites = [1,2,3,4,5,6,7,15,16]
    # dD = [ "2020-08-04"]

    # for dataDate in range(20, 32):
    #     print(dataDate)
    #     for sId in sites:
    #         main(sId, f'2020-07-{dataDate}', insert=True)
