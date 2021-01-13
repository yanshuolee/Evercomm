import sqlalchemy as sql
import pandas as pd
import numpy as np
import copy
import time
import datetime
import math
import json
from multiprocessing import Process, Manager
try:
    import initialization, information
except:
    from . import initialization, information

manager = Manager()
return_df = manager.list()

def getOldDailySR(dailySR, site):
    dailySR_dict = dailySR[dailySR["siteId"]==site].to_dict('records')[0]
    dailySR_dict["realRevenuePerHour"] = json.loads(dailySR_dict["realRevenuePerHour"])
    dailySR_dict["budgetRevenuePerHour"] = json.loads(dailySR_dict["budgetRevenuePerHour"])
    dailySR_dict["referenceRevenuePerHour"] = json.loads(dailySR_dict["referenceRevenuePerHour"])
    return dailySR_dict

def startHour(currentTS, sellToGridPrice_dict, vRealPowerGenerationPerHour, vSiteDailyPowerGeneration):
    dataDate = currentTS.strftime('%Y-%m-%d')
    hour_str = f"{(currentTS.hour + 1):02}H"
    insertString = ""
    for site in sellToGridPrice_dict.keys():
        tempString = ""
        realPowerGenerationData = vRealPowerGenerationPerHour[vRealPowerGenerationPerHour["siteId"]==site]
        sellToGridPrice = sellToGridPrice_dict[site]["sellToGridPrice"]
        jsonObj = initialization.initdailySRJsonObj(6, 19)
        realRevenuePerHour = copy.deepcopy(jsonObj)
       
        realRevenuePerHour = json.dumps(realRevenuePerHour)
        budgetRevenuePerHour = copy.deepcopy(realRevenuePerHour)
        referenceRevenuePerHour = copy.deepcopy(realRevenuePerHour)

        revenueData = vSiteDailyPowerGeneration[vSiteDailyPowerGeneration["siteId"]==site]

        realRevenue = 0.0
        budgetRevenue = 0.0
        referenceRevenue = 0.0

        tempString = f"'{dataDate}', '{site}', '{realRevenue}', '{realRevenuePerHour}',\
                       '{budgetRevenue}', '{budgetRevenuePerHour}', '{referenceRevenue}', \
                       '{referenceRevenuePerHour}'"
        insertString += f"({tempString}) , "
    
    insertString = insertString[:-2]
    return insertString

def continueHour(currentTS, sellToGridPrice_dict, vRealPowerGenerationPerHour, vSiteDailyPowerGeneration, dailySR, vReferencePowerGenerationPerHour, vBudgetPowerGenerationPerHour):
    dataDate = currentTS.strftime('%Y-%m-%d')
    hour_str = f"{(currentTS.hour + 1):02}H"
    insertString = ""
    for site in sellToGridPrice_dict.keys():
        tempString = ""
        realPowerGenerationData = vRealPowerGenerationPerHour[vRealPowerGenerationPerHour["siteId"]==site]
        refPowerGenerationData = vReferencePowerGenerationPerHour[vReferencePowerGenerationPerHour["siteId"]==site]
        budgetPowerGenerationData = vBudgetPowerGenerationPerHour[vBudgetPowerGenerationPerHour["siteId"]==site]
        sellToGridPrice = sellToGridPrice_dict[site]["sellToGridPrice"]
        dailySR_old = getOldDailySR(dailySR, site)
    
        dailySR_old["realRevenuePerHour"][hour_str]["data"] = round(((realPowerGenerationData[hour_str]/1000)*sellToGridPrice).values[0], 3)
        dailySR_old["budgetRevenuePerHour"][hour_str]["data"] = round(((budgetPowerGenerationData[hour_str]/1000)*sellToGridPrice).values[0], 3)
        dailySR_old["referenceRevenuePerHour"][hour_str]["data"] = round(((refPowerGenerationData[hour_str]/1000)*sellToGridPrice).values[0], 3)

        revenueData = vSiteDailyPowerGeneration[vSiteDailyPowerGeneration["siteId"]==site]

        dailySR_old["realRevenue"] = 0 if pd.isna(revenueData["sumRealPowerGeneration"].values[0]) else int(round(((revenueData["sumRealPowerGeneration"]/1000)*sellToGridPrice).values[0]))
        dailySR_old["budgetRevenue"] = int(round(((revenueData["sumBudgetPowerGeneration"]/1000)*sellToGridPrice).values[0]))
        dailySR_old["referenceRevenue"] = int(round(((revenueData["sumReferencePowerGeneration"]/1000)*sellToGridPrice).values[0]))

        tempString = f"'{dataDate}', '{site}', '{dailySR_old['realRevenue']}', '{json.dumps(dailySR_old['realRevenuePerHour'])}',\
                       '{dailySR_old['budgetRevenue']}', '{json.dumps(dailySR_old['budgetRevenuePerHour'])}', '{dailySR_old['referenceRevenue']}', \
                       '{json.dumps(dailySR_old['referenceRevenuePerHour'])}'"
        insertString += f"({tempString}) , "
    
    insertString = insertString[:-2]
    return_df.append(insertString)

def update(currentTS, processplatform_engine, reportplatform_engine, ncpu, insert=False):
    s = time.time()
    dataDate = currentTS.strftime('%Y-%m-%d')
    conn = processplatform_engine.connect()
    sellToGridPrice = pd.read_sql(f"SELECT id, sellToGridPrice FROM uiplatform.TSite where deleteFlag = 0", con=processplatform_engine).set_index("id").to_dict("index")
    s1 = time.time()
    vRealPowerGenerationPerHour = pd.read_sql(f"SELECT * FROM reportplatform.vRealPowerGenerationPerHour_Realtime", con=reportplatform_engine)
    print(f"vRealPowerGenerationPerHour in {time.time() - s1}.")
    s1 = time.time()
    vReferencePowerGenerationPerHour = pd.read_sql(f"SELECT * FROM reportplatform.vReferencePowerGenerationPerHour_Realtime", con=reportplatform_engine)
    print(f"vReferencePowerGenerationPerHour in {time.time() - s1}.")
    s1 = time.time()
    vBudgetPowerGenerationPerHour = pd.read_sql(f"SELECT * FROM reportplatform.vBudgetPowerGenerationPerHour_Realtime", con=reportplatform_engine)
    print(f"vBudgetPowerGenerationPerHour in {time.time() - s1}.")
    s1 = time.time()
    vSiteDailyPowerGeneration_Realtime_sql = information.getvSiteDailyPowerGeneration_RealtimeSQL()
    vSiteDailyPowerGeneration = pd.read_sql(vSiteDailyPowerGeneration_Realtime_sql, con=reportplatform_engine)
    print(f"vSiteDailyPowerGeneration in {time.time() - s1}.")
    dailySR = pd.read_sql(f"SELECT * FROM processplatform.dailySolarRevenue where operationDate = '{dataDate}'", con=reportplatform_engine)
    insertKey = ""

    if dailySR.shape[0] == 0:
        insertString = startHour(currentTS, sellToGridPrice, vRealPowerGenerationPerHour, vSiteDailyPowerGeneration)
    else:
        # insertString = continueHour(currentTS, sellToGridPrice, vRealPowerGenerationPerHour, vSiteDailyPowerGeneration, dailySR, vReferencePowerGenerationPerHour, vBudgetPowerGenerationPerHour)
        if ncpu <= 0:
            ncpu = 1
        step = math.ceil(len(sellToGridPrice) / ncpu)
        sellToGridPriceSplit = [dict(list(sellToGridPrice.items())[i:i+step]) for i in range(0, len(sellToGridPrice), step)]

        # continueHour(currentTS, sellToGridPrice, vRealPowerGenerationPerHour, vSiteDailyPowerGeneration, dailySR, vReferencePowerGenerationPerHour, vBudgetPowerGenerationPerHour)
        
        processes = [Process(target=continueHour, args=(currentTS, processChild, vRealPowerGenerationPerHour, vSiteDailyPowerGeneration, dailySR, vReferencePowerGenerationPerHour, vBudgetPowerGenerationPerHour)) for processChild in sellToGridPriceSplit]
        pStart = [p.start() for p in processes]
        pJoin = [p.join() for p in processes]

        insertString = ",".join(return_df)

    insertKey = "`operationDate`, `siteId`, `realRevenue`, `realRevenuePerHour`, `budgetRevenue`, `budgetRevenuePerHour`, `referenceRevenue`, `referenceRevenuePerHour`"

    if insert:
        if insertString == "": return -1
        sqlstr = f"replace into processplatform.dailySolarRevenue ({insertKey}) values {insertString}"
        conn.execute(sql.text(sqlstr))
        print(f"DailySolarRevenue insert successfully in {time.time()-s} sec.")
    return 0

if __name__ == "__main__":
    currentTS = datetime.datetime(2020, 11, 3, 8, 43)
    #IP
    host = "localhost"
    user = "ecoetl"
    pwd = "ECO4etl"

    #DB name
    dbUi = "uiplatform"
    dbProcessPF = "processplatform"
    dbRPF = "reportplatform"

    # Engine
    processplatform_engine = sql.create_engine(f"mysql+mysqldb://{user}:{pwd}@{host}/{dbProcessPF}", pool_recycle=3600*7)
    reportplatform_engine = sql.create_engine(f"mysql+mysqldb://{user}:{pwd}@{host}/{dbRPF}", pool_recycle=3600*7)
    
    update(currentTS, processplatform_engine, reportplatform_engine, ncpu=4, insert=True)