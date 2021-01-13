import sqlalchemy as sql
import pandas as pd
import numpy as np
import copy
import time
import datetime
import math
import json
try:
    import initialization
except:
    from . import initialization

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

    realPowerGen = vRealPowerGenerationPerHour[["siteId", hour_str]].merge(sellToGridPrice_dict, left_on=["siteId"], right_on=["id"])
    refPowerGen = vReferencePowerGenerationPerHour[["siteId", hour_str]].merge(sellToGridPrice_dict, left_on=["siteId"], right_on=["id"])
    bugtPowerGen = vBudgetPowerGenerationPerHour[["siteId", hour_str]].merge(sellToGridPrice_dict, left_on=["siteId"], right_on=["id"])

    realPowerGen = pd.eval("realRevenuePerHour = realPowerGen[hour_str]/1000*realPowerGen['sellToGridPrice']", target=realPowerGen)
    refPowerGen = pd.eval("budgetRevenuePerHour = refPowerGen[hour_str]/1000*refPowerGen['sellToGridPrice']", target=refPowerGen)
    bugtPowerGen = pd.eval("referenceRevenuePerHour = bugtPowerGen[hour_str]/1000*bugtPowerGen['sellToGridPrice']", target=bugtPowerGen)

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
    return insertString

def update(currentTS, processplatform_engine, reportplatform_engine, insert=False):
    s = time.time()
    dataDate = currentTS.strftime('%Y-%m-%d')
    conn = processplatform_engine.connect()
    sellToGridPrice = pd.read_sql(f"SELECT id, sellToGridPrice FROM uiplatform.TSite where deleteFlag = 0", con=processplatform_engine)
    vRealPowerGenerationPerHour = pd.read_sql(f"SELECT * FROM reportplatform.vRealPowerGenerationPerHour_Realtime", con=reportplatform_engine)
    vReferencePowerGenerationPerHour = pd.read_sql(f"SELECT * FROM reportplatform.vReferencePowerGenerationPerHour_Realtime", con=reportplatform_engine)
    vBudgetPowerGenerationPerHour = pd.read_sql(f"SELECT * FROM reportplatform.vBudgetPowerGenerationPerHour_Realtime", con=reportplatform_engine)
    vSiteDailyPowerGeneration = pd.read_sql(f"SELECT * FROM reportplatform.vSiteDailyPowerGeneration_Realtime where operationDate = '{dataDate}' ", con=reportplatform_engine)
    dailySR = pd.read_sql(f"SELECT * FROM processplatform.dailySolarRevenue where operationDate = '{dataDate}'", con=reportplatform_engine)
    insertKey = ""

    if dailySR.shape[0] == 0:
        insertString = startHour(currentTS, sellToGridPrice, vRealPowerGenerationPerHour, vSiteDailyPowerGeneration)
    else:
        insertString = continueHour(currentTS, sellToGridPrice, vRealPowerGenerationPerHour, vSiteDailyPowerGeneration, dailySR, vReferencePowerGenerationPerHour, vBudgetPowerGenerationPerHour)
    
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
    
    update(currentTS, processplatform_engine, reportplatform_engine, insert=False)