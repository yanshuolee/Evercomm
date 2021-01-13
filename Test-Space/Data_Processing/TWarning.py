# -*- coding: utf-8 -*-
from sqlalchemy import create_engine
import sqlalchemy as sql
import pandas as pd
import copy
import time
from datetime import datetime
import sys, traceback
import json
import pushNotification

def checkSiteOffline(warningSignal_list, warningCount, sId):
    deviceOfflineCnt = warningSignal_list.count('2')
    siteDeviceCnt = DeviceCnt[DeviceCnt["siteId"]==sID]["DetectDeviceAmount"].values[0]
    if siteDeviceCnt == deviceOfflineCnt:
        warningCount[1] = 0
        warningCount[0] = 1
    return warningSignal_list, warningCount

js = {"errorcode1": "0002", "errorcode2": "0002", "errorcode3": "0002", "errorcode4": "0002"}
def errorCode(table, update=False):
    errorCodeInd = table[table["errorCode"].notnull()].index
    for ind in errorCodeInd:
        if table.loc[ind, "siteId"] in [15, 16]: continue # 華為先不跑
        code = []; desc = []
        logicDeviceId = table.loc[ind, "logicDeviceId"]
        errorDict = json.loads(table.loc[ind, "errorCode"]) # errorDict = js
        modelId = TSiteInverter[TSiteInverter["logicDeviceId"] == logicDeviceId]["modelId"].values[0]

        for key, val in errorDict.items():
            if val != "0":
                group = key.split("errorcode")[1]
                row = TInverterErrorCode[(TInverterErrorCode["modelId"] == modelId) & (TInverterErrorCode["errorGroup"] == int(group)) & (TInverterErrorCode["errorCode"] == val)]
                desc.append(row["name"].values[0])
                code.append(val)

        table.loc[ind, "warnCode"] = ", ".join(code)
        table.loc[ind, "warnDesc"] = ", ".join(desc)
    
    if update:
        conn.execute(sql.text("TRUNCATE `uiplatform`.`DeviceWarning`"))
        table.to_sql('DeviceWarning', con=uiplatform_engine, if_exists='append', index=False)

uiplatform_engine = create_engine('mysql+mysqldb://admin:Admin99@localhost/uiplatform?charset=utf8', pool_recycle=3600*7)
TWarning = pd.read_sql_table("TWarning", con=uiplatform_engine)
TSite = pd.read_sql_query(sql="SELECT id, siteName FROM uiplatform.TSite where deleteFlag = 0", con=uiplatform_engine)
TSiteInverter = pd.read_sql_query(sql="SELECT siteId, logicDeviceId, modelId FROM uiplatform.TSiteInverter", con=uiplatform_engine)
TInverterErrorCode = pd.read_sql_query(sql="SELECT * FROM uiplatform.TInverterErrorCode", con=uiplatform_engine)
conn = uiplatform_engine.connect()

print(f"{datetime.now()}: Start TWarning process.")
TWarning_dict = {"siteId":[], "siteName": [], "logicDeviceDesc": [], "logicDeviceId": [], "siteOffline": [], "deviceOfflineCnt": [], "saftyAbnCnt": [], "deviceAbnCnt": [], "stringAbnCnt": [], "efficiencyAbnCnt": [], "temperatureAbnCnt": [], "UpdTs": [], "warningSignal": []}

s = time.time()
conn.execute("call spUpdateWarnings()")
print(f"spUpdateWarnings() executed in {time.time()-s} sec.")

# update deviceWarning
DeviceWarning = pd.read_sql_query(sql="SELECT * FROM uiplatform.DeviceWarning", con=uiplatform_engine)
errorCode(DeviceWarning, update=True)

# push notification
# DeviceWarning2 = pd.read_sql_query(sql="SELECT * FROM uiplatform.DeviceWarning2", con=uiplatform_engine)
# pushNotification.send(DeviceWarning2, uiplatform_engine)

DeviceWarning_2 = pd.read_sql_table("DeviceWarning", con=uiplatform_engine)
DeviceCnt = pd.read_sql_query(sql.text("SELECT siteId, DetectDeviceAmount FROM uiplatform.TMonitorSite"), uiplatform_engine)
for sID, sName in zip(TSite["id"].tolist(), TSite["siteName"].tolist()):
    
    notNullEndTime = DeviceWarning_2.loc[DeviceWarning_2["endTime"].isnull()]
    
    tmpTb = notNullEndTime.loc[notNullEndTime["siteId"]==sID]
    warningCount = [0, 0, 0, 0, 0, 0, 0]
    logicDeviceId_list = []
    deviceDesc_list = []
    warningSignal_list = []
    
    for _, row in tmpTb.iterrows():
        
        logicDeviceId = row["logicDeviceId"]
        deviceDesc = row["deviceDesc"]
        warningSignal = row["warningSignal"]
        
        warningCount[int(warningSignal)-1] += 1
        logicDeviceId_list.append(logicDeviceId)
        deviceDesc_list.append(deviceDesc)
        warningSignal_list.append(warningSignal)
    
    warningSignal_list, warningCount = checkSiteOffline(warningSignal_list, warningCount, sID)

    for dID, dDesc, wSign in zip(logicDeviceId_list, deviceDesc_list, warningSignal_list):
        TWarning_dict["siteId"].append(sID)
        TWarning_dict["siteName"].append(sName)
        TWarning_dict["siteOffline"].append(warningCount[0])
        TWarning_dict["deviceOfflineCnt"].append(warningCount[1])
        TWarning_dict["saftyAbnCnt"].append(warningCount[2])
        TWarning_dict["temperatureAbnCnt"].append(warningCount[3])
        TWarning_dict["deviceAbnCnt"].append(warningCount[4])
        TWarning_dict["stringAbnCnt"].append(warningCount[5])
        TWarning_dict["efficiencyAbnCnt"].append(warningCount[6])
        TWarning_dict["UpdTs"].append(datetime.now())
        
        TWarning_dict["logicDeviceDesc"].append(dDesc)
        TWarning_dict["logicDeviceId"].append(dID)
        TWarning_dict["warningSignal"].append(wSign)

new_TWarning = pd.DataFrame(data=TWarning_dict)
conn.execute(sql.text("TRUNCATE `uiplatform`.`TWarning`"))
new_TWarning.to_sql('TWarning', con=uiplatform_engine, if_exists='append')

if sys.stdout is not None:
    print(f"{datetime.now()}: working fine!")
else:
    traceback.print_exc(sys.stdout)
