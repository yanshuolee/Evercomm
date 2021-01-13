import numpy as np
import math
import json
import datetime
import pandas as pd
import sqlalchemy as sql
try:
    import notify
except:
    from . import notify

class dataQualityAlgo():
    def __init__(self, engines, ieeeLists):
        self.engines = engines
        engine = engines.engine_dict["iotmgmt"]
        self.ieeeLists = ieeeLists

        # For All Day
        self.ainIeees = ieeeLists[(ieeeLists["warningCategoryId"].isin([3, 6, 7]))][["ieee", "gatewayId", "deviceDesc"]]
        ain_query = "SELECT ieee, DATE_FORMAT(receivedSync, '%Y-%m-%d %H:%i') as receivedSync, value1 FROM iotmgmt.ain where date(receivedSync)=date(now()-interval 1 day)"
        self.TAin = pd.read_sql_query(sql.text(ain_query), con=engine).drop_duplicates(subset=['receivedSync', 'ieee'])
        
        self.pmIeees = ieeeLists[(ieeeLists["warningCategoryId"].isin([1]))][["ieee", "gatewayId", "deviceDesc"]]
        pm_query = "SELECT ieee, DATE_FORMAT(receivedSync, '%Y-%m-%d %H:%i') as receivedSync FROM iotmgmt.pm where date(receivedSync)=date(now()-interval 1 day)"
        self.TPm = pd.read_sql_query(sql.text(pm_query), con=engine).drop_duplicates(subset=['receivedSync', 'ieee'])
        
        self.huaweiIeees = ieeeLists[(ieeeLists["warningCategoryId"].isin([10]))][["ieee", "gatewayId", "deviceDesc"]]
        solar2_query = "SELECT ieee, gatewayId, DATE_FORMAT(receivedSync, '%Y-%m-%d %H:%i') as receivedSync FROM iotmgmt.solarInverter2 where date(receivedSync)=date(now()-interval 1 day)"
        self.TSolar2 = pd.read_sql_query(sql.text(solar2_query), con=engine).drop_duplicates(subset=['receivedSync', 'ieee'])
        
        # For All Day - Complicated
        self.AESIeees_All = ieeeLists[(ieeeLists["warningCategoryId"].isin([4, 5, 8]))][["ieee", "gatewayId", "deviceDesc", "sensorDataQualityThreshold", "criticalIeee", "criticalThreshold"]]
        solar_query = "SELECT ieee, gatewayId, DATE_FORMAT(receivedSync, '%Y-%m-%d %H:%i') as receivedSync FROM iotmgmt.solarInverter where date(receivedSync)=date(now()-interval 1 day)"
        self.TSolar = pd.read_sql_query(sql.text(solar_query), con=engine).drop_duplicates(subset=['receivedSync', 'ieee'])
        
        # DC ain
        self.panelTempIeee = ieeeLists[(ieeeLists["warningCategoryId"].isin([9]))][["ieee", "gatewayId", "deviceDesc", "criticalIeee", "criticalThreshold", "sensorDataQualityThreshold"]]

    def calculateAllDay(self, insert=False):
        ainCount = self.TAin[self.TAin["ieee"].isin(self.ainIeees.ieee)].groupby(["ieee"]).count()
        pmCount = self.TPm[self.TPm["ieee"].isin(self.pmIeees.ieee)].groupby(["ieee"]).count()
        solar2Count = self.TSolar2[self.TSolar2["ieee"].isin(self.huaweiIeees.ieee)].groupby(["ieee"]).count()

        # Ain DQ
        ainIeees = self.ainIeees
        ainDQ = ainIeees.merge(ainCount, on=["ieee"], how="left").fillna(0).rename(columns={"receivedSync":"count"})
        ainDQ = pd.eval("dataQuality = ainDQ['count'] / 1440 * 100", target=ainDQ)

        # pm DQ
        pmIeees = self.pmIeees
        pmDQ = pmIeees.merge(pmCount, on=["ieee"], how="left").fillna(0).rename(columns={"receivedSync":"count"})
        pmDQ = pd.eval("dataQuality = pmDQ['count'] / 1440 * 100", target=pmDQ)

        # huawei DQ
        huaweiIeees = self.huaweiIeees
        solar2DQ = huaweiIeees.merge(solar2Count, on=["ieee"], how="left").fillna(0).rename(columns={"receivedSync":"count", "gatewayId_x":"gatewayId"})
        solar2DQ = pd.eval("dataQuality = solar2DQ['count'] / 1440 * 100", target=solar2DQ)

        # To Database.
        combinedTbl = pd.concat([ainDQ[["ieee", "gatewayId", "deviceDesc", "count", "dataQuality"]], pmDQ, solar2DQ[["ieee", "gatewayId", "deviceDesc", "count", "dataQuality"]]])
        yesterday = datetime.date.today() - datetime.timedelta(days=1)
        combinedTbl.insert(0, "ts", [str(yesterday)]*combinedTbl.shape[0])
        combinedTbl.insert(5, "totalAmount", [1440]*combinedTbl.shape[0])
        combinedTbl = combinedTbl.rename(columns={"count":"trueAmount"})
        if insert:
            combinedTbl.to_sql('dailyDataQualityReport', con=self.engines.engine_dict["iotcomui"], if_exists='append', index=False)
            print("24H DQ report inserted.")
        
    def calculateAllDayComplicated(self, insert=False):
        radiators = self.panelTempIeee.merge(self.TAin, left_on=["criticalIeee"], right_on=["ieee"], how="left")
        radiators = radiators[radiators["value1"]>=radiators["criticalThreshold"]][['ieee_x', 'gatewayId', 'deviceDesc', 'criticalIeee', 'criticalThreshold', 'receivedSync']]
        radiatorCount = radiators.groupby(["criticalIeee"]).count()
        radiatorCount = radiatorCount.fillna(0).rename(columns={"receivedSync":"radiatorCount"})[["radiatorCount"]]
        
        panelTemp = radiators.merge(self.TAin, left_on=["ieee_x", "receivedSync"], right_on=["ieee", "receivedSync"])
        panelTempCount = panelTemp.groupby(["ieee"]).count()
        panelTempCount = panelTempCount.fillna(0).rename(columns={"receivedSync":"panelTempCount"})[["panelTempCount"]]

        radPanelTempMerge = self.panelTempIeee.merge(radiatorCount, on=["criticalIeee"]).merge(panelTempCount, on=["ieee"])
        panelTempDQ = pd.eval("dataQuality = radPanelTempMerge.panelTempCount / radPanelTempMerge.radiatorCount * 100", target=radPanelTempMerge)

        # AES DQ
        radiators4AES = self.AESIeees_All.merge(self.TAin, left_on=["criticalIeee"], right_on=["ieee"], how="left")
        radiators4AES = radiators4AES[radiators4AES["value1"]>=radiators4AES["criticalThreshold"]][['ieee_x', 'gatewayId', 'deviceDesc', 'criticalIeee', 'criticalThreshold', 'receivedSync']]
        radiator4Solar = radiators4AES[radiators4AES["gatewayId"]==43]
        radiator4Solar2 = radiators4AES[~(radiators4AES["gatewayId"]==43)]
        solar2 = radiator4Solar2.merge(self.TSolar2, left_on=["ieee_x", "receivedSync"], right_on=["ieee", "receivedSync"])
        solar = radiator4Solar.merge(self.TSolar, left_on=["ieee_x", "receivedSync"], right_on=["ieee", "receivedSync"])
        solar2Count = solar2.groupby(["gatewayId_x", "ieee"]).count().rename(columns={"receivedSync":"solarCount"})[["solarCount"]]
        solarCount = solar.groupby(["gatewayId_x", "ieee"]).count().rename(columns={"receivedSync":"solarCount"})[["solarCount"]]
        solarMergedCount = pd.concat([solarCount, solar2Count])
        radiatorCount = radiators4AES.groupby(["criticalIeee"]).count().rename(columns={"receivedSync":"radiatorCount"})[["radiatorCount"]]
        radSolarMerge = self.AESIeees_All.merge(solarMergedCount, on=["ieee"], how="left").merge(radiatorCount, on=["criticalIeee"], how="left")
        AESAllSolarDQ = pd.eval("dataQuality = radSolarMerge.solarCount / radSolarMerge.radiatorCount * 100", target=radSolarMerge)
        AESAllSolarDQ = AESAllSolarDQ.fillna(0.0)

        # To Database
        combinedTbl = pd.concat(
            [AESAllSolarDQ[["ieee", "gatewayId", "deviceDesc", "solarCount", "radiatorCount", "dataQuality"]].rename(columns={"solarCount":"trueAmount", "radiatorCount":"totalAmount"}),
            panelTempDQ[["ieee", "gatewayId", "deviceDesc", "panelTempCount", "radiatorCount", "dataQuality"]].rename(columns={"panelTempCount":"trueAmount", "radiatorCount":"totalAmount"})]
        )
        yesterday = datetime.date.today() - datetime.timedelta(days=1)
        combinedTbl.insert(0, "ts", [str(yesterday)]*combinedTbl.shape[0])
        if insert:
            combinedTbl.to_sql('dailyDataQualityReport', con=self.engines.engine_dict["iotcomui"], if_exists='append', index=False)
            print("Complicated DQ report inserted.")

    def pushNotify(self):
        sql_query = f"SELECT * FROM iotcomui.dailyDataQualityReport where ts = date(now()-interval 1 day) and dataQuality < {DQThreshold}"
        DQ = pd.read_sql_query(sql.text(sql_query), con=self.engines.engine_dict["iotcomui"])
        data = DQ.merge(self.ieeeLists[["ieee", "sensorDataQualityThreshold"]], on=["ieee"])
        data = data[data["dataQuality"] < data["sensorDataQualityThreshold"]]
        # dataSlack = TWarningLog[["gatewayId", "ieee", "deviceDesc", "warningSignal", "warningDesc", "warningAmount"]].to_markdown(tablefmt="grid", stralign="center", numalign="center")
        # notify.Slack(dataSlack)
        notify.Line(data, "dq")

class warningDetector():
    def __init__(self, engines, ieeeLists, insert=None):
        self.engines = engines
        engine = engines.engine_dict["iotmgmt"]
        
        # necessaries
        self.insert = insert
        self.TWarningSignal = pd.read_sql_query("SELECT * FROM iotcomui.TWarningSignal", con=self.engines.engine_dict["iotcomui"])
        self.TDevice = pd.read_sql_query("SELECT id, ieee, deviceDesc FROM iotmgmt.TDevice", con=self.engines.engine_dict["iotcomui"])

        # 2538 pm
        self.pmIeees = ieeeLists[ieeeLists["warningCategoryId"].isin([1])]
        # pm_query = f"SELECT ieee, DATE_FORMAT(receivedSync, '%Y-%m-%d %H:%i') as receivedSync, ch1Watt, ch2Watt, ch3Watt FROM iotmgmt.pm where receivedSync >= now()-interval {self.WARNING_DETECT_MINUTE} minute"
        # self.TPm = pd.read_sql_query(sql.text(pm_query), con=engine).drop_duplicates(subset=['receivedSync', 'ieee'])
        yesterday_pm_query = f"SELECT ieee, DATE_FORMAT(receivedSync, '%Y-%m-%d %H:%i') as receivedSync, ch1Watt, ch2Watt, ch3Watt FROM iotmgmt.pm where date(receivedSync)=date(now()-interval 1 day)"
        self.TPmYesterday = pd.read_sql_query(sql.text(yesterday_pm_query), con=engine).drop_duplicates(subset=['receivedSync', 'ieee'])

        # 2538 ain
        self.ainIeees = ieeeLists[ieeeLists["warningCategoryId"].isin([3,6,7,9])]
        # ain_query = f"SELECT ieee, gatewayId, DATE_FORMAT(receivedSync, '%Y-%m-%d %H:%i') as receivedSync, value1, value2, value3, value4, value5 FROM iotmgmt.ain where receivedSync >= now()-interval {self.WARNING_DETECT_MINUTE} minute"
        # self.TAin = pd.read_sql_query(sql.text(ain_query), con=engine).drop_duplicates(subset=['receivedSync', 'ieee'])
        yesterday_ain_query = f"SELECT ieee, gatewayId, DATE_FORMAT(receivedSync, '%Y-%m-%d %H:%i') as receivedSync, value1, value2, value3, value4, value5 FROM iotmgmt.ain where date(receivedSync)=date(now()-interval 1 day)"
        self.TAinYesterday = pd.read_sql_query(sql.text(yesterday_ain_query), con=engine).drop_duplicates(subset=['receivedSync', 'ieee'])
        
        # solarInverter
        self.solarIeees = ieeeLists[ieeeLists["warningCategoryId"].isin([4,5,8,10])]
        # solar2_query = f"SELECT ieee, gatewayId, DATE_FORMAT(receivedSync, '%Y-%m-%d %H:%i') as receivedSync, acPower as totalActivePower FROM iotmgmt.solarInverter2 where receivedSync >= now()-interval {self.WARNING_DETECT_MINUTE} minute"
        # self.TSolar2 = pd.read_sql_query(sql.text(solar2_query), con=engine).drop_duplicates(subset=['receivedSync', 'ieee'])
        # solar_query = f"SELECT ieee, gatewayId, DATE_FORMAT(receivedSync, '%Y-%m-%d %H:%i') as receivedSync, totalActivePower FROM iotmgmt.solarInverter where receivedSync >= now()-interval {self.WARNING_DETECT_MINUTE} minute"
        # self.TSolar = pd.read_sql_query(sql.text(solar_query), con=engine).drop_duplicates(subset=['receivedSync', 'ieee'])
        # self.TSolarAll = pd.concat([self.TSolar, self.TSolar2]).sort_index()

        yesterday_solar2_query = f"SELECT ieee, gatewayId, DATE_FORMAT(receivedSync, '%Y-%m-%d %H:%i') as receivedSync, acPower as totalActivePower FROM iotmgmt.solarInverter2 where date(receivedSync)=date(now()-interval 1 day)"
        self.TSolar2Yesterday = pd.read_sql_query(sql.text(yesterday_solar2_query), con=engine).drop_duplicates(subset=['receivedSync', 'ieee'])
        yesterday_solar_query = f"SELECT ieee, gatewayId, DATE_FORMAT(receivedSync, '%Y-%m-%d %H:%i') as receivedSync, totalActivePower FROM iotmgmt.solarInverter where date(receivedSync)=date(now()-interval 1 day)"
        self.TSolarYesterday = pd.read_sql_query(sql.text(yesterday_solar_query), con=engine).drop_duplicates(subset=['receivedSync', 'ieee'])
        self.TSolarYesterdayAll = pd.concat([self.TSolarYesterday, self.TSolar2Yesterday]).sort_index()

        # panel temp
        self.panelTempIeees = ieeeLists[ieeeLists["warningCategoryId"].isin([3,9])]

        """
        # 手刻 radiator ieee lists
        self.radiatorIeees = ieeeLists[
            ((ieeeLists["gatewayId"].isin([42, 44, 45])) & (ieeeLists["deviceAttribId"]==9)) |
            ((ieeeLists["gatewayId"]==43) & (ieeeLists["ieee"]=="00124b000be4d1fc"))
        ]
        self.panelTempIeees = ieeeLists[
            ((ieeeLists["gatewayId"].isin([42, 44, 45])) & (ieeeLists["deviceAttribId"]==10)) |
            ((ieeeLists["gatewayId"]==43) & (ieeeLists["ieee"]=="00124b000be4cdc2"))
        ]

        # panel Temp
        # 用前面的 # self.panelTempIeee = ieeeLists[(ieeeLists["gatewayId"].isin([42, 43, 44])) & (ieeeLists["deviceAttribId"]==10)]
        # 用前面的 # self.radiatorIeee = ieeeLists[(ieeeLists["gatewayId"].isin([42, 43, 44])) & (ieeeLists["deviceAttribId"]==9)]
        self.envTempIeees = ieeeLists[(ieeeLists["gatewayId"].isin([42, 43, 44, 45])) & (ieeeLists["deviceAttribId"]==6)]
        """

    def toPRDB(self, info, tableName="Table"):
        info[["receivedSync", "ieee", "gatewayId", "deviceDesc", "radiatorValue", "panelTemp", "PR"]].to_sql("TWarningPR", con=self.engines.engine_dict["iotcomui"], if_exists='append', index=False)

        print(f"PR inserted")

    def toDBProcessing(self, info, warningSignal, tableName="Table"):
        warningDesc = self.TWarningSignal[self.TWarningSignal.id == warningSignal].warningDesc.to_numpy()[0]
        
        if self.insert:
            insertString = ""
            for _, row in info.iterrows():
                tempString = ""
                try: 
                    metadata = json.dumps(row['metadata'], ensure_ascii=False)
                except: 
                    metadata = None
                tempString = f"\
                    '{row['receivedSync']}',\
                    null,\
                    null,\
                    '{row['gatewayId']}',\
                    '{row['ieee'] if not pd.isna(row['ieee']) else None}',\
                    '{row['deviceDesc'] if not pd.isna(row['deviceDesc']) else None}', \
                    '{warningSignal}',\
                    '{warningDesc}',\
                    '{row['count'] if not pd.isna(row['count']) else None}',\
                    '{metadata}' "
                insertString += f"({tempString}) , "
            insertString = insertString[:-2]

            sqlstr = "REPLACE INTO iotcomui.TWarningLog\
                    (`reportDate`,`startTime`,`endTime`,`gatewayId`,`ieee`,`deviceDesc`,`warningSignal`,`warningDesc`,`warningAmount`,`abnormalDataLists`) \
                    values " + insertString
            sqlstr = sqlstr.replace("'None'", 'null')
            sqlstr=sqlstr.replace('":', '" : ')
            
            conn = self.engines.engine_dict["iotcomui"].connect()
            conn.execute(sql.text(sqlstr))

            print(f"{tableName} detected.")

    def pmAlgo(self, TPm):
        pm = self.pmIeees[["ieee", "gatewayId", "deviceDesc", "WarningAmount", "sensorLowerLimit", "sensorUpperLimit"]].merge(TPm, on=["ieee"], how="left").fillna(0)
        pm = pd.eval("chWattTotal = pm.ch1Watt+pm.ch2Watt+pm.ch3Watt", target=pm)
        pm = pm[(pm["chWattTotal"]>pm["sensorUpperLimit"]) | (pm["chWattTotal"]<pm["sensorLowerLimit"])]
        
        pmCount = pm["ieee"].value_counts()
        pmCount = pmCount.to_frame()
        pmCount.reset_index(inplace=True)
        pmCount = pmCount.rename(columns={"ieee":"count", "index":"ieee"})
        pmCount = pmCount.merge(self.pmIeees[["ieee", "WarningAmount"]], on=["ieee"], how="left").fillna(0)
        pmCount = pmCount[pmCount["count"]>pmCount["WarningAmount"]]

        pmWarning = pmCount[["ieee", "count"]].merge(pm, on=["ieee"], how="left")
        return pmWarning

    def detectPowerMeter(self):
        pmWarning = self.pmAlgo(self.TPmYesterday)

        # min will get first ts.
        info = pmWarning.groupby(["ieee"]).apply(min)[["receivedSync", "gatewayId", "ieee", "deviceDesc", "count"]]

        # To DB
        self.toDBProcessing(info, 2, "pm")

    def ainAlgo(self, TAin):
        ain = self.ainIeees[
            ["ieee", "gatewayId", "deviceDesc", "WarningAmount", "ainPort1LowerLimit", "ainPort1UpperLimit",
             "ainPort2LowerLimit", "ainPort2UpperLimit", "ainPort3LowerLimit", "ainPort3UpperLimit", 
             "ainPort4LowerLimit", "ainPort4UpperLimit", "ainPort5LowerLimit", "ainPort5UpperLimit"]
        ].merge(TAin, on=["ieee"], how="left").fillna(0)
        ain = ain[
            ((ain["value1"]>ain["ainPort1UpperLimit"])|(ain["value1"]<ain["ainPort1LowerLimit"])) |
            ((ain["value2"]>ain["ainPort2UpperLimit"])|(ain["value2"]<ain["ainPort2LowerLimit"])) |
            ((ain["value3"]>ain["ainPort3UpperLimit"])|(ain["value3"]<ain["ainPort3LowerLimit"])) |
            ((ain["value4"]>ain["ainPort4UpperLimit"])|(ain["value4"]<ain["ainPort4LowerLimit"])) |
            ((ain["value5"]>ain["ainPort5UpperLimit"])|(ain["value5"]<ain["ainPort5LowerLimit"]))
        ]

        ainCount = ain["ieee"].value_counts()
        ainCount = ainCount.to_frame()
        ainCount.reset_index(inplace=True)
        ainCount = ainCount.rename(columns={"ieee":"count", "index":"ieee"})
        ainCount = ainCount.merge(self.ainIeees[["ieee", "WarningAmount"]], on=["ieee"], how="left").fillna(0)
        ainCount = ainCount[ainCount["count"]>ainCount["WarningAmount"]]

        ainWarning = ainCount[["ieee", "count"]].merge(ain, on=["ieee"], how="left")
        return ainWarning

    def detectAin(self):
        ainWarning = self.ainAlgo(self.TAinYesterday)
        
        # To DB
        info = ainWarning.groupby(["ieee"]).apply(min)[["receivedSync", "gatewayId", "ieee", "deviceDesc", "count"]]
        self.toDBProcessing(info, 4, "ain")

    def inverterPR(self, TAin, TSolarAll, signal):
        # generate rad & panelTemp tbl
        TDevice = self.TDevice
        TDevice = TDevice.rename(columns={"ieee":"PanelTempIeee", "deviceDesc":"PanelTempDeviceDesc"})
        self.solarIeees = self.solarIeees.merge(TDevice, left_on=["PanelTempId"], right_on=["id"], how="left")

        radiatorData = TAin[TAin["ieee"].isin(self.solarIeees.criticalIeee.unique())]
        panelTempData = TAin[TAin["ieee"].isin(self.solarIeees.PanelTempIeee.unique())]
        radiatorPanelTemp = radiatorData[["ieee", "gatewayId", "receivedSync", "value1"]].merge(
            panelTempData[["ieee", "gatewayId", "receivedSync", "value3"]],
            on=["gatewayId", "receivedSync"],
            how="left",
            suffixes=("_rad", "_pt")
        )
        # get criticalThreshold
        radiatorPanelTemp = radiatorPanelTemp.merge(self.solarIeees[["criticalIeee", "criticalThreshold"]].drop_duplicates(), left_on=["ieee_rad"], right_on=["criticalIeee"], how="left")
        radiatorPanelTemp = radiatorPanelTemp[radiatorPanelTemp["value1"]>=radiatorPanelTemp["criticalThreshold"]]
        # generate solar tbl
        solarData = self.solarIeees[["ieee", "deviceDesc", "PRThresHold", "watt", "sensorLowerLimit", "sensorUpperLimit"]].fillna(.0).merge(
            TSolarAll,
            on=["ieee"],
            how="left"
        )
        mergedTbl = solarData.merge(radiatorPanelTemp, on=["gatewayId", "receivedSync"])

        withoutPT = mergedTbl[(mergedTbl["value3"].isna())]
        withPT = mergedTbl[~(mergedTbl["value3"].isna())]
        if withoutPT.shape[0] != 0:
            PRTable1 = pd.eval("PR = 100 * (withPT.totalActivePower) / (withPT.watt * withPT.value1/1000) / (1-(withPT.value3-25)*0.0043)", target=withPT)
            PRTable2 = pd.eval("PR = 100 * (withoutPT.totalActivePower) / (withoutPT.watt * withoutPT.value1/1000)", target=withoutPT)
            PRTable = pd.concat([PRTable1, PRTable2]).sort_index()
        else:
            PRTable = pd.eval("PR = 100 * (withPT.totalActivePower) / (withPT.watt * withPT.value1/1000) / (1-(withPT.value3-25)*0.0043)", target=withPT)

        rule1 = PRTable[(PRTable["PR"]>PRTable["sensorUpperLimit"]) | (PRTable["PR"]<PRTable["sensorLowerLimit"])].rename(columns={"value1":"radiatorValue", "value3":"panelTemp"})
        warningCount = rule1["ieee"].value_counts()
        warningCount = warningCount.to_frame()
        warningCount.reset_index(inplace=True)
        warningCount = warningCount.rename(columns={"ieee":"count", "index":"ieee"})
        warningCount = self.solarIeees[["ieee", "WarningAmount"]].merge(warningCount, on=["ieee"], how="left").fillna(0)
        warningCount = warningCount[warningCount["count"]>warningCount["WarningAmount"]]
        inverterWarning = warningCount.merge(rule1, on=["ieee"], how="left")
        
        # To DB
        if inverterWarning.shape[0] != 0:
            # info = inverterWarning.groupby(["ieee"]).apply(min)[["receivedSync", "gatewayId", "deviceDesc", "count"]]
            # info_metadata = pd.concat([inverterWarning, inverterWarning[["radiatorValue", "panelTemp", "PR"]].apply(lambda x: x.to_json(), axis=1)], axis=1).rename(columns={0:"metadata"})
            # dataDict = {"ieee":[], "metadata":[]}
            # for ieee in info_metadata.ieee.unique():
            #     tmpTbl = info_metadata[info_metadata.ieee==info_metadata.ieee.unique()[0]]
            #     dataDict["ieee"].append(ieee)
            #     dataDict["metadata"].append(dict([(i,j) for i, j in zip(tmpTbl.receivedSync, tmpTbl.metadata)]))
            # metadataFrame = pd.DataFrame(dataDict)

            # info = info.merge(metadataFrame, on=["ieee"], how="left")

            # self.toDBProcessing(info, signal, "pm")
            self.toPRDB(inverterWarning, "pm")

    def detectInverter(self):
        # self.inverterPR(TAin, TSolarAll, 5)
        self.inverterPR(self.TAinYesterday, self.TSolarYesterdayAll, 6)

    def detectPanelTemp(self):
        TDeviceRad, TDeviceEnv = self.TDevice, self.TDevice
        TAin = self.TAinYesterday
        TDeviceRad = TDeviceRad.rename(columns={"ieee":"RadIeee", "deviceDesc":"RadDeviceDesc"})
        TDeviceEnv = TDeviceEnv.rename(columns={"ieee":"EnvIeee", "deviceDesc":"EnvDeviceDesc"})
        self.panelTempIeees = self.panelTempIeees.merge(TDeviceRad, left_on=["RadiationId"], right_on=["id"], how="left")
        self.panelTempIeees = self.panelTempIeees.merge(TDeviceEnv, left_on=["envTempId"], right_on=["id"], how="left")
        
        radiatorData = TAin[TAin["ieee"].isin(self.panelTempIeees.RadIeee)]
        envTempData = TAin[TAin["ieee"].isin(self.panelTempIeees.EnvIeee)]
        panelTempData = TAin[TAin["ieee"].isin(self.panelTempIeees.ieee)]
        radLists = np.array([], dtype=float)
        radIeeeLists = np.array([], dtype=float)
        radrcLists = np.array([], dtype=float)
        gwLists = np.array([], dtype=float)
        # use multi-processing for improvement
        for _, (gatewayId, ieee, PANEL_TEMP_TIME_DURATION) in self.panelTempIeees[["gatewayId", "RadIeee", "ptempTimeDuration"]].iterrows():
            PANEL_TEMP_TIME_DURATION = int(PANEL_TEMP_TIME_DURATION)
            tempAin = radiatorData[radiatorData["ieee"]==ieee]
            tempAin = tempAin.reset_index()
            for i in range(tempAin.shape[0]-PANEL_TEMP_TIME_DURATION):
                roi = tempAin.loc[i:i+PANEL_TEMP_TIME_DURATION]
                radLists = np.append(radLists, (np.mean(roi["value1"]) if roi.shape[0] != 0 else 0.0))
                radIeeeLists = np.append(radIeeeLists, [ieee])
                radrcLists = np.append(radrcLists, [roi["receivedSync"].to_numpy()[-1]])
                gwLists = np.append(gwLists, [gatewayId])
        radAvgDF = pd.DataFrame(data={"ieee":radIeeeLists, "gatewayId":gwLists, "receivedSync":radrcLists, "radiator":radLists})
        radAvgDF = self.panelTempIeees[["RadIeee", "tempCompareRadThreshod"]].merge(radAvgDF, left_on=["RadIeee"], right_on=["ieee"])[['RadIeee', 'gatewayId', 'receivedSync', 'radiator', 'tempCompareRadThreshod']]
        radAvgDF = radAvgDF[radAvgDF.radiator<=radAvgDF.tempCompareRadThreshod][['RadIeee', 'gatewayId', 'receivedSync', 'radiator']]
        envTempData = envTempData[['ieee', 'gatewayId', 'receivedSync', 'value4']].rename(columns={"ieee":"EnvIeee", "value4":"envTemp"})
        panelTempData = panelTempData[['ieee', 'gatewayId', 'receivedSync', 'value3']].rename(columns={"ieee":"panelIeee", "value3":"panelTemp"})
        panelEnvTemp = envTempData.merge(panelTempData, on=["gatewayId", "receivedSync"])
        panelEnvTemp = pd.eval("diff = abs(panelEnvTemp.envTemp - panelEnvTemp.panelTemp)", target=panelEnvTemp)
        panelEnvTemp = radAvgDF.merge(panelEnvTemp, on=["gatewayId", "receivedSync"])
        avgDiff = panelEnvTemp[["gatewayId", "envTemp", "panelTemp", "diff"]].groupby(["gatewayId"]).mean()
        avgDiff = self.panelTempIeees[["gatewayId", "tempCompareThreshold"]].merge(avgDiff, on=["gatewayId"])
        avgDiffWarning = avgDiff[avgDiff["diff"]>=avgDiff["tempCompareThreshold"]]
        avgDiffWarning = avgDiffWarning.merge(self.panelTempIeees[["gatewayId", "ieee"]], on=["gatewayId"]).merge(self.panelTempIeees[["gatewayId", "EnvIeee"]], on=["gatewayId"])

        metadataLists = []
        gw = []
        for ind, info in avgDiffWarning.iterrows():
            gw.append(info["gatewayId"])
            metadataLists.append({"EnvTemp":[info["EnvIeee"], info["envTemp"]],
                                  "PanelTemp":[info["ieee"], info["panelTemp"]],
                                  "太陽能板溫度計 & 環境溫度計的平均差異":info["diff"]})

        yesterday = datetime.date.today() - datetime.timedelta(days=1)
        rowNum = len(metadataLists)
        part1 = pd.DataFrame(data={"receivedSync":[str(yesterday)]*rowNum,
                                   "gatewayId":gw,
                                   "ieee":[None]*rowNum,
                                   "deviceDesc":[None]*rowNum,
                                   "count":[None]*rowNum,
                                   "metadata":metadataLists})
        self.toDBProcessing(part1, 7, "panelEnvTempDiff")

        
        # make avg radiator data
        # ain = self.TAinYesterday[self.TAinYesterday.ieee.isin(self.radiatorIeees.ieee)]
        # radLists = np.array([], dtype=float)
        # radIeeeLists = np.array([], dtype=float)
        # radrcLists = np.array([], dtype=float)
        # for ieee in self.radiatorIeees.ieee:
        #     tempAin = ain[ain["ieee"]==ieee]
        #     tempAin = tempAin.reset_index()
        #     for i in range(tempAin.shape[0]-PANEL_TEMP_TIME_DURATION):
        #         roi = tempAin.loc[i:i+PANEL_TEMP_TIME_DURATION]
        #         radLists = np.append(radLists, (np.mean(roi["value1"]) if roi.shape[0] != 0 else 0.0))
        #         radIeeeLists = np.append(radIeeeLists, [ieee])
        #         radrcLists = np.append(radrcLists, [roi["receivedSync"].to_numpy()[-1]])
        # radAvgDF = pd.DataFrame(data={"ieee":radIeeeLists, "receivedSync":radrcLists, "radiator":radLists})
        # radAvgDF = self.radiatorIeees.merge(radAvgDF, on=["ieee"])
        # radiatorBelowThreshold = radAvgDF[radAvgDF.radiator<=TEMP_COMPARE_RAD_THRESHOLD]

        # if radiatorBelowThreshold.shape[0] != 0:
            # panelTemp = self.panelTempIeees[self.panelTempIeees["gatewayId"].isin(self.radiatorIeees.gatewayId)].merge(self.TAinYesterday, on=["ieee"], how="left")[['deviceTypeId', 'deviceAttribId', 'gatewayId', 'ieee', 'deviceDesc', 'receivedSync', 'value3']]
            # envTemp = self.envTempIeees[self.envTempIeees["gatewayId"].isin(self.radiatorIeees.gatewayId)].merge(self.TAinYesterday, on=["ieee"], how="left")[['deviceTypeId', 'deviceAttribId', 'gatewayId', 'ieee', 'deviceDesc', 'receivedSync', 'value4']]

            # panelEnvTemp = panelTemp.merge(envTemp, on=["gatewayId", "receivedSync"])[['gatewayId', 'receivedSync', 'value3', 'value4']]
            # panelEnvTemp = pd.eval("diff = abs(panelEnvTemp.value3 - panelEnvTemp.value4)", target=panelEnvTemp)

            # panelEnvTempAvgDiff = panelEnvTemp.groupby(["gatewayId"]).mean()
            # panelEnvTempAvgDiffWarning = panelEnvTempAvgDiff[panelEnvTempAvgDiff["diff"]>=TEMP_COMPARE_THRESHOLD]

            # panelEnvInfo = self.envTempIeees.merge(self.panelTempIeees, on=["gatewayId"], suffixes=("EnvTemp", "PanelTemp"))[["gatewayId", "ieeeEnvTemp", "ieeePanelTemp", "deviceDescEnvTemp", "deviceDescPanelTemp"]]
            # panelEnvInfo = panelEnvTempAvgDiffWarning.merge(panelEnvInfo, on=["gatewayId"])
            # gwLists = []
            # metadataLists = []
            # for ind, info in panelEnvInfo.iterrows():
            #     gwLists.append(info["gatewayId"])
            #     metadataLists.append({info["deviceDescEnvTemp"]:[info["ieeeEnvTemp"], info["value4"]],
            #                           info["deviceDescPanelTemp"]:[info["ieeePanelTemp"], info["value3"]],
            #                           "太陽能板溫度計 & 環境溫度計的平均差異":info["diff"]})
            # yesterday = datetime.date.today() - datetime.timedelta(days=1)
            # rowNum = len(gwLists)
            # part1 = pd.DataFrame(data={"receivedSync":[str(yesterday)]*rowNum,
                                    #    "gatewayId":gwLists,
                                    #    "ieee":[None]*rowNum,
                                    #    "deviceDesc":[None]*rowNum,
                                    #    "count":[None]*rowNum,
                                    #    "metadata":metadataLists})

            # self.toDBProcessing(part1, 7, "panelEnvTempDiff")
        # else:
        #     pass

        # ============================================ Part 2 ================================================== #
        x1 = pd.eval(f"x = radAvgDF.radiator / 800 * {PANEL_TEMP_COEFF}", target=radAvgDF)
        x1 = x1.merge(panelEnvTempAvgDiff, on=["gatewayId"])
        y1 = pd.eval("y = x1.x + x1['diff']", target=x1)[["gatewayId", "receivedSync", "y"]]

        envTemp_x = self.envTempIeees.merge(self.TAinYesterday, on=["ieee"], how="left")[['deviceTypeId', 'deviceAttribId', 'gatewayId', 'ieee', 'deviceDesc', 'receivedSync', 'value4']]
        idealPanelTemp = envTemp_x.merge(y1, on=["gatewayId", "receivedSync"], how="inner")
        idealPanelTemp = pd.eval("idealPanelTemp = idealPanelTemp.value4 - idealPanelTemp.y", target=idealPanelTemp)

        # To DB
        info = idealPanelTemp.merge(panelTemp, on=["gatewayId", "receivedSync"])
        info = pd.eval(f"realIdealDiff = abs(info.value3 - info.idealPanelTemp)", target=info)[["gatewayId",
                                                                                                "receivedSync",
                                                                                                "ieee_y", 
                                                                                                "deviceDesc_y", 
                                                                                                "value3", 
                                                                                                "idealPanelTemp", 
                                                                                                "realIdealDiff"]].rename(columns={"ieee_y":"ieee", "deviceDesc_y":"deviceDesc", "value3":"truePanelTemp"})

        info = info[info["realIdealDiff"]>=TEMP_DATA_DIFF_AMOUNT]

        infoCount = info["ieee"].value_counts()
        infoCount = infoCount[infoCount > self.WARNING_AMOUNT]
        infoCount = infoCount.to_frame()
        infoCount.reset_index(inplace=True)
        infoCount = infoCount.rename(columns={"ieee":"count", "index":"ieee"})

        if info.shape[0] != 0:
            info_1 = info.merge(infoCount, on=["ieee"])
            info_1[['receivedSync',
                   'ieee',
                   'gatewayId',
                   'deviceDesc',
                   'truePanelTemp',
                   'idealPanelTemp',
                   'realIdealDiff']].to_sql("TWarningPanelTempAbnormal", con=self.engines.engine_dict["iotcomui"], if_exists='append', index=False)            
            info_2 = info_1.groupby(["gatewayId"]).apply(min)[["receivedSync", "gatewayId", "ieee", "deviceDesc", "count"]]
            info_2.insert(0, "metadata", [['see iotcomui.TWarningPanelTempAbnormal for details']]*info_2.shape[0])
            self.toDBProcessing(info_2, 8, "panelEnvTempDiff")

    def detectAll(self):
        # self.detectPowerMeter()
        # self.detectAin()
        # self.detectInverter()
        self.detectPanelTemp()
    
    def pushNotify(self):
        sql_query = "SELECT * FROM iotcomui.TWarningLog where reportDate = date(now()-interval 1 day)"
        TWarningLog = pd.read_sql_query(sql.text(sql_query), con=self.engines.engine_dict["iotcomui"])
        dataSlack = TWarningLog[["gatewayId", "ieee", "deviceDesc", "warningSignal", "warningDesc", "warningAmount"]].to_markdown(tablefmt="grid", stralign="center", numalign="center")
        data = TWarningLog[["gatewayId", "ieee", "deviceDesc", "warningSignal", "warningDesc", "warningAmount", "abnormalDataLists"]]
        # notify.Slack(dataSlack)
        notify.Line(data, "wd")
