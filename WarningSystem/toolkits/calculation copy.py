import numpy as np
import math
import json
import datetime
import pandas as pd
import sqlalchemy as sql

class dataQualityAlgo():
    def __init__(self, engines, ieeeLists):
        self.CRITICAL_THRESHOLD = 50
        self.SOLAR1_GW = 43
        self.SENSOR_DATA_QUALITY_THRESHOLD = 95
        self.engines = engines
        engine = engines.engine_dict["iotmgmt"]

        # For All Day
        ain_query = "SELECT ieee, DATE_FORMAT(receivedSync, '%Y-%m-%d %H:%i') as receivedSync, value1 FROM iotmgmt.ain where date(receivedSync)=date(now()-interval 1 day)"
        self.TAin = pd.read_sql_query(sql.text(ain_query), con=engine).drop_duplicates(subset=['receivedSync', 'ieee'])
        self.ainIeees = ieeeLists[(ieeeLists["deviceAttribId"]==6) | (ieeeLists["deviceAttribId"]==9)][["ieee", "gatewayId", "deviceDesc"]]

        pm_query = "SELECT ieee, DATE_FORMAT(receivedSync, '%Y-%m-%d %H:%i') as receivedSync FROM iotmgmt.pm where date(receivedSync)=date(now()-interval 1 day)"
        self.TPm = pd.read_sql_query(sql.text(pm_query), con=engine).drop_duplicates(subset=['receivedSync', 'ieee'])
        self.pmIeees = ieeeLists[(ieeeLists["deviceAttribId"]==1)][["ieee", "gatewayId", "deviceDesc"]]

        solar2_query = "SELECT ieee, gatewayId, DATE_FORMAT(receivedSync, '%Y-%m-%d %H:%i') as receivedSync FROM iotmgmt.solarInverter2 where date(receivedSync)=date(now()-interval 1 day)"
        self.TSolar2 = pd.read_sql_query(sql.text(solar2_query), con=engine).drop_duplicates(subset=['receivedSync', 'ieee'])
        self.huaweiIeees = ieeeLists[(ieeeLists["deviceTypeId"]==81)][["ieee", "gatewayId", "deviceDesc"]]

        # For All Day - Complicated
        solar_query = "SELECT ieee, gatewayId, DATE_FORMAT(receivedSync, '%Y-%m-%d %H:%i') as receivedSync FROM iotmgmt.solarInverter where date(receivedSync)=date(now()-interval 1 day)"
        self.TSolar = pd.read_sql_query(sql.text(solar_query), con=engine).drop_duplicates(subset=['receivedSync', 'ieee'])
        
        self.AESIeees_All = ieeeLists[(ieeeLists["deviceTypeId"].isin([61, 62, 63, 69, 70]))][["ieee", "gatewayId", "deviceDesc"]]

        # 手刻 radiator ieee lists
        self.radiatorIeees = ieeeLists[(ieeeLists["gatewayId"].isin([42, 44, 45, 40, 41])) & (ieeeLists["deviceAttribId"]==9) | ((ieeeLists["gatewayId"]==43) & (ieeeLists["ieee"]=="00124b000be4d203")) ]
        
        # special case for DC ain
        self.panelTempIeee = ieeeLists[(ieeeLists["gatewayId"]==42) & (ieeeLists["deviceAttribId"]==10)]

    def calculateAllDay(self, insert=False):
        def f(x): return x*100/1440
        ainCount = self.TAin[self.TAin["ieee"].isin(self.ainIeees.ieee)].groupby(["ieee"]).count()
        pmCount = self.TPm[self.TPm["ieee"].isin(self.pmIeees.ieee)].groupby(["ieee"]).count()
        solar2Count = self.TSolar2[self.TSolar2["ieee"].isin(self.huaweiIeees.ieee)].groupby(["ieee"]).count()

        # Ain DQ
        ainIeees = self.ainIeees # pd.DataFrame({"ieee":self.ainIeees.ieee})
        ainDQ = ainIeees.merge(ainCount, on=["ieee"], how="left").fillna(0).rename(columns={"receivedSync":"count"})
        ainDQ = pd.eval("dataQuality = ainDQ['count'] / 1440 * 100", target=ainDQ)
        # ainDQ["count"] = ainDQ["count"].apply(f)

        # pm DQ
        pmIeees = self.pmIeees # pd.DataFrame({"ieee":self.pmIeees})
        pmDQ = pmIeees.merge(pmCount, on=["ieee"], how="left").fillna(0).rename(columns={"receivedSync":"count"})
        pmDQ = pd.eval("dataQuality = pmDQ['count'] / 1440 * 100", target=pmDQ)
        # pmDQ["count"] = pmDQ["count"].apply(f)

        # huawei DQ
        huaweiIeees = self.huaweiIeees # pd.DataFrame({"ieee":self.huaweiIeees})
        solar2DQ = huaweiIeees.merge(solar2Count, on=["ieee"], how="left").fillna(0).rename(columns={"receivedSync":"count", "gatewayId_x":"gatewayId"})
        solar2DQ = pd.eval("dataQuality = solar2DQ['count'] / 1440 * 100", target=solar2DQ)
        # solar2DQ["count"] = solar2DQ["count"].apply(f)

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
        radiators = self.TAin[(self.TAin["ieee"].isin(self.radiatorIeees.ieee.to_numpy())) & (self.TAin["value1"]>=self.CRITICAL_THRESHOLD)]
        radiatorCount = radiators.groupby(["ieee"]).count()
        radiatorCount = self.radiatorIeees.merge(radiatorCount, on=["ieee"], how="left").fillna(0).rename(columns={"value1":"radiatorCount"})
        radiatorListsForTS = radiators.merge(self.radiatorIeees, on=["ieee"], how="left")

        # AES DQ
        radiator4Solar = radiatorListsForTS[radiatorListsForTS["gatewayId"]==self.SOLAR1_GW]
        radiator4Solar2 = radiatorListsForTS[~(radiatorListsForTS["gatewayId"]==self.SOLAR1_GW)]

        solarLists = radiator4Solar.merge(self.TSolar, on=["gatewayId", "receivedSync"], how="inner").rename(columns={"ieee_x":"radiatorIeee", "ieee_y":"invIeee"})
        solarCount = solarLists.groupby(["gatewayId", "invIeee"]).count().rename(columns={"radiatorIeee":"solarCount"}).reset_index().drop(['receivedSync', 'value1', 'deviceTypeId', 'deviceAttribId', 'deviceDesc'], axis=1)
        solar2Lists = radiator4Solar2.merge(self.TSolar2, on=["gatewayId", "receivedSync"], how="inner").rename(columns={"ieee_x":"radiatorIeee", "ieee_y":"invIeee"})
        solar2Count = solar2Lists.groupby(["gatewayId", "invIeee"]).count().rename(columns={"radiatorIeee":"solarCount"}).reset_index().drop(['receivedSync', 'value1', 'deviceTypeId', 'deviceAttribId', 'deviceDesc'], axis=1)
        solarMergedCount = pd.concat([solarCount, solar2Count]).sort_index()

        AESIeees = self.AESIeees_All
        solarMergedCount = AESIeees.merge(solarMergedCount, left_on=["gatewayId", "ieee"], right_on=["gatewayId", "invIeee"], how="left").fillna(0)

        radAllSolarMerge = solarMergedCount.merge(radiatorCount, on=["gatewayId"], how="left").fillna(0)
        AESAllSolarDQ = pd.eval("AESSolarDQ = radAllSolarMerge.solarCount / radAllSolarMerge.radiatorCount * 100", target=radAllSolarMerge).fillna(0)
        
        # panel temp DQ
        panelTemp = self.TAin[(self.TAin["ieee"].isin(self.panelTempIeee.ieee.to_numpy()))]
        panelTemp = panelTemp.merge(self.panelTempIeee, on=["ieee"], how="left")
        radiator4PanelTemp = radiatorListsForTS[radiatorListsForTS["gatewayId"]==42]
        panelTempLists = radiator4PanelTemp.merge(panelTemp, on=["gatewayId", "receivedSync"], how="inner").rename(columns={"ieee_x":"radiatorIeee", "ieee_y":"panelTempIeee"})
        panelTempCount = panelTempLists.groupby(["gatewayId", "panelTempIeee"]).count().rename(columns={"radiatorIeee":"panelTempCount"}).reset_index().drop(['receivedSync', 'value1_x', 'deviceTypeId_x', 'deviceAttribId_x', 'deviceDesc_x', 'value1_y', 'deviceTypeId_y', 'deviceAttribId_y', 'deviceDesc_y'], axis=1)
        panelTempCount = panelTempCount.merge(self.panelTempIeee, left_on=["gatewayId", "panelTempIeee"], right_on=["gatewayId", "ieee"])
        radPanelTempMerge = radiatorCount[radiatorCount["gatewayId"]==42].merge(panelTempCount, on=["gatewayId"], how="left").fillna(0)
        panelTempDQ = pd.eval("panelTempDQ = radPanelTempMerge.panelTempCount / radPanelTempMerge.radiatorCount * 100", target=radPanelTempMerge)[["panelTempIeee", "gatewayId", "deviceDesc_y", "panelTempCount", "radiatorCount", "panelTempDQ"]]
        panelTempDQ = panelTempDQ.rename(columns={"panelTempIeee":"ieee", "deviceDesc_y":"deviceDesc", "panelTempDQ":"dataQuality", "panelTempCount":"trueAmount", "radiatorCount":"totalAmount"})
        
        # To Database.
        combinedTbl = pd.concat([AESAllSolarDQ[["ieee_x", "gatewayId", "deviceDesc_x", "solarCount", "radiatorCount", "AESSolarDQ"]].rename(columns={"ieee_x":"ieee", "deviceDesc_x":"deviceDesc", "AESSolarDQ":"dataQuality", "solarCount":"trueAmount", "radiatorCount":"totalAmount"}), panelTempDQ])
        yesterday = datetime.date.today() - datetime.timedelta(days=1)
        combinedTbl.insert(0, "ts", [str(yesterday)]*combinedTbl.shape[0])
        if insert:
            combinedTbl.to_sql('dailyDataQualityReport', con=self.engines.engine_dict["iotcomui"], if_exists='append', index=False)
            print("Complicated DQ report inserted.")

class warningDetector():
    def __init__(self, engines, ieeeLists):
        self.CRITICAL_THRESHOLD = 50
        self.WARNING_DETECT_MINUTE = 10
        self.WARNING_AMOUNT = 2

        self.engines = engines
        engine = engines.engine_dict["iotmgmt"]
        
        # necessaries
        self.TWarningSignal = pd.read_sql_query("SELECT * FROM iotcomui.TWarningSignal", con=self.engines.engine_dict["iotcomui"])

        # 2538 pm
        self.pmIeees = ieeeLists[(ieeeLists["deviceAttribId"]==1)][["ieee", "gatewayId", "deviceDesc"]]
        # pm_query = f"SELECT ieee, DATE_FORMAT(receivedSync, '%Y-%m-%d %H:%i') as receivedSync, ch1Watt, ch2Watt, ch3Watt FROM iotmgmt.pm where receivedSync >= now()-interval {self.WARNING_DETECT_MINUTE} minute"
        # self.TPm = pd.read_sql_query(sql.text(pm_query), con=engine).drop_duplicates(subset=['receivedSync', 'ieee'])
        yesterday_pm_query = f"SELECT ieee, DATE_FORMAT(receivedSync, '%Y-%m-%d %H:%i') as receivedSync, ch1Watt, ch2Watt, ch3Watt FROM iotmgmt.pm where date(receivedSync)=date(now()-interval 1 day)"
        self.TPmYesterday = pd.read_sql_query(sql.text(yesterday_pm_query), con=engine).drop_duplicates(subset=['receivedSync', 'ieee'])

        # 2538 ain
        self.ainIeees = ieeeLists[(ieeeLists["deviceAttribId"]==6) | (ieeeLists["deviceAttribId"]==9)][["ieee", "gatewayId", "deviceDesc"]]
        ain_query = f"SELECT ieee, DATE_FORMAT(receivedSync, '%Y-%m-%d %H:%i') as receivedSync, value1, value2, value3, value4, value5 FROM iotmgmt.ain where receivedSync >= now()-interval {self.WARNING_DETECT_MINUTE} minute"
        self.TAin = pd.read_sql_query(sql.text(ain_query), con=engine).drop_duplicates(subset=['receivedSync', 'ieee'])
        yesterday_ain_query = f"SELECT ieee, DATE_FORMAT(receivedSync, '%Y-%m-%d %H:%i') as receivedSync, value1, value2, value3, value4, value5 FROM iotmgmt.ain where date(receivedSync)=date(now()-interval 1 day)"
        self.TAinYesterday = pd.read_sql_query(sql.text(yesterday_ain_query), con=engine).drop_duplicates(subset=['receivedSync', 'ieee'])
        
        # solarInverter
        self.solarIeees = ieeeLists[((ieeeLists["deviceTypeId"].isin([61, 62, 63, 69, 70])) & (ieeeLists["gatewayId"].isin([42, 43, 44, 45]))) | ((ieeeLists["deviceTypeId"]==81) & ~(ieeeLists["gatewayId"].isin([0])))][["ieee", "gatewayId", "deviceDesc", "watt"]]
        """
        self.AESIeees_All = ieeeLists[(ieeeLists["deviceTypeId"].isin([61, 62, 63, 69, 70]))][["ieee", "gatewayId", "deviceDesc"]]
        self.huaweiIeees = ieeeLists[(ieeeLists["deviceTypeId"]==81)][["ieee", "gatewayId", "deviceDesc"]]
        """
        solar2_query = f"SELECT ieee, gatewayId, DATE_FORMAT(receivedSync, '%Y-%m-%d %H:%i') as receivedSync, acPower as totalActivePower FROM iotmgmt.solarInverter2 where receivedSync >= now()-interval {self.WARNING_DETECT_MINUTE} minute"
        self.TSolar2 = pd.read_sql_query(sql.text(solar2_query), con=engine).drop_duplicates(subset=['receivedSync', 'ieee'])
        solar_query = f"SELECT ieee, gatewayId, DATE_FORMAT(receivedSync, '%Y-%m-%d %H:%i') as receivedSync, totalActivePower FROM iotmgmt.solarInverter where receivedSync >= now()-interval {self.WARNING_DETECT_MINUTE} minute"
        self.TSolar = pd.read_sql_query(sql.text(solar_query), con=engine).drop_duplicates(subset=['receivedSync', 'ieee'])
        self.TSolarAll = pd.concat([self.TSolar, self.TSolar2]).sort_index()

        yesterday_solar2_query = f"SELECT ieee, gatewayId, DATE_FORMAT(receivedSync, '%Y-%m-%d %H:%i') as receivedSync, acPower as totalActivePower FROM iotmgmt.solarInverter2 where date(receivedSync)=date(now()-interval 1 day)"
        self.TSolar2Yesterday = pd.read_sql_query(sql.text(yesterday_solar2_query), con=engine).drop_duplicates(subset=['receivedSync', 'ieee'])
        yesterday_solar_query = f"SELECT ieee, gatewayId, DATE_FORMAT(receivedSync, '%Y-%m-%d %H:%i') as receivedSync, totalActivePower FROM iotmgmt.solarInverter where date(receivedSync)=date(now()-interval 1 day)"
        self.TSolarYesterday = pd.read_sql_query(sql.text(yesterday_solar_query), con=engine).drop_duplicates(subset=['receivedSync', 'ieee'])
        self.TSolarYesterdayAll = pd.concat([self.TSolarYesterday, self.TSolar2Yesterday]).sort_index()

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

    def toPRDB(self, info, tableName="Table"):
        info[["receivedSync", "ieee", "gatewayId", "deviceDesc", "radiatorValue", "panelTemp", "PR"]].to_sql("TWarningPR", con=self.engines.engine_dict["iotcomui"], if_exists='append', index=False)

        print(f"PR inserted")

    def toDBProcessing(self, info, warningSignal, tableName="Table"):
        warningDesc = self.TWarningSignal[self.TWarningSignal.id == warningSignal].warningDesc.to_numpy()[0]
        insertString = ""
        for _, row in info.iterrows():
            tempString = ""
            try: 
                metadata = row['metadata'] # json.dumps(row['metadata']) 
                # metadata = str(metadata).replace('":', '" : ').replace("': ", "' : ")
            except: 
                metadata = None
            tempString = f"\
'{row['receivedSync']}',\
null,\
null,\
'{row['gatewayId']}',\
'{row['ieee']}',\
'{row['deviceDesc']}', \
'{warningSignal}',\
'{warningDesc}',\
'{row['count']}',\
null "
            insertString += f"({tempString}) , "
        insertString = insertString[:-2]
        # sqlstr = f"REPLACE INTO iotcomui.TWarningLog\
        #             (`reportDate`,`startTime`,`endTime`,`gatewayId`,`ieee`,`deviceDesc`,`warningSignal`,`warningDesc`,`warningAmount`,`abnormalDataLists`)\
        #             values {insertString}"
        sqlstr = "REPLACE INTO iotcomui.TWarningLog\
(`reportDate`,`startTime`,`endTime`,`gatewayId`,`ieee`,`deviceDesc`,`warningSignal`,`warningDesc`,`warningAmount`,`abnormalDataLists`)\
values " + insertString
        sqlstr = sqlstr.replace("'None'", 'null')
        sqlstr=sqlstr.replace('":', '" : ')
        
        conn = self.engines.engine_dict["iotcomui"].connect()
        conn.execute(sql.text(sqlstr))

        print(f"{tableName} detected.")

    def pmAlgo(self, TPm):
        # define limit variable
        UPPER_LIMIT = 100
        LOWER_LIMIT = 10

        pm = self.pmIeees.merge(TPm, on=["ieee"], how="left").fillna(0)
        pm = pd.eval("chWattTotal = pm.ch1Watt+pm.ch2Watt+pm.ch3Watt", target=pm)
        pm = pm[(pm["chWattTotal"]>UPPER_LIMIT) | (pm["chWattTotal"]<LOWER_LIMIT)]
        
        pmCount = pm["ieee"].value_counts()
        pmCount = pmCount[pmCount > self.WARNING_AMOUNT]

        pmCount = pmCount.to_frame()
        pmCount.reset_index(inplace=True)
        pmCount = pmCount.rename(columns={"ieee":"count", "index":"ieee"})

        pmWarning = pmCount.merge(pm, on=["ieee"], how="left")
        return pmWarning

    def detectPowerMeter(self):
        pmWarning = self.pmAlgo(self.TPmYesterday)

        info = pmWarning.groupby(["ieee"]).apply(min)[["receivedSync", "gatewayId", "ieee", "deviceDesc", "count"]]
        # info = pd.concat([pmWarning, pmWarning[["ch1Watt", "ch2Watt", "ch3Watt", "chWattTotal"]].apply(lambda x: x.to_json(), axis=1)], axis=1).rename(columns={0:"metadata"})
        # info = pd.concat([info, info[["receivedSync", "metadata"]].apply(lambda x: x.to_json(), axis=1) ], axis=1).rename(columns={0:"metadataWithTs"})

        # To DB
        self.toDBProcessing(info, 2, "pm")

    def ainAlgo(self, TAin):
        # define limit variable
        AIN_PORT_1_LOWER_LIMIT = 2
        AIN_PORT_1_UPPER_LIMIT = 10
        AIN_PORT_2_LOWER_LIMIT = 2
        AIN_PORT_2_UPPER_LIMIT = 10
        AIN_PORT_3_LOWER_LIMIT = 2
        AIN_PORT_3_UPPER_LIMIT = 10
        AIN_PORT_4_LOWER_LIMIT = 2
        AIN_PORT_4_UPPER_LIMIT = 10
        AIN_PORT_5_LOWER_LIMIT = 2
        AIN_PORT_5_UPPER_LIMIT = 10

        ain = self.ainIeees.merge(TAin, on=["ieee"], how="left").fillna(0)
        ain = ain[
            ((ain["value1"]>AIN_PORT_1_UPPER_LIMIT)|(ain["value1"]<AIN_PORT_1_LOWER_LIMIT)) |
            ((ain["value2"]>AIN_PORT_2_UPPER_LIMIT)|(ain["value2"]<AIN_PORT_2_LOWER_LIMIT)) |
            ((ain["value3"]>AIN_PORT_3_UPPER_LIMIT)|(ain["value3"]<AIN_PORT_3_LOWER_LIMIT)) |
            ((ain["value4"]>AIN_PORT_4_UPPER_LIMIT)|(ain["value4"]<AIN_PORT_4_LOWER_LIMIT)) |
            ((ain["value5"]>AIN_PORT_5_UPPER_LIMIT)|(ain["value5"]<AIN_PORT_5_LOWER_LIMIT))
        ]

        ainCount = ain["ieee"].value_counts()
        ainCount = ainCount[ainCount > self.WARNING_AMOUNT]

        ainCount = ainCount.to_frame()
        ainCount.reset_index(inplace=True)
        ainCount = ainCount.rename(columns={"ieee":"count", "index":"ieee"})

        ainWarning = ainCount.merge(ain, on=["ieee"], how="left")
        return ainWarning

    def detectAin(self):
        ainWarning = self.ainAlgo(self.TAinYesterday)
        
        # To DB
        info = ainWarning.groupby(["ieee"]).apply(min)[["receivedSync", "gatewayId", "ieee", "deviceDesc", "count"]]
        self.toDBProcessing(info, 4, "ain")

    def inverterPR(self, TAin, TSolarAll, signal):
        # define limit variable
        UPPER_LIMIT = 100
        LOWER_LIMIT = 10

        radiator = self.radiatorIeees.merge(TAin, on=["ieee"], how="left")
        radiator = radiator[radiator["value1"]>=self.CRITICAL_THRESHOLD][['deviceTypeId', 'deviceAttribId', 'gatewayId', 'ieee', 'deviceDesc', 'receivedSync', 'value1']]
        panelTemp = self.panelTempIeees.merge(TAin, on=["ieee"], how="left")[['deviceTypeId', 'deviceAttribId', 'gatewayId', 'ieee', 'deviceDesc', 'receivedSync', 'value3']]
        radiatorPanelTemp = radiator.merge(panelTemp, on=["gatewayId", "receivedSync"], how="left")[["gatewayId", "receivedSync", "value1", "value3"]]

        solar = self.solarIeees.merge(TSolarAll, on=["ieee"], how="left")[["ieee", "gatewayId_x", "deviceDesc", "watt", "receivedSync", "totalActivePower"]].rename(columns={"gatewayId_x":"gatewayId"})
        mergedTbl = solar.merge(radiatorPanelTemp, on=["gatewayId", "receivedSync"], how="left")

        withoutPT = mergedTbl[(mergedTbl["value3"].isna())]
        withPT = mergedTbl[~(mergedTbl["value3"].isna())]
        if withoutPT.shape[0] != 0:
            PRTable1 = pd.eval("PR = 100 * (withPT.totalActivePower) / (withPT.watt * withPT.value1/1000) / (1-(withPT.value3-25)*0.0043)", target=withPT)
            PRTable2 = pd.eval("PR = 100 * (withoutPT.totalActivePower) / (withoutPT.watt * withoutPT.value1/1000)", target=withoutPT)
            PRTable = pd.concat([PRTable1, PRTable2]).sort_index()
        else:
            PRTable = pd.eval("PR = 100 * (withPT.totalActivePower) / (withPT.watt * withPT.value1/1000) / (1-(withPT.value3-25)*0.0043)", target=withPT)

        rule1 = PRTable[(PRTable["PR"]>UPPER_LIMIT) | (PRTable["PR"]<LOWER_LIMIT)].rename(columns={"value1":"radiatorValue", "value3":"panelTemp"})
        warningCount = rule1["ieee"].value_counts()
        warningCount = warningCount.to_frame()
        warningCount.reset_index(inplace=True)
        warningCount = warningCount.rename(columns={"ieee":"count", "index":"ieee"})
        warningCount = warningCount[warningCount["count"]>self.WARNING_AMOUNT]

        inverterWarning = warningCount.merge(rule1, on=["ieee"], how="left")
        
        # To DB
        if inverterWarning.shape[0] != 0:
            info = inverterWarning.groupby(["ieee"]).apply(min)[["receivedSync", "gatewayId", "deviceDesc", "count"]]
            info_metadata = pd.concat([inverterWarning, inverterWarning[["radiatorValue", "panelTemp", "PR"]].apply(lambda x: x.to_json(), axis=1)], axis=1).rename(columns={0:"metadata"})
            dataDict = {"ieee":[], "metadata":[]}
            for ieee in info_metadata.ieee.unique():
                tmpTbl = info_metadata[info_metadata.ieee==info_metadata.ieee.unique()[0]]
                dataDict["ieee"].append(ieee)
                dataDict["metadata"].append(dict([(i,j) for i, j in zip(tmpTbl.receivedSync, tmpTbl.metadata)]))
            metadataFrame = pd.DataFrame(dataDict)

            info = info.merge(metadataFrame, on=["ieee"], how="left")

            # self.toDBProcessing(info, signal, "pm")
            self.toPRDB(inverterWarning, "pm")

    def detectInverter(self):
        # self.inverterPR(TAin, TSolarAll, 5)
        self.inverterPR(self.TAinYesterday, self.TSolarYesterdayAll, 6)

    def detectPanelTemp(self, yesterdayValue=True):
        # define limit variable
        UPPER_LIMIT = 100
        LOWER_LIMIT = 10
        TEMP_COMPARE_RAD_THRESHOLD = 100 # 5
        TEMP_COMPARE_THRESHOLD = -1
        PANEL_TEMP_COEFF = 20 # gw 43: 25
        TEMP_DATA_DIFF_AMOUNT = 2
        PANEL_TEMP_DIFF_THRESHOLD = 2

        radiatorMean = self.radiatorIeees.merge(self.TAinYesterday, on=["ieee"], how="left").groupby(["ieee"]).mean()[['deviceTypeId', 'deviceAttribId', 'gatewayId', 'value1']]
        radiatorMean.reset_index(inplace=True)

        radiatorBelowThreshold = radiatorMean[radiatorMean["value1"]<=TEMP_COMPARE_RAD_THRESHOLD]

        if radiatorBelowThreshold.shape[0] != 0:
            panelTemp = self.panelTempIeees[self.panelTempIeees["gatewayId"].isin(radiatorBelowThreshold.gatewayId)].merge(self.TAin, on=["ieee"], how="left")[['deviceTypeId', 'deviceAttribId', 'gatewayId', 'ieee', 'deviceDesc', 'receivedSync', 'value3']]
            envTemp = self.envTempIeees[self.envTempIeees["gatewayId"].isin(radiatorBelowThreshold.gatewayId)].merge(self.TAin, on=["ieee"], how="left")[['deviceTypeId', 'deviceAttribId', 'gatewayId', 'ieee', 'deviceDesc', 'receivedSync', 'value4']]

            panelEnvTemp = panelTemp.merge(envTemp, on=["gatewayId", "receivedSync"])[['gatewayId', 'receivedSync', 'value3', 'value4']]
            panelEnvTemp = pd.eval("diff = (panelEnvTemp.value3 - panelEnvTemp.value4)", target=panelEnvTemp)

            panelEnvTempAvgDiff = panelEnvTemp.groupby(["gatewayId"]).mean()
            panelEnvTempAvgDiff = panelEnvTempAvgDiff.merge(panelEnvTemp.groupby(["gatewayId"]).apply(min)[["receivedSync"]], on=["gatewayId"])
            panelEnvTempAvgDiffWarning = panelEnvTempAvgDiff[panelEnvTempAvgDiff["diff"]>=TEMP_COMPARE_THRESHOLD]

            # if panelEnvTempAvgDiffWarning.shape[0] != 0:
            #     # To DB
            #     info = self.panelTempIeees.merge(panelEnvTempAvgDiffWarning, on=["gatewayId"])
            #     insertString = ""
            #     for _, row in info.iterrows():
            #         tempString = ""
            #         tempString = f"\
            #             '{row['receivedSync']}',\
            #             null,\
            #             '{row['gatewayId']}',\
            #             '{row['ieee']}',\
            #             '{row['deviceDesc']}', \
            #             '7',\
            #             '{self.TWarningSignal[self.TWarningSignal.id == 7].warningDesc.to_numpy()[0]}',\
            #             null,\
            #             '{json.dumps({'panelEnvTempDiff': abs(round(row['diff'], 3))})}' "
                        
            #         insertString += f"({tempString}) , "
            #     insertString = insertString[:-2]
            #     sqlstr = f"REPLACE INTO iotcomui.TWarningLog\
            #                 (`startTime`,`endTime`,`gatewayId`,`ieee`,`deviceDesc`,`warningSignal`,`warningDesc`,`warningAmount`,`abnormalDataLists`)\
            #                 values {insertString}"
            #     sqlstr = sqlstr.replace("'None'", 'null')
                
            #     conn = self.engines.engine_dict["iotmgmt"].connect()
            #     conn.execute(sql.text(sqlstr))

            #     print(f"panelEnvTempDiff detected.")
        else:
            pass

        # =================================================================================================== #
        if yesterdayValue:
            x1 = pd.eval(f"x = radiatorMean.value1 / 800 * {PANEL_TEMP_COEFF}", target=radiatorMean)
            x1 = x1.merge(panelEnvTempAvgDiff, on=["gatewayId"])
            y1 = pd.eval("y = x1.x + x1['diff']", target=x1)[["gatewayId", "y"]]

            envTemp_x = self.envTempIeees.merge(self.TAin, on=["ieee"], how="left")[['deviceTypeId', 'deviceAttribId', 'gatewayId', 'ieee', 'deviceDesc', 'receivedSync', 'value4']]
            idealPanelTemp = envTemp_x.merge(y1, on=["gatewayId"], how="left")
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
                info_1 = info[(info["ieee"].isin(info.groupby(["ieee"]).apply(min)[["ieee", "gatewayId", "receivedSync"]].ieee)) & (info["receivedSync"].isin(info.groupby(["ieee"]).apply(min)[["ieee", "gatewayId", "receivedSync"]].receivedSync))]
                info_2 = info_1.merge(infoCount, on=["ieee"])
                info_3 = pd.concat([info_2, info_2[["idealPanelTemp", "truePanelTemp", "realIdealDiff"]].apply(lambda x: x.to_json(), axis=1)], axis=1).rename(columns={0:"metadata"})
                
                self.toDBProcessing(info_3, 8, "panelEnvTempDiff")

    def detectAll(self):
        # self.detectPowerMeter()
        # self.detectAin()
        # self.detectInverter()
        self.detectPanelTemp()

    