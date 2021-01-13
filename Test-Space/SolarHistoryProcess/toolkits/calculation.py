import numpy as np
import math
import json
import datetime

class Cal():
    def __init__(self, 
                 processStart=None,
                 processEnd=None, 
                 environment_dbARCNum=None,
                 historyDataCWB_dbCWBNum=None, 
                 _insolationIEEE=None, 
                 _moduleTempIEEE=None, 
                 InvInfo=None, 
                 _panelEfficiencySet=None, 
                 _panelTempSet=None, 
                 solarInverter=None,
                 solarMpptPowerGeneration_tb=None):
        self.processStart = processStart 
        self.processEnd = processEnd 
        self.environment_dbARCNum = environment_dbARCNum 
        self.historyDataCWB_dbCWBNum = historyDataCWB_dbCWBNum 
        self._insolationIEEE = _insolationIEEE 
        self._moduleTempIEEE = _moduleTempIEEE 
        self.InvInfo = InvInfo 
        self._panelEfficiencySet = _panelEfficiencySet 
        self._panelTempSet = _panelTempSet 
        self.solarInverter = solarInverter
        self.solarMpptPowerGeneration_tb = solarMpptPowerGeneration_tb
    
    def getMPPTNum(self, _id, invType):
        # invType == 5 (華為)
        MPPT = {}
        if _id == "A":
            if invType == 5:
                MPPT["mpptNum"] = 1
                MPPT["mpptCurrentNum1"] = 1
                MPPT["mpptCurrentNum2"] = 2
            else:
                MPPT["mpptNum"] = 1
        elif _id == "B":
            if invType == 5:
                MPPT["mpptNum"] = 3
                MPPT["mpptCurrentNum1"] = 3
                MPPT["mpptCurrentNum2"] = 4
            else:
                MPPT["mpptNum"] = 2
        elif _id == "C":
            if invType == 5:
                MPPT["mpptNum"] = 5
                MPPT["mpptCurrentNum1"] = 5
                MPPT["mpptCurrentNum2"] = 6
            else:
                MPPT["mpptNum"] = 3
        elif _id == "D":
            if invType == 5:
                MPPT["mpptNum"] = 7
                MPPT["mpptCurrentNum1"] = 7
                MPPT["mpptCurrentNum2"] = 8
            else:
                MPPT["mpptNum"] = 4
        
        return MPPT
    
    def calRealIrradiation(self):
        # ======= 日照 realIrradiation ======= #
        realIrradiation_list = self.environment_dbARCNum[(self.environment_dbARCNum["receivedSync"] >= self.processStart) & \
                                                         (self.processEnd > self.environment_dbARCNum["receivedSync"]) & \
                                                         (self.environment_dbARCNum["ieee"]==self._insolationIEEE)].groupby(["receivedSync"]).mean()["value5"].values
        if realIrradiation_list.size == 0:
            realIrradiation = None
        else:
            realIrradiation = round(np.average(realIrradiation_list)/1000,3)
        return realIrradiation

    def calRealPanelTemperature(self):
        realPanelTemperature_list = self.environment_dbARCNum[(self.environment_dbARCNum["receivedSync"] >= self.processStart) & \
                                                              (self.processEnd > self.environment_dbARCNum["receivedSync"]) & \
                                                              (self.environment_dbARCNum["ieee"]==self._moduleTempIEEE)].groupby(["receivedSync"]).mean()["value3"].values
        if realPanelTemperature_list.size == 0:
            realPanelTemperature = None 
        else:
            realPanelTemperature = round(np.average(realPanelTemperature_list), 3)
        return realPanelTemperature

    def calBudgetPowerGeneration(self):
        tmp = self.historyDataCWB_dbCWBNum[(self.historyDataCWB_dbCWBNum["ts"] >= self.processStart.replace(year=2020)) & \
                                           (self.processEnd.replace(year=2020) > self.historyDataCWB_dbCWBNum["ts"]) & \
                                           (self.historyDataCWB_dbCWBNum["cityId"] == self.InvInfo["cityId"])]
            
        _azimuth = tmp["azimuth"].values[0]
        _inclination = tmp["inclination"].values[0]
        _budtTemp = tmp["budtTemp"].values[0]
        _budtDirectInsolation = tmp["budtDirectInsolation"].values[0]
        _azimuthInsolation = round(_budtDirectInsolation*math.cos((_azimuth-self.InvInfo['azimuth'])*0.0175),3)

        if _azimuthInsolation > 0:
            _budgetAzimuthInclinationInsolation =             math.sqrt(
            math.pow(_azimuthInsolation*math.sin((_inclination+self.InvInfo['inclination'])*0.0175),2)
            +
            math.pow(_budtDirectInsolation*math.cos((_azimuth-self.InvInfo['azimuth']-90)*0.0175)*math.sin(_inclination*0.0175)*math.cos(self.InvInfo['inclination']*0.0175),2)
            )
        else:
            _budgetAzimuthInclinationInsolation =             math.sqrt(
            math.pow(_azimuthInsolation*math.sin((_inclination-self.InvInfo['inclination'])*0.0175),2)
            +
            math.pow(_budtDirectInsolation*math.cos((_azimuth-self.InvInfo['azimuth']-90)*0.0175)*math.sin(_inclination*0.0175)*math.cos(self.InvInfo['inclination']*0.0175),2)
            )

        budgetPowerGeneration = round((_budgetAzimuthInclinationInsolation*self._panelEfficiencySet)*(1-(_budgetAzimuthInclinationInsolation*25.3/0.8+_budtTemp-25)*self._panelTempSet)*0.9835*self.InvInfo['efficiency'], 3)
        return budgetPowerGeneration

    def calReferencePowerGeneration(self, realIrradiation, realPanelTemperature):
        tmp = self.historyDataCWB_dbCWBNum[(self.historyDataCWB_dbCWBNum["ts"] >= self.processStart.replace(year=2020)) & \
                                           (self.processEnd.replace(year=2020) > self.historyDataCWB_dbCWBNum["ts"]) & \
                                           (self.historyDataCWB_dbCWBNum["cityId"] == self.InvInfo["cityId"])]
        
        _refDirectInsolation = round(tmp["oneSIN"]*realIrradiation, 3).values[0]
        _referenceAzimuthIrradiation = round((tmp["oneSIN"].values[0]*realIrradiation)*math.cos((tmp["azimuth"].values[0]-self.InvInfo["azimuth"])*0.0175),3)
        
        if _referenceAzimuthIrradiation > 0:
            _refAzimuthInclinationInsolation =             math.sqrt(
            math.pow(_referenceAzimuthIrradiation*math.sin((tmp["inclination"].values[0]+self.InvInfo["inclination"])*0.0175), 2)
            +\
            math.pow(_refDirectInsolation*math.cos((tmp["azimuth"].values[0]-self.InvInfo["azimuth"]-90)*0.0175)*math.sin(tmp["inclination"].values[0]*0.0175)*math.cos(self.InvInfo["inclination"]*0.0175), 2)
            )
        else:
            _refAzimuthInclinationInsolation =             math.sqrt(
            math.pow(_referenceAzimuthIrradiation*math.sin((tmp["inclination"].values[0]-self.InvInfo["inclination"])*0.0175), 2)
            +\
            math.pow(_refDirectInsolation*math.cos((tmp["azimuth"].values[0]-self.InvInfo["azimuth"]-90)*0.0175)*math.sin(tmp["inclination"].values[0]*0.0175)*math.cos(self.InvInfo["inclination"]*0.0175), 2)
            )

        referencePowerGeneration = round((_refAzimuthInclinationInsolation*self._panelEfficiencySet)*(1-(realPanelTemperature-25)*self._panelTempSet)*0.9835*self.InvInfo['efficiency'], 3)
        return referencePowerGeneration

    # 改成 < 試看看
    def calRealPowerGeneration(self):
        mpptNum = self.getMPPTNum(self.InvInfo["mpptId"], self.InvInfo["invTypeId"])
        tmp = self.solarInverter[(self.solarInverter["receivedSync"] >= self.processStart) &
                                 (self.processEnd > self.solarInverter["receivedSync"]) & 
                                 (self.solarInverter["ieee"]==self.InvInfo['ieee'])]
        if tmp.shape[0] == 0:
            realPowerGeneration = 0
        else:
            # 亞力
            if self.InvInfo["invTypeId"] in [1,2,3,4]:
                mpptNum = mpptNum["mpptNum"]
                dc_list = []
                for vtg, crt in zip(tmp["dcVoltage"], tmp["dcCurrent"]):
                    if isinstance(vtg, str):
                        vtg = json.loads(vtg)
                        crt = json.loads(crt)
                    dc_list.append(vtg[f"voltage{mpptNum}"] * crt[f"current{mpptNum}"])
                avg_dc = sum(dc_list)/len(dc_list)
                realPowerGeneration = round(avg_dc / self.InvInfo['mpptInstCapacity'], 3)
            # 華為
            elif self.InvInfo["invTypeId"] == 5:
                mpptNum, mpptCurrentNum1, mpptCurrentNum2 = mpptNum["mpptNum"], mpptNum["mpptCurrentNum1"], mpptNum["mpptCurrentNum2"]
                
                dc_list = []
                for vtg, crt in zip(tmp["dcVoltage"], tmp["dcCurrent"]):
                    if isinstance(vtg, str):
                        vtg = json.loads(vtg)
                        crt = json.loads(crt)
                    dc_list.append(vtg[f"voltage{mpptNum}"] * crt[f"current{mpptNum}"])
                avg_dc = sum(dc_list)/len(dc_list)
                realPowerGeneration = round(avg_dc / self.InvInfo['mpptInstCapacity'], 3)
            
            realPowerGeneration = realPowerGeneration * self.InvInfo['efficiency']
            if realPowerGeneration > 1.5:
                    realPowerGeneration = None
        return realPowerGeneration
    
    def calStationPowerGeneration(self):
        tmp = self.historyDataCWB_dbCWBNum[(self.historyDataCWB_dbCWBNum["ts"] >= self.processStart.replace(year=2020)) & \
                                           (self.processEnd.replace(year=2020) > self.historyDataCWB_dbCWBNum["ts"]) & \
                                           (self.historyDataCWB_dbCWBNum["cityId"] == self.InvInfo["cityId"])]
        
        _stationAzimuthIrradiation = round(tmp["refDirectInsolation"].values[0]*math.cos((tmp["azimuth"].values[0]-self.InvInfo["azimuth"])*0.0175), 3)
        
        if _stationAzimuthIrradiation > 0:
            _stationAzimuthInclinationInsolation =             math.sqrt(
            math.pow(_stationAzimuthIrradiation*math.sin((tmp["inclination"].values[0]+self.InvInfo['inclination'])*0.0175), 2)
            +
            math.pow(tmp["refDirectInsolation"].values[0]*math.cos((tmp["azimuth"].values[0]-self.InvInfo['azimuth']-90)*0.0175)*math.sin(tmp["inclination"].values[0]*0.0175)*math.cos(self.InvInfo['inclination']*0.0175), 2)
            )
        else:
            _stationAzimuthInclinationInsolation =             math.sqrt(
            math.pow(_stationAzimuthIrradiation*math.sin((tmp["inclination"].values[0]-self.InvInfo['inclination'])*0.0175), 2)
            +
            math.pow(tmp["refDirectInsolation"].values[0]*math.cos((tmp["azimuth"].values[0]-self.InvInfo['azimuth']-90)*0.0175)*math.sin(tmp["inclination"].values[0]*0.0175)*math.cos(self.InvInfo['inclination']*0.0175), 2)
            )

        stationPowerGeneration = round((_stationAzimuthInclinationInsolation*self._panelEfficiencySet)*(1-(_stationAzimuthInclinationInsolation*25.3/0.8+tmp["refTemp"].values[0]-25)*self._panelTempSet)*0.9835*self.InvInfo['efficiency'], 3)
        return stationPowerGeneration
    
    def calPredictPowerGeneration(self):
        mpptNum = self.getMPPTNum(self.InvInfo["mpptId"], self.InvInfo["invTypeId"])
        # realKW
        tmp = self.solarMpptPowerGeneration_tb[(self.solarMpptPowerGeneration_tb["ts"] >= self.processStart-datetime.timedelta(minutes=30)) &
                                             (self.processStart-datetime.timedelta(minutes=16) >= self.solarMpptPowerGeneration_tb["ts"]) &
                                             (self.solarMpptPowerGeneration_tb["inverterId"] == self.InvInfo["inverterId"])]
        if tmp.shape[0] == 0:
            _realKW = 0.0
        else:
            # 亞力
            if self.InvInfo["invTypeId"] in [1,2,3,4]:
                _realKW = tmp["predictPowerGeneration"].values.mean()
            # 華為
            elif self.InvInfo["invTypeId"] == 5:
                _realKW = tmp["predictPowerGeneration"].values.mean()

        # tmp = self.solarInverter[(self.solarInverter["receivedSync"] >= self.processStart-datetime.timedelta(minutes=30)) &
        #                          (self.processStart-datetime.timedelta(minutes=16) >= self.solarInverter["receivedSync"]) &
        #                          (self.solarInverter["ieee"] == self.InvInfo["ieee"])]
        # if tmp.shape[0] == 0:
        #     _realKW = 0.0
        # else:
        #     # 亞力
        #     if self.InvInfo["invTypeId"] in [1,2,3,4]:
        #         mpptNum = mpptNum["mpptNum"]
        #         dc_list = []
        #         for vtg, crt in zip(tmp["dcVoltage"], tmp["dcCurrent"]):
        #             if isinstance(vtg, str):
        #                 vtg = json.loads(vtg)
        #                 crt = json.loads(crt)
        #             dc_list.append(vtg[f"voltage{mpptNum}"] * crt[f"current{mpptNum}"])
        #         avg_dc = sum(dc_list)/len(dc_list)
        #         _realKW = round(avg_dc / self.InvInfo['mpptInstCapacity'], 3)
        #     # 華為
        #     elif self.InvInfo["invTypeId"] == 5:
        #         mpptNum, mpptCurrentNum1, mpptCurrentNum2 = mpptNum["mpptNum"], mpptNum["mpptCurrentNum1"], mpptNum["mpptCurrentNum2"]
        #         dc_list = []
        #         realKw = []
        #         for crt in tmp["dcCurrent"]:
        #             if isinstance(crt, str):
        #                 crt = json.loads(crt)
        #             dc_list.append(crt[f"current{mpptCurrentNum1}"] + crt[f"current{mpptCurrentNum2}"])
        #         avg_dc = sum(dc_list)/len(dc_list)

        #         for vtg in tmp["dcVoltage"]:
        #             if isinstance(vtg, str):
        #                 vtg = json.loads(vtg)
        #             realKw.append(vtg[f"voltage{mpptNum}"] * avg_dc)
        #         avg_realKw = sum(realKw)/len(realKw)
        #         _realKW = round(avg_realKw / self.InvInfo['mpptInstCapacity'], 3)
        
        # preOneSIN
        tmp = self.historyDataCWB_dbCWBNum[(self.historyDataCWB_dbCWBNum["ts"] >= (self.processStart-datetime.timedelta(minutes=30)).replace(year=2020)) & ((self.processStart-datetime.timedelta(minutes=16)).replace(year=2020) > self.historyDataCWB_dbCWBNum["ts"]) & (self.historyDataCWB_dbCWBNum["cityId"] == self.InvInfo["cityId"])]
        if tmp.shape[0] == 0:
            _preOneSIN = 0.0
        else:
            _preOneSIN = np.average(tmp["oneSIN"].values)
            
        # postOneSIN
        tmp = self.historyDataCWB_dbCWBNum[(self.historyDataCWB_dbCWBNum["ts"] >= self.processStart.replace(year=2020)) & ((self.processStart+datetime.timedelta(minutes=16)).replace(year=2020) > self.historyDataCWB_dbCWBNum["ts"]) & (self.historyDataCWB_dbCWBNum["cityId"] == self.InvInfo["cityId"])]
        if tmp.shape[0] == 0:
            _postOneSIN = 0.0
        else:
            _postOneSIN = np.average(tmp["oneSIN"].values)
        
        # final
        if _postOneSIN == 0:
            predictPowerGeneration = 0.0
        else:
            predictPowerGeneration = round((_realKW * _preOneSIN) / _postOneSIN, 3)

        return predictPowerGeneration