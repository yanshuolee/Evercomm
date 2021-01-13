import numpy as np
import math
import json
import datetime
import pandas as pd
import sqlalchemy as sql

def Q1(x): return np.quantile(x, q=0.25)
def Q2(x): return np.quantile(x, q=0.5)
def Q3(x): return np.quantile(x, q=0.75)

def generateRecord(dataset, col):
    record = {}
    tblData = dataset[["ieee", "H", col]].groupby(["ieee", "H"]).agg([min,Q1,Q2,Q3,max])
    if tblData.size == 0:
        return record, tblData

    tblDaily = tblData[[(col,'Q1'),(col,'Q2'),(col,'Q3')]].groupby(["ieee"]).agg("mean")
    for idx, data in tblData.iterrows():
        ieee, hour = idx
        data = data[col].to_dict()
        if ieee not in record:
            record[ieee] = {}
        record[ieee][str(hour)] = data

    return record, tblDaily

def toDB(operationDate, sitePairs, TDevice, pHData, pHDaily, orpData, orpDaily, tdsData, tdsDaily, cocData, cocDaily, engine, insert=False):
    insertString = ""
    for siteId, coolingId in sitePairs:
        tempString = ""
        info = TDevice[(TDevice["siteId"]==siteId) & (TDevice["coolingId"]==coolingId)]
        
        dailyAnalysis = {
            "25th" : {
                "COC" : cocDaily[cocDaily.index.isin(info["ieee"])][("cyclesOfConcentration", "Q1")].values[0],
                "ORP" : orpDaily[orpDaily.index.isin(info["ieee"])][("oxidationReductionPotential", "Q1")].values[0],
                "TDS" : tdsDaily[tdsDaily.index.isin(info["ieee"])][("totalDissovedSolids", "Q1")].values[0],
                "ph" : pHDaily[pHDaily.index.isin(info["ieee"])][("ph", "Q1")].values[0]
            },
            "50th" : {
                "COC" : cocDaily[cocDaily.index.isin(info["ieee"])][("cyclesOfConcentration", "Q2")].values[0],
                "ORP" : orpDaily[orpDaily.index.isin(info["ieee"])][("oxidationReductionPotential", "Q2")].values[0],
                "TDS" : tdsDaily[tdsDaily.index.isin(info["ieee"])][("totalDissovedSolids", "Q2")].values[0],
                "ph" : pHDaily[pHDaily.index.isin(info["ieee"])][("ph", "Q2")].values[0]
            },
            "75th" : {
                "COC" : cocDaily[cocDaily.index.isin(info["ieee"])][("cyclesOfConcentration", "Q3")].values[0],
                "ORP" : orpDaily[orpDaily.index.isin(info["ieee"])][("oxidationReductionPotential", "Q3")].values[0],
                "TDS" : tdsDaily[tdsDaily.index.isin(info["ieee"])][("totalDissovedSolids", "Q3")].values[0],
                "ph" : pHDaily[pHDaily.index.isin(info["ieee"])][("ph", "Q3")].values[0]
            }
        }

        dailyAnalysis = json.dumps(dailyAnalysis, ensure_ascii=False)
        pHPerHour = json.dumps(pHData[info[info["typeName"]=="pH"].ieee.values[0]], ensure_ascii=False)
        orpPerHour = json.dumps(orpData[info[info["typeName"]=="ORP"].ieee.values[0]], ensure_ascii=False)
        tdsPerHour = json.dumps(tdsData[info[info["typeName"]=="TDS"].ieee.values[0]], ensure_ascii=False)
        cocPerHour = json.dumps(cocData[info[info["typeName"]=="TDS"].ieee.values[0]], ensure_ascii=False)
        
        tempString = f"\
            '{operationDate}',\
            '{siteId}',\
            '{info['gatewayId'].values[0]}',\
            '{coolingId}',\
            'CoolingTower#{coolingId}',\
            '{dailyAnalysis}',\
            '{pHPerHour}',\
            '{orpPerHour}',\
            '{tdsPerHour}',\
            '{cocPerHour}'\
        "

        insertString += f"({tempString}) , "
    insertString = insertString[:-2]

    sqlstr = f"REPLACE INTO `reportplatform`.`dailyCoolingWaterQuality`\
              (`operationDate`,\
              `siteId`,\
              `gatewayId`,\
              `coolingId`,\
              `coolingDescription`,\
              `dailyAnalysis`,\
              `phPerHour`,\
              `oxidationReductionPotentialPerHour`,\
              `totalDissovedSolidsPerHour`,\
              `cycleOfConcentrationPerHour`) values {insertString}"

    if insert:
        conn = engine.connect()
        conn.execute(sql.text(sqlstr))
        print("dailyCoolingWaterQuality inserted.")

def dailyCoolingWaterQuality(TDevice, waterQualityData, engine, insert):
    """
    91	Water Quality - pH
    92	Water Quality - ORP
    93	Water Quality - TDS
    """
    waterQualityData = TDevice.merge(waterQualityData, on=["ieee", "gatewayId"], how="left")
    # ph
    pHData, pHDaily = generateRecord(waterQualityData[waterQualityData["typeName"]=="pH"], col="ph")

    # oxidationReductionPotential
    orpData, orpDaily = generateRecord(waterQualityData[waterQualityData["typeName"]=="ORP"], col="oxidationReductionPotential")

    # totalDissovedSolids
    tdsData, tdsDaily = generateRecord(waterQualityData[waterQualityData["typeName"]=="TDS"], col="totalDissovedSolids")

    # COC
    coc = waterQualityData[waterQualityData["typeName"]=="TDS"]
    coc = pd.eval("cyclesOfConcentration = coc.totalDissovedSolids / coc.TDSofCTMakeupWater", target=coc)
    cocData, cocDaily = generateRecord(coc, col="cyclesOfConcentration")

    sitePairs = TDevice[["siteId", "coolingId"]].groupby(["siteId", "coolingId"]).size().index
    
    operationDate = str(waterQualityData["receivedSync"][0].date())
    toDB(operationDate, sitePairs, TDevice, pHData, pHDaily, orpData, orpDaily, tdsData, tdsDaily, cocData, cocDaily, engine, insert=insert)
