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
    dbARC = "archiveplatform"

    # Engine
    archiveplatform_engine = sql.create_engine(f"mysql+mysqldb://{user}:{pwd}@{host}/{dbARC}?charset=utf8", pool_recycle=3600*7)

    print("Start processing.")

    # insert into solarInvPowerGeneration
    solarInv_sql = f"\
        SELECT  \
            ts, \
            a.siteId, \
            a.groupId, \
            a.inverterId, \
            inverterDescription, \
            ROUND(SUM(realPowerGeneration * mpptInstCapacity) / SUM(mpptInstCapacity), \
                    3) AS realPowerGeneration, \
            ROUND(SUM(budgetPowerGeneration * mpptInstCapacity) / SUM(mpptInstCapacity), \
                    3) AS budgetPowerGeneration, \
            ROUND(SUM(referencePowerGeneration * mpptInstCapacity) / SUM(mpptInstCapacity), \
                    3) AS referencePowerGeneration, \
            ROUND(SUM(stationPowerGeneration * mpptInstCapacity) / SUM(mpptInstCapacity), \
                    3) AS stationPowerGeneration, \
            ROUND(SUM(predictPowerGeneration * mpptInstCapacity) / SUM(mpptInstCapacity), \
                    3) AS predictPowerGeneration, \
            realIrradiation, \
            realPanelTemperature \
        FROM \
            archiveplatform.solarMpptPowerGeneration AS a, \
            uiplatform.TInverterMppt AS b \
        WHERE \
            a.inverterId = b.inverterId \
                AND a.mpptId = b.mpptId \
                AND ts >= '{todayStart}' \
                AND ts < '{todayEnd}' \
        GROUP BY a.inverterId , a.siteId , ts \
    "
    solarInvPowerGeneration = pd.read_sql(solarInv_sql, con=archiveplatform_engine)
    if insert:
        solarInvPowerGeneration.to_sql('solarInvPowerGeneration', con=archiveplatform_engine, if_exists='append', index=False)
        print("[Table Insert] solarInvPowerGeneration")
        
    #insert into group
    # insert into solarGroupPowerGeneration
    solarGroup_sql = f"\
        SELECT  \
            ts, \
            a.siteId, \
            a.groupId, \
            ROUND(SUM(realPowerGeneration * instCapacity) / SUM(instCapacity), \
                    3) AS realPowerGeneration, \
            ROUND(SUM(budgetPowerGeneration * instCapacity) / SUM(instCapacity), \
                    3) AS budgetPowerGeneration, \
            ROUND(SUM(referencePowerGeneration * instCapacity) / SUM(instCapacity), \
                    3) AS referencePowerGeneration, \
            ROUND(SUM(stationPowerGeneration * instCapacity) / SUM(instCapacity), \
                    3) AS stationPowerGeneration, \
            ROUND(SUM(predictPowerGeneration * instCapacity) / SUM(instCapacity), \
                    3) AS predictPowerGeneration, \
            realIrradiation, \
            realPanelTemperature \
        FROM \
            archiveplatform.solarInvPowerGeneration AS a, \
            uiplatform.TSiteInverter AS b \
        WHERE \
            a.siteId = b.siteId \
                AND a.groupId = b.groupId \
                AND a.inverterId = b.inverterId \
                AND ts >= '{todayStart}' \
                AND ts < '{todayEnd}' \
        GROUP BY a.groupId , a.siteId , ts \
    "
    solarGroupPowerGeneration = pd.read_sql(solarGroup_sql, con=archiveplatform_engine)
    if insert:
        solarGroupPowerGeneration.to_sql('solarGroupPowerGeneration', con=archiveplatform_engine, if_exists='append', index=False)
        print("[Table Insert] solarGroupPowerGeneration")

    #insert into site
    # insert into solarSitePowerGeneration
    solarSite_sql = f"\
        SELECT  \
            ts, \
            a.siteId, \
            ROUND(SUM(realPowerGeneration * instCapacity) / SUM(instCapacity), \
                    3) AS realPowerGeneration, \
            ROUND(SUM(budgetPowerGeneration * instCapacity) / SUM(instCapacity), \
                    3) AS budgetPowerGeneration, \
            ROUND(SUM(referencePowerGeneration * instCapacity) / SUM(instCapacity), \
                    3) AS referencePowerGeneration, \
            ROUND(SUM(stationPowerGeneration * instCapacity) / SUM(instCapacity), \
                    3) AS stationPowerGeneration, \
            ROUND(SUM(predictPowerGeneration * instCapacity) / SUM(instCapacity), \
                    3) AS predictPowerGeneration, \
            realIrradiation, \
            realPanelTemperature \
        FROM \
            archiveplatform.solarInvPowerGeneration AS a, \
            uiplatform.TSiteInverter AS b \
        WHERE \
            a.siteId = b.siteId \
                AND a.groupId = b.groupId \
                AND a.inverterId = b.inverterId \
                AND ts >= '{todayStart}' \
                AND ts < '{todayEnd}' \
        GROUP BY a.siteId , ts \
    "
    solarSitePowerGeneration = pd.read_sql(solarSite_sql, con=archiveplatform_engine)
    if insert:
        solarSitePowerGeneration.to_sql('solarSitePowerGeneration', con=archiveplatform_engine, if_exists='append', index=False)
        print("[Table Insert] solarSitePowerGeneration")
    
    return 0

if __name__ == "__main__":
    sId = 7
    dataDate = "2020-08-18"
    main(sId, dataDate, insert=True)
    # sites = [1,2,3,4,5,6,7,15,16]
    # for sId in sites:
    #     dataDate = "2020-07-14"
    #     main(sId, dataDate, insert=True)
