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

    # Engine
    processplatform_engine = sql.create_engine(f"mysql+mysqldb://{user}:{pwd}@{host}/{dbProcessPF}", pool_recycle=3600*7)
    uiplatform_engine = sql.create_engine(f"mysql+mysqldb://{user}:{pwd}@{host}/{dbUi}", pool_recycle=3600*7)
    ui_85_engine = sql.create_engine(f'mysql+mysqldb://{user}:{pwd}@{host}/{dbProcessPF}', pool_recycle=3600*7)

    print("Start processing.")

    #insert into Inverter
    # insert into solarInvPowerGeneration
    solarInv_sql = f"SELECT ts,   a.siteId,   a.groupId,   a.inverterId,   inverterDescription,   round(sum(realPowerGeneration*mpptInstCapacity)/sum(mpptInstCapacity),3) as realPowerGeneration,   round(sum(budgetPowerGeneration*mpptInstCapacity)/sum(mpptInstCapacity),3) as budgetPowerGeneration,   round(sum(referencePowerGeneration*mpptInstCapacity)/sum(mpptInstCapacity),3)  as referencePowerGeneration,   round(sum(stationPowerGeneration*mpptInstCapacity)/sum(mpptInstCapacity),3) as stationPowerGeneration,   round(sum(predictPowerGeneration*mpptInstCapacity)/sum(mpptInstCapacity),3) as predictPowerGeneration,   realIrradiation,   realPanelTemperature FROM     processplatform.solarMpptPowerGeneration as a,     uiplatform.TInverterMppt as b where     a.inverterId=b.inverterId and     a.mpptId=b.mpptId and     a.siteId={sId} and     ts>='{todayStart}' and     ts<'{todayEnd}' group by a.inverterId, ts"
    solarInvPowerGeneration = pd.read_sql(solarInv_sql, con=processplatform_engine)
    if insert:
        solarInvPowerGeneration.to_sql('solarInvPowerGeneration', con=ui_85_engine, if_exists='append', index=False)
        
    #insert into group
    # insert into solarGroupPowerGeneration
    solarGroup_sql = f"SELECT ts,     a.siteId,     a.groupId,     round(sum(realPowerGeneration*instCapacity)/sum(instCapacity),3) as realPowerGeneration,     round(sum(budgetPowerGeneration*instCapacity)/sum(instCapacity),3) as budgetPowerGeneration,     round(sum(referencePowerGeneration*instCapacity)/sum(instCapacity),3)  as referencePowerGeneration,     round(sum(stationPowerGeneration*instCapacity)/sum(instCapacity),3) as stationPowerGeneration,     round(sum(predictPowerGeneration*instCapacity)/sum(instCapacity),3) as predictPowerGeneration,     realIrradiation,     realPanelTemperature FROM     processplatform.solarInvPowerGeneration as a,     uiplatform.TSiteInverter as b where     a.siteId=b.siteId and     a.groupId=b.groupId and     a.inverterId=b.inverterId and     a.siteId={sId} and     ts>='{todayStart}' and     ts<'{todayEnd}' group by a.groupId, ts"
    solarGroupPowerGeneration = pd.read_sql(solarGroup_sql, con=processplatform_engine)
    if insert:
        solarGroupPowerGeneration.to_sql('solarGroupPowerGeneration', con=ui_85_engine, if_exists='append', index=False)

    #insert into site
    # insert into solarSitePowerGeneration
    solarSite_sql = f"SELECT ts,     a.siteId,     round(sum(realPowerGeneration*instCapacity)/sum(instCapacity),3) as realPowerGeneration,     round(sum(budgetPowerGeneration*instCapacity)/sum(instCapacity),3) as budgetPowerGeneration,     round(sum(referencePowerGeneration*instCapacity)/sum(instCapacity),3)  as referencePowerGeneration,     round(sum(stationPowerGeneration*instCapacity)/sum(instCapacity),3) as stationPowerGeneration,     round(sum(predictPowerGeneration*instCapacity)/sum(instCapacity),3) as predictPowerGeneration,     realIrradiation,     realPanelTemperature FROM     processplatform.solarInvPowerGeneration as a,     uiplatform.TSiteInverter as b WHERE     a.siteId=b.siteId and     a.groupId=b.groupId and     a.inverterId=b.inverterId and     a.siteId={sId} and     ts>='{todayStart}' and     ts<'{todayEnd}' group by a.siteId,ts"
    solarSitePowerGeneration = pd.read_sql(solarSite_sql, con=processplatform_engine)
    if insert:
        solarSitePowerGeneration.to_sql('solarSitePowerGeneration', con=ui_85_engine, if_exists='append', index=False)

    print("solarInvPowerGeneration / solarGroupPowerGeneration / solarSitePowerGeneration insert successfully.")
    
    return 0

if __name__ == "__main__":
    sId = 5
    dataDate = "2020-06-14"
    main(sId, dataDate, insert=False)
