import pandas as pd
import datetime
import sqlalchemy as sql

def getDevices(engine):
    sql_query = "\
        SELECT \
            c.siteId,\
            b.buildingName,\
            SUBSTRING_INDEX(a.deviceDesc, '#', - 1) AS coolingId,\
            typeName,\
            ieee,\
            deviceDesc,\
            c.gatewayId,\
            b.TDSofCTMakeupWater\
        FROM\
            iotmgmt.vDeviceInfo a,\
            reportplatform.site_info b,\
            reportplatform.gateway_info c\
        WHERE\
            a.gatewayId = c.gatewayId\
                AND c.siteId = b.id\
                AND typeName IN ('ORP' , 'pH', 'TDS');\
    "
    return pd.read_sql_query(sql.text(sql_query), con=engine)

def getWaterQualityData(engine, ieees, ts):
    # correct one
    # sql_query = "\
    #     SELECT * FROM iotmgmt.waterQuality where ieee in (SELECT ieee FROM iotmgmt.TDevice where deviceTypeId in (91,92,93)) and date(receivedSync) = CURDATE() order by receivedSync desc ;\
    # "

    # test purpose
    sql_query = f"\
        SELECT \
            gatewayId, receivedSync, ieee, ph, oxidationReductionPotential, totalDissovedSolids, HOUR(receivedSync) AS H\
        FROM\
            iotmgmt.waterQuality\
        WHERE\
            ieee IN {tuple(ieees)}\
        AND DATE(receivedSync) >= '{ts}';\
    "
    return pd.read_sql_query(sql.text(sql_query), con=engine)
