import pandas as pd
import datetime
import sqlalchemy as sql
import math

def getIeeeListsForDQ(engine):
    sql_query = "\
        SELECT \
            ieee,\
            b.gatewayId,\
            deviceDesc,\
            CONCAT(b.gatewayId, '_', deviceDesc) AS deviceId,\
            a.warningCategoryId,\
            sensorType,\
            sensorTable,\
            sensorUpperLimit,\
            sensorDataQualityThreshold,\
            RadiationId,\
            PanelTempId,\
            sensorWorkingStartTime,\
            sensorWorkingEndTime,\
            siteTypeId,\
            criticalIeee,\
            criticalThreshold\
        FROM\
            iotcomui.TDeviceWarning AS a,\
            iotmgmt.TDevice AS b,\
            iotcomui.TGatewayWarning AS c,\
            iotcomui.TWarningCategory AS d\
        WHERE\
            a.deviceId = b.id\
            AND b.gatewayId = c.gatewayId\
            AND a.warningCategoryId = d.warningCategoryId\
    "
    return pd.read_sql_query(sql.text(sql_query), con=engine)

def getIeeeListsForWD(engine):
    sql_query = "\
        SELECT \
            ieee,\
            b.gatewayId,\
            deviceDesc,\
            CONCAT(b.gatewayId, '_', deviceDesc) AS deviceId,\
            a.warningCategoryId,\
            sensorType,\
            sensorTable,\
            sensorLowerLimit,\
            sensorUpperLimit,\
            ainPort1LowerLimit,\
            ainPort1UpperLimit,\
            ainPort2LowerLimit,\
            ainPort2UpperLimit,\
            ainPort3LowerLimit,\
            ainPort3UpperLimit,\
            ainPort4LowerLimit,\
            ainPort4UpperLimit,\
            ainPort5LowerLimit,\
            ainPort5UpperLimit,\
            sensorDataQualityThreshold,\
            RadiationId,\
            PanelTempId,\
            sensorWorkingStartTime,\
            sensorWorkingEndTime,\
            siteTypeId,\
            criticalIeee,\
            criticalThreshold,\
            WarningDetectMinute,\
            WarningAmount,\
            watt,\
            PRThresHold,\
            PRTimeDuration,\
            envTempId,\
            tempCompareRadThreshod,\
            tempCompareThreshold,\
            tempDataDifferenceAmount,\
            ptempCoefficient,\
            ptempTimeDuration,\
            ptempDataDifferenceThreshold\
        FROM\
            iotcomui.TDeviceWarning AS a,\
            iotmgmt.TDevice AS b,\
            iotcomui.TGatewayWarning AS c,\
            iotcomui.TWarningCategory AS d\
        WHERE\
            a.deviceId = b.id\
            AND b.gatewayId = c.gatewayId\
            AND a.warningCategoryId = d.warningCategoryId\
    "
    return pd.read_sql_query(sql.text(sql_query), con=engine)

def getLineToken():
    return "wVaWxokJaVs0wtMc8tqtIw5CbCArevjTUsnuVQGFYLv"

def getSlackURL():
    return "https://hooks.slack.com/services/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"

def getGoogleAuth():
    auth = "warning/toolkits/rosy-strata-293907-9858b3057bb5.json"

    return auth