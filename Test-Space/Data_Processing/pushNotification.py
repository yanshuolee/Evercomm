# -*- coding: utf-8 -*-
import sqlalchemy as sql
import pandas as pd
import numpy as np
import copy
import time
import smtplib
from datetime import datetime
import sys, traceback

logicDevice_group_sql = "\
    SELECT \
        a.id,\
        /*a.deviceId,\
        b.id,\
        b.ieee,\
        b.deviceTypeId,\
        c.id,\
        c.deviceCategoryId,\
        d.id,\
        d.categoryName,*/\
        d.`group`\
    FROM\
        dataplatform.TLogicDevice AS a\
            LEFT JOIN\
        (SELECT \
            id, ieee, deviceTypeId\
        FROM\
            dataplatform.TDevice) AS b ON a.deviceId = b.id\
            LEFT JOIN\
        (SELECT \
            id, deviceCategoryId\
        FROM\
            dataplatform.TDeviceType) AS c ON b.deviceTypeId = c.id\
            LEFT JOIN\
        (SELECT \
            id,\
                categoryName,\
                IF(id = 1, 'inverters', 'others') AS `group`\
        FROM\
            dataplatform.TDeviceCategory) AS d ON c.deviceCategoryId = d.id \
"

def getContent(table, uiplatform_engine):
    logicDevice_group = pd.read_sql_query(sql=logicDevice_group_sql, con=uiplatform_engine)
    TNotification = pd.read_sql_query(sql="SELECT * FROM uiplatform.TNotification", con=uiplatform_engine)
    TSite = pd.read_sql_query(sql="SELECT id, siteName FROM uiplatform.TSite", con=uiplatform_engine)

    threshold = 2

    # non deviceAbnormal
    deviceWarning_1 = table[table["warningSignal"] != "5"]
    deviceWarning_1 = deviceWarning_1.merge(TNotification, how="left", left_on="warningSignal", right_on="categoryId").set_index(deviceWarning_1.index)
    timeDiff = (pd.Timestamp.now() - deviceWarning_1["startTime"])/np.timedelta64(1,'m')
    remainder = timeDiff % deviceWarning_1["pushCondition"]
    print(remainder[remainder <= 10])
    remainderInd = remainder[remainder < threshold].index
    content_1 = deviceWarning_1.loc[remainderInd]

    # deviceAbnormal
    deviceWarning_2 = table[table["warningSignal"] == "5"]
    deviceWarning_2 = deviceWarning_2.merge(logicDevice_group, how="left", left_on="logicDeviceId", right_on="id").set_index(deviceWarning_2.index)
    deviceWarning_2 = deviceWarning_2.merge(TNotification[TNotification["categoryId"] == "5"], how="left", on="group").set_index(deviceWarning_2.index)
    timeDiff = (pd.Timestamp.now() - deviceWarning_2["startTime"])/np.timedelta64(1,'m')
    remainder = timeDiff % deviceWarning_2["pushCondition"]
    print(remainder[remainder <= 10])
    remainderInd = remainder[remainder < threshold].index
    content_2 = deviceWarning_2.loc[remainderInd]

    # combine
    content_str = None
    content = pd.concat([content_1, content_2], ignore_index=True)
    if content.size != 0:
        content = content.merge(TSite, how="left", left_on="siteId", right_on="id")
        content_str = content[["siteName", "deviceDesc", "warningDesc", "startTime"]].to_string(index = False)
    
    return content_str

def send(table, uiplatform_engine, debug=False):
    content = getContent(table, uiplatform_engine)
    if not content:
        if debug:
            content = "Debug mode."
    if content:
        TPushUser = pd.read_sql_query(sql="SELECT * FROM uiplatform.TPushUser", con=uiplatform_engine)
        gmail_user = 'plee@evercomm.com'
        gmail_password = 'plee99'
        from_add = gmail_user
        to = TPushUser[TPushUser["recipient"] > 0]["Email"].to_list()
        cc = TPushUser[TPushUser["cc"] > 0]["Email"].to_list()
        subject = '推播測試'
        body = content

        email_text = f"""\
From: {from_add}
To: {", ".join(to)}
Subject: {subject}

Current DT: {str(datetime.now())}

{body}
        """

        smtpObj = smtplib.SMTP('evercomm.com', 587)
        smtpObj.ehlo()
        smtpObj.starttls()
        smtpObj.login(gmail_user, gmail_password)
        smtpObj.sendmail(from_add, to, bytes(email_text, "utf8"))
        smtpObj.close()
        print ('Email sent')
    else:
        print ('No content')

if __name__ == "__main__":
    uiplatform_engine = sql.create_engine('mysql+mysqldb://admin:Admin99@localhost/uiplatform?charset=utf8', pool_recycle=3600*7)
    conn = uiplatform_engine.connect()
    DeviceWarning = pd.read_sql_query(sql="SELECT * FROM uiplatform.DeviceWarningLog2 group by warningSignal", con=uiplatform_engine) # 測試用

    send(DeviceWarning, uiplatform_engine, debug=True)