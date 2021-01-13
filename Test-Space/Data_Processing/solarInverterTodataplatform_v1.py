import sqlalchemy as sql
import pandas as pd
import copy
import time
import datetime
import sys, traceback
import json

def to_arr(_str):
    return _str.split('[')[1].split(']')[0].split(',')

def getIEEE(engine, invType):
    if invType == "allis":
        ids = "(1,2,3,4)"
    elif invType == "huawei":
        ids = "(5)"
    sites = "(5,6,7,15,16)"

    sql_query = f"\
        SELECT \
            a.siteId,\
            a.groupId,\
            a.inverterId,\
            b.mpptId,\
            b.mpptInstCapacity,\
            c.azimuth,\
            c.inclination,\
            d.efficiency,\
            f.ieee,\
            f.deviceDesc,\
            d.id AS invTypeId,\
            g.cityId\
        FROM\
            uiplatform.TSiteInverter AS a,\
            uiplatform.TInverterMppt AS b,\
            uiplatform.TSiteBuildList AS c,\
            uiplatform.TInverterModel AS d,\
            dataplatform.TLogicDevice AS e,\
            dataplatform.TDevice AS f,\
            uiplatform.TSite AS g\
        WHERE\
            a.inverterId = b.inverterId\
                AND b.mpptPanelAzimuthId = c.azimuthId\
                AND b.mpptPanelInclinationId = c.inclinationId\
                AND a.invModelId = d.id\
                AND a.logicDeviceId = e.id\
                AND e.deviceId = f.id\
                AND g.id = a.siteId\
                and d.id in {ids}\
                and a.siteId in {sites}\
        GROUP BY ieee\
        ORDER BY siteId\
    "
    IEEE = pd.read_sql(sql.text(sql_query), con=engine)["ieee"]
    return IEEE

def allis(solarInverter2, engine):
    try:
        ieeeList = getIEEE(engine, "allis")
        tb1 = solarInverter2[(solarInverter2["gatewayId"] == 40) | (solarInverter2["gatewayId"] == 42)]
        try:
            tb1 = tb1[tb1["receivedSync"].dt.minute % 2 == 0]
        except:
            pass
        tb1 = tb1.drop_duplicates(subset=['receivedSync', 'ieee'])
        insertString = ""

        # == #
        # 要改
        DTArr = pd.date_range(start='2020-08-26 05:02', end='2020-08-26 19', freq='2T')
        for ieee in ieeeList:
            tb2 = tb1[tb1["ieee"]==ieee]
            DT = pd.DataFrame({"receivedSync":DTArr})
            tb2 = DT.merge(tb2, on="receivedSync", how="left")

            for _, row in tb2.iterrows():
                tempString = ""
                err = row['errorCode'].replace('"','') if not pd.isna(row['errorCode']) else None

                tempString = f"'{row['ts'].strftime('%Y-%m-%d %H:%M:00') if not pd.isna(row['ts']) else None}',\
                '{row['gatewayId'] if not pd.isna(row['gatewayId']) else None}',\
                '{row['ieee'] if not pd.isna(row['ieee']) else ieee}',\
                '{row['receivedSync'].strftime('%Y-%m-%d %H:%M:00')}',\
                '{row['dailyKWh'] if not pd.isna(row['dailyKWh']) else None }',\
                '{row['lifeTimeKWh'] if not pd.isna(row['lifeTimeKWh']) else None }',\
                '{row['lifeTimeHour'] if not pd.isna(row['lifeTimeHour']) else None}',\
                '{json.dumps({'temp1': float(to_arr(row['Temperature'])[0])}) if not pd.isna(row['Temperature']) else None}',\
                '{json.dumps({'voltage1':float(to_arr(row['dcVoltage'])[0]), 'voltage2': float(to_arr(row['dcVoltage'])[1])}) if not pd.isna(row['dcVoltage']) else None}',\
                '{row['dcPower'] if not pd.isna(row['dcPower']) else None }',\
                '{row['acVoltageA'] if not pd.isna(row['acVoltageA']) else None}',\
                '{row['acVoltageB'] if not pd.isna(row['acVoltageB']) else None}',\
                '{row['acVoltageC'] if not pd.isna(row['acVoltageC']) else None}',\
                '{row['acCurrentA'] if not pd.isna(row['acCurrentA']) else None}',\
                '{row['acCurrentB'] if not pd.isna(row['acCurrentB']) else None}',\
                '{row['acCurrentC'] if not pd.isna(row['acCurrentC']) else None}',\
                '{row['apparentPower'] if not pd.isna(row['apparentPower']) else None}',\
                '{row['acPower'] if not pd.isna(row['acPower']) else None}',\
                '{row['reactivePower'] if not pd.isna(row['reactivePower']) else None}',\
                '{row['pf'] if not pd.isna(row['pf']) else None}',\
                '{row['gridFrequency'] if not pd.isna(row['gridFrequency']) else None}',\
                '{row['operationState'] if not pd.isna(row['operationState']) else None}',\
                '{json.dumps({'errorcode1': (to_arr(err)[0]), 'errorcode2': (to_arr(err)[1]), 'errorcode3': (to_arr(err)[2]), 'errorcode4': (to_arr(err)[3])}) if err is not None else None}',\
                '{row['dailyOperationMinute'] if not pd.isna(row['dailyOperationMinute']) else None}',\
                '{row['monthlyKWh'] if not pd.isna(row['monthlyKWh']) else None}',\
                '{json.dumps({'current1': float(to_arr(row['dcCurrent'])[0]), 'current2': float(to_arr(row['dcCurrent'])[1])}) if not pd.isna(row['dcCurrent']) else None}',\
                '{json.dumps({'data1': float(to_arr(row['groundResistance'])[0]), 'data2': float(to_arr(row['groundResistance'])[1])} )  if not pd.isna(row['groundResistance']) else None}' "

                insertString += f"({tempString}) , "
            
        
        insertString = insertString[:-2]
        insertString = insertString.replace("'None'", "null")

        sqlstr = f"replace into dataplatform.solarInverter (`ts`,`gatewayId`,`ieee`,`receivedSync`,`energyProducedToday`,\
            `energyProducedLifeTime`,`totalOperationHourLifeTime`,`internalTemperature`,\
            `dcVoltage`,`totalDCPower`,`phaseAVoltage`,`phaseBVoltage`,`phaseCVoltage`,\
            `phaseACurrent`,`phaseBCurrent`,`phaseCCurrent`,`totalApparentPower`,\
            `totalActivePower`,`reactivePower`,`powerFactor`,`gridFrequency`,`operationState`,\
            `faultAlarmCode`,`dailyOperationTime`,`monthlyEnergy`,`dcCurrent`, `groundResistance`) values {insertString}"

        conn = engine.connect()
        conn.execute(sql.text(sqlstr))

        print("亞力補資料成功!")
    except:
        traceback.print_exc(file=sys.stdout)

def huawei(solarInverter2, engine):
    try:
        ieeeList = getIEEE(engine, "huawei")
        tb1 = solarInverter2[(solarInverter2["gatewayId"] == 44) | (solarInverter2["gatewayId"] == 45)]
        try:
            tb1 = tb1[tb1["receivedSync"].dt.minute % 2 == 0]
        except:
            pass
        tb1 = tb1.drop_duplicates(subset=['receivedSync', 'ieee'])
        insertString = ""

        # 要改
        DTArr = pd.date_range(start='2020-08-26 05:02', end='2020-08-26 19', freq='2T')
        for ieee in ieeeList:
            tb2 = tb1[tb1["ieee"]==ieee]
            DT = pd.DataFrame({"receivedSync":DTArr})
            tb2 = DT.merge(tb2, on="receivedSync", how="left")

            for _, row in tb2.iterrows():
                tempString = ""
                err = row['errorCode'].replace('"','') if not pd.isna(row['errorCode']) else None

                tempString = f"'{row['ts'].strftime('%Y-%m-%d %H:%M:00') if not pd.isna(row['ts']) else None}',\
                '{row['gatewayId'] if not pd.isna(row['gatewayId']) else None}',\
                '{row['ieee'] if not pd.isna(row['ieee']) else ieee}',\
                '{row['receivedSync'].strftime('%Y-%m-%d %H:%M:00')}',\
                '{row['dailyKWh'] if not pd.isna(row['dailyKWh']) else None }',\
                '{row['lifeTimeKWh'] if not pd.isna(row['lifeTimeKWh']) else None }',\
                '{row['lifeTimeHour'] if not pd.isna(row['lifeTimeHour']) else None}',\
                '{json.dumps({'temp1': float(to_arr(row['Temperature'])[0])}) if not pd.isna(row['Temperature']) else None}',\
                '{json.dumps({'voltage1':float(to_arr(row['dcVoltage'])[0]), 'voltage2': float(to_arr(row['dcVoltage'])[1]), 'voltage3': float(to_arr(row['dcVoltage'])[2]), 'voltage4': float(to_arr(row['dcVoltage'])[3]), 'voltage5': float(to_arr(row['dcVoltage'])[4]), 'voltage6': float(to_arr(row['dcVoltage'])[5]), 'voltage7': float(to_arr(row['dcVoltage'])[6]), 'voltage8': float(to_arr(row['dcVoltage'])[7])}) if not pd.isna(row['dcVoltage']) else None}',\
                '{row['dcPower'] if not pd.isna(row['dcPower']) else None }',\
                '{row['acVoltageA'] if not pd.isna(row['acVoltageA']) else None}',\
                '{row['acVoltageB'] if not pd.isna(row['acVoltageB']) else None}',\
                '{row['acVoltageC'] if not pd.isna(row['acVoltageC']) else None}',\
                '{row['acCurrentA'] if not pd.isna(row['acCurrentA']) else None}',\
                '{row['acCurrentB'] if not pd.isna(row['acCurrentB']) else None}',\
                '{row['acCurrentC'] if not pd.isna(row['acCurrentC']) else None}',\
                '{row['apparentPower'] if not pd.isna(row['apparentPower']) else None}',\
                '{row['acPower'] if not pd.isna(row['acPower']) else None}',\
                '{row['reactivePower'] if not pd.isna(row['reactivePower']) else None}',\
                '{row['pf'] if not pd.isna(row['pf']) else None}',\
                '{row['gridFrequency'] if not pd.isna(row['gridFrequency']) else None}',\
                '{row['operationState'] if not pd.isna(row['operationState']) else None}',\
                '{json.dumps({'errorcode1': (to_arr(err)[0]), 'errorcode2': (to_arr(err)[1]), 'errorcode3': (to_arr(err)[2]), 'errorcode4': (to_arr(err)[3]), 'errorcode5': (to_arr(err)[4]), 'errorcode6': (to_arr(err)[5]), 'errorcode7': (to_arr(err)[6]), 'errorcode8': (to_arr(err)[7]), 'errorcode9': (to_arr(err)[8]), 'errorcode10': (to_arr(err)[9]), 'errorcode11': (to_arr(err)[10])}) if err is not None else None}',\
                '{row['dailyOperationMinute'] if not pd.isna(row['dailyOperationMinute']) else None}',\
                '{row['monthlyKWh'] if not pd.isna(row['monthlyKWh']) else None}',\
                '{json.dumps({'current1': float(to_arr(row['dcCurrent'])[0]), 'current2': float(to_arr(row['dcCurrent'])[1]), 'current3': float(to_arr(row['dcCurrent'])[2]), 'current4': float(to_arr(row['dcCurrent'])[3]), 'current5': float(to_arr(row['dcCurrent'])[4]), 'current6': float(to_arr(row['dcCurrent'])[5]), 'current7': float(to_arr(row['dcCurrent'])[6]), 'current8': float(to_arr(row['dcCurrent'])[7])}) if not pd.isna(row['dcCurrent']) else None}',\
                '{json.dumps({'data1': float(to_arr(row['groundResistance'])[0])}) if not pd.isna(row['groundResistance']) else None}' "

                insertString += f"({tempString}) , "
            

        insertString = insertString[:-2]
        insertString = insertString.replace("'None'", "null")

        sqlstr = f"replace into dataplatform.solarInverter (`ts`,`gatewayId`,`ieee`,`receivedSync`,`energyProducedToday`,\
            `energyProducedLifeTime`,`totalOperationHourLifeTime`,`internalTemperature`,\
            `dcVoltage`,`totalDCPower`,`phaseAVoltage`,`phaseBVoltage`,`phaseCVoltage`,\
            `phaseACurrent`,`phaseBCurrent`,`phaseCCurrent`,`totalApparentPower`,\
            `totalActivePower`,`reactivePower`,`powerFactor`,`gridFrequency`,`operationState`,\
            `faultAlarmCode`,`dailyOperationTime`,`monthlyEnergy`,`dcCurrent`, `groundResistance`) values {insertString}"

        conn = engine.connect()
        conn.execute(sql.text(sqlstr))

        print("華為補資料成功!")
    except:
        traceback.print_exc(file=sys.stdout)

def runDaily():
    pass

if __name__ == "__main__":
    try:
        #IP
        host = "localhost"
        user = "ecoetl"
        pwd = "ECO4etl"

        #DB name
        dbiotmgmt = "iotmgmt"

        # Engine
        engine = sql.create_engine(f"mysql+mysqldb://{user}:{pwd}@{host}/{dbiotmgmt}?charset=utf8", pool_recycle=3600*7)

        # Table
        # startTime = datetime.datetime.now().replace(hour=5, minute=0, second=0)
        # endTime = datetime.datetime.now().replace(hour=19, minute=0, second=0)
        startTime = datetime.datetime.now().replace(month=8, day=26, hour=5, minute=0, second=0)
        endTime = datetime.datetime.now().replace(month=8, day=26, hour=19, minute=0, second=0)
        solarInverter2_sql = f"SELECT * FROM iotmgmt.solarInverter2 where receivedSync >= '{str(startTime)[:16]}' and receivedSync <= '{str(endTime)[:16]}' order by receivedSync asc"
        solarInverter2 = pd.read_sql(solarInverter2_sql, con=engine)

        s = time.time()
        allis(solarInverter2, engine)
        print(time.time()-s)

        s = time.time()
        huawei(solarInverter2, engine)
        print(time.time()-s)
    except:
        traceback.print_exc(file=sys.stdout)