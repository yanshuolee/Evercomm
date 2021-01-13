import sqlalchemy as sql
import pandas as pd
import copy
import time
import datetime
import sys, traceback
import json

def to_arr(_str):
    return _str.split('[')[1].split(']')[0].split(',')

def allis(solarInverter2, engine):
    try:
        tb1 = solarInverter2[(solarInverter2["gatewayId"] == 40) | (solarInverter2["gatewayId"] == 42)]
        tb1 = tb1[tb1["receivedSync"].dt.minute % 2 == 0]
        tb1 = tb1.drop_duplicates(subset=['receivedSync', 'ieee'])
        insertString = ""
        for _, row in tb1.iterrows():
            tempString = ""
            err = row['errorCode'].replace('"','')
            tempString = f"'{row['ts'].strftime('%Y-%m-%d %H:%M:00')}',\
            '{row['gatewayId']}',\
            '{row['ieee']}',\
            '{row['receivedSync'].strftime('%Y-%m-%d %H:%M:00')}',\
            '{row['dailyKWh'] }',\
            '{row['lifeTimeKWh'] }',\
            '{row['lifeTimeHour']}',\
            '{json.dumps({'temp1': float(to_arr(row['Temperature'])[0])})}',\
            '{json.dumps({'voltage1':float(to_arr(row['dcVoltage'])[0]), 'voltage2': float(to_arr(row['dcVoltage'])[1])})}',\
            '{row['dcPower'] }',\
            '{row['acVoltageA']}',\
            '{row['acVoltageB']}',\
            '{row['acVoltageC']}',\
            '{row['acCurrentA']}',\
            '{row['acCurrentB']}',\
            '{row['acCurrentC']}',\
            '{row['apparentPower']}',\
            '{row['acPower']}',\
            '{row['reactivePower']}',\
            '{row['pf']}',\
            '{row['gridFrequency']}',\
            '{row['operationState']}',\
            '{json.dumps({'errorcode1': (to_arr(err)[0]), 'errorcode2': (to_arr(err)[1]), 'errorcode3': (to_arr(err)[2]), 'errorcode4': (to_arr(err)[3])})}',\
            '{row['dailyOperationMinute']}',\
            '{row['monthlyKWh']}',\
            '{json.dumps({'current1': float(to_arr(row['dcCurrent'])[0]), 'current2': float(to_arr(row['dcCurrent'])[1])})}',\
            '{json.dumps({'data1': float(to_arr(row['groundResistance'])[0]), 'data2': float(to_arr(row['groundResistance'])[1])} ) } ' "

            insertString += f"({tempString}) , "
        insertString = insertString[:-2]

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
        tb1 = solarInverter2[(solarInverter2["gatewayId"] == 44) | (solarInverter2["gatewayId"] == 45)]
        tb1 = tb1[tb1["receivedSync"].dt.minute % 2 == 0]
        tb1 = tb1.drop_duplicates(subset=['receivedSync', 'ieee'])
        conn = engine.connect()
        insertString = ""

        for ind, row in tb1.iterrows():
            tempString = ""
            err = row['errorCode'].replace('"','')
            tempString = f"'{row['ts'].strftime('%Y-%m-%d %H:%M:00')}',\
            '{row['gatewayId']}',\
            '{row['ieee']}',\
            '{row['receivedSync'].strftime('%Y-%m-%d %H:%M:00')}',\
            '{row['dailyKWh'] }',\
            '{row['lifeTimeKWh'] }',\
            '{row['lifeTimeHour']}',\
            '{json.dumps({'temp1': float(to_arr(row['Temperature'])[0])})}',\
            '{json.dumps({'voltage1':float(to_arr(row['dcVoltage'])[0]), 'voltage2': float(to_arr(row['dcVoltage'])[1]), 'voltage3': float(to_arr(row['dcVoltage'])[2]), 'voltage4': float(to_arr(row['dcVoltage'])[3]), 'voltage5': float(to_arr(row['dcVoltage'])[4]), 'voltage6': float(to_arr(row['dcVoltage'])[5]), 'voltage7': float(to_arr(row['dcVoltage'])[6]), 'voltage8': float(to_arr(row['dcVoltage'])[7])})}',\
            '{row['dcPower'] }',\
            '{row['acVoltageA']}',\
            '{row['acVoltageB']}',\
            '{row['acVoltageC']}',\
            '{row['acCurrentA']}',\
            '{row['acCurrentB']}',\
            '{row['acCurrentC']}',\
            '{row['apparentPower']}',\
            '{row['acPower']}',\
            '{row['reactivePower']}',\
            '{row['pf']}',\
            '{row['gridFrequency']}',\
            '{row['operationState']}',\
            '{json.dumps({'errorcode1': (to_arr(err)[0]), 'errorcode2': (to_arr(err)[1]), 'errorcode3': (to_arr(err)[2]), 'errorcode4': (to_arr(err)[3]), 'errorcode5': (to_arr(err)[4]), 'errorcode6': (to_arr(err)[5]), 'errorcode7': (to_arr(err)[6]), 'errorcode8': (to_arr(err)[7]), 'errorcode9': (to_arr(err)[8]), 'errorcode10': (to_arr(err)[9]), 'errorcode11': (to_arr(err)[10])})}',\
            '{row['dailyOperationMinute']}',\
            '{row['monthlyKWh']}',\
            '{json.dumps({'current1': float(to_arr(row['dcCurrent'])[0]), 'current2': float(to_arr(row['dcCurrent'])[1]), 'current3': float(to_arr(row['dcCurrent'])[2]), 'current4': float(to_arr(row['dcCurrent'])[3]), 'current5': float(to_arr(row['dcCurrent'])[4]), 'current6': float(to_arr(row['dcCurrent'])[5]), 'current7': float(to_arr(row['dcCurrent'])[6]), 'current8': float(to_arr(row['dcCurrent'])[7])})}',\
            '{json.dumps({'data1': float(to_arr(row['groundResistance'])[0])} ) } ' "

            insertString += f"({tempString}) , "
            
            if ind % 100 == 0:
                insertString = insertString[:-2]

                sqlstr = f"replace into dataplatform.solarInverter (`ts`,`gatewayId`,`ieee`,`receivedSync`,`energyProducedToday`,\
                    `energyProducedLifeTime`,`totalOperationHourLifeTime`,`internalTemperature`,\
                    `dcVoltage`,`totalDCPower`,`phaseAVoltage`,`phaseBVoltage`,`phaseCVoltage`,\
                    `phaseACurrent`,`phaseBCurrent`,`phaseCCurrent`,`totalApparentPower`,\
                    `totalActivePower`,`reactivePower`,`powerFactor`,`gridFrequency`,`operationState`,\
                    `faultAlarmCode`,`dailyOperationTime`,`monthlyEnergy`,`dcCurrent`, `groundResistance`) values {insertString}"
                
                conn.execute(sql.text(sqlstr))
                insertString = ""
        
        if insertString != "":
            insertString = insertString[:-2]

            sqlstr = f"replace into dataplatform.solarInverter (`ts`,`gatewayId`,`ieee`,`receivedSync`,`energyProducedToday`,\
                `energyProducedLifeTime`,`totalOperationHourLifeTime`,`internalTemperature`,\
                `dcVoltage`,`totalDCPower`,`phaseAVoltage`,`phaseBVoltage`,`phaseCVoltage`,\
                `phaseACurrent`,`phaseBCurrent`,`phaseCCurrent`,`totalApparentPower`,\
                `totalActivePower`,`reactivePower`,`powerFactor`,`gridFrequency`,`operationState`,\
                `faultAlarmCode`,`dailyOperationTime`,`monthlyEnergy`,`dcCurrent`, `groundResistance`) values {insertString}"
            
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
        startTime = datetime.datetime.now().replace(hour=5, minute=0, second=0)
        endTime = datetime.datetime.now().replace(hour=19, minute=0, second=0)
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

    finally:
        engine.dispose()