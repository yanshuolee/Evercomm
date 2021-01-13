import argparse
import sqlalchemy as sql
import pandas as pd
import time
import datetime
import sys, traceback
import json
from multiprocessing import Process

def envToDB(environment, engine):
    try:
        s = time.time()
        # ieees = pd.read_sql_query("SELECT ieee FROM iotmgmt.TDevice WHERE gatewayId IN (40 , 41, 42, 44, 45) AND deviceAttribId in (6,10)", con=engine)["ieee"].to_numpy()
        # tb1 = ain[ain["ieee"].isin(ieees)]
        # tb1 = tb1[tb1["receivedSync"].dt.minute % 2 == 0]
        # tb1 = tb1.drop_duplicates(subset=['receivedSync', 'ieee'])
        insertString = ""
        for _, row in environment.iterrows():
            tempString = ""
            tempString = f"\
                '{row['ts']}',\
                '{row['gatewayId']}',\
                '{row['ieee']}',\
                '{row['receivedSync'].strftime('%Y-%m-%d %H:%M:00')}', \
                '{row['ain1'] if not pd.isna(row['ain1']) else None}',\
                '{row['ain2'] if not pd.isna(row['ain2']) else None}',\
                '{row['ain3'] if not pd.isna(row['ain3']) else None}',\
                '{row['ain4'] if not pd.isna(row['ain4']) else None}',\
                '{row['ain5'] if not pd.isna(row['ain5']) else None}',\
                '{row['voltage1'] if not pd.isna(row['voltage1']) else None}',\
                '{row['voltage2'] if not pd.isna(row['voltage2']) else None}',\
                '{row['voltage3'] if not pd.isna(row['voltage3']) else None}',\
                '{row['voltage4'] if not pd.isna(row['voltage4']) else None}',\
                '{row['voltage5'] if not pd.isna(row['voltage5']) else None}',\
                '{row['value1'] if not pd.isna(row['value1']) else None}',\
                '{row['value2'] if not pd.isna(row['value2']) else None}',\
                '{row['value3'] if not pd.isna(row['value3']) else None}',\
                '{row['value4'] if not pd.isna(row['value4']) else None}',\
                '{row['value5'] if not pd.isna(row['value5']) else None}'"
            
            insertString += f"({tempString}) , "
        
        insertString = insertString[:-2]

        sqlstr = f"REPLACE INTO dataplatform.environment (`ts`,`gatewayId`, `ieee`, `receivedSync`, `ain1`, `ain2`, `ain3`, `ain4`, `ain5`,\
                   `voltage1`,`voltage2`,`voltage3`,`voltage4`,`voltage5`,`value1`, `value2`, `value3`, `value4`, `value5`) values {insertString}"

        sqlstr = sqlstr.replace("'None'", 'null')

        conn = engine.connect()
        conn.execute(sql.text(sqlstr))

        print(f"Env {time.time()-s} 秒")
    except:
        traceback.print_exc(file=sys.stdout)

def solarToDB(solarInverter, engine):
    try:
        # tb1 = solarInverter2[(solarInverter2["gatewayId"] == 44) | (solarInverter2["gatewayId"] == 45)]
        # tb1 = tb1[tb1["receivedSync"].dt.minute % 2 == 0]
        # tb1 = tb1.drop_duplicates(subset=['receivedSync', 'ieee'])
        s = time.time()
        conn = engine.connect()
        insertString = ""

        for ind, row in solarInverter.iterrows():
            tempString = ""
            err = row['faultAlarmCode'].replace('"','')
            tempString = f"'{row['ts'].strftime('%Y-%m-%d %H:%M:00')}',\
            '{row['gatewayId']}',\
            '{row['ieee']}',\
            '{row['receivedSync'].strftime('%Y-%m-%d %H:%M:00')}',\
            '{row['energyProducedToday'] }',\
            '{row['energyProducedLifeTime'] }',\
            '{row['totalOperationHourLifeTime']}',\
            '{row['internalTemperature']}',\
            '{row['dcVoltage']}',\
            '{row['totalDCPower'] }',\
            '{row['phaseAVoltage']}',\
            '{row['phaseBVoltage']}',\
            '{row['phaseCVoltage']}',\
            '{row['phaseACurrent']}',\
            '{row['phaseBCurrent']}',\
            '{row['phaseCCurrent']}',\
            '{row['totalApparentPower']}',\
            '{row['totalActivePower']}',\
            '{row['reactivePower']}',\
            '{row['powerFactor']}',\
            '{row['gridFrequency']}',\
            '{row['operationState']}',\
            '{row['faultAlarmCode']}',\
            '{row['dailyOperationTime']}',\
            '{row['monthlyEnergy']}',\
            '{row['dcCurrent']}',\
            '{row['groundResistance']} ' "

            insertString += f"({tempString}) , "
        
        if insertString != "":
            insertString = insertString[:-2]

            sqlstr = f"replace into dataplatform.solarInverter (`ts`,`gatewayId`,`ieee`,`receivedSync`,`energyProducedToday`,\
                `energyProducedLifeTime`,`totalOperationHourLifeTime`,`internalTemperature`,\
                `dcVoltage`,`totalDCPower`,`phaseAVoltage`,`phaseBVoltage`,`phaseCVoltage`,\
                `phaseACurrent`,`phaseBCurrent`,`phaseCCurrent`,`totalApparentPower`,\
                `totalActivePower`,`reactivePower`,`powerFactor`,`gridFrequency`,`operationState`,\
                `faultAlarmCode`,`dailyOperationTime`,`monthlyEnergy`,`dcCurrent`, `groundResistance`) values {insertString}"
            
            conn.execute(sql.text(sqlstr))

        print(f"solarInv in {time.time()-s} sec")
    except:
        traceback.print_exc(file=sys.stdout)

if __name__ == "__main__":
    try:
        #IP
        srcHost = "61.31.171.230:52199"
        srcUser = "ecoetl"
        srcPwd = "ECO4etl"

        dstHost = "localhost"
        dstUser = "ecoetl"
        dstPwd = "ECO4etl"

        #DB name
        dbInt = "integrationplatform"
        dbData = "dataplatform"

        # Engine
        engine = sql.create_engine(f"mysql+mysqldb://{srcUser}:{srcPwd}@{srcHost}/{dbInt}?charset=utf8", pool_recycle=3600*7)
        destEngine = sql.create_engine(f"mysql+mysqldb://{dstUser}:{dstPwd}@{dstHost}/{dbData}?charset=utf8", pool_recycle=3600*7)

        # Table
        envSQL = "SELECT * FROM integrationplatform.environment where receivedSync >= now() - interval 10 minute"
        solarSQL = "SELECT * FROM integrationplatform.solarInverter where receivedSync >= now() - interval 10 minute"

        environment = pd.read_sql(envSQL, con=engine)
        solarInverter = pd.read_sql(solarSQL, con=engine)

        solarToDB(solarInverter, destEngine)
        envToDB(environment, destEngine)

        engine.dispose()
        destEngine.dispose()
    except:
        traceback.print_exc(file=sys.stdout)