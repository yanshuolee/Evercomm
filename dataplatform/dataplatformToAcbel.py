import argparse
import sqlalchemy as sql
import pandas as pd
import time
import datetime
import sys, traceback
import json

def toSolar(solar, engine):
    try:
        s = time.time()
        insertString = ""
        for _, row in solar.iterrows():
            tempString = ""
            tempString = f"\
                '{row['ts'].strftime('%Y-%m-%d %H:%M:00')}',\
                '{row['gatewayId']}',\
                '{row['ieee']}',\
                '{row['receivedSync'].strftime('%Y-%m-%d %H:%M:00')}',\
                '{row['energyProducedToday']}',\
                '{row['energyProducedLifeTime']}',\
                '{row['totalOperationHourLifeTime']}',\
                '{row['internalTemperature']}',\
                '{row['dcVoltage']}',\
                '{row['totalDCPower']}',\
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
                '{row['groundResistance']}'"
            
            insertString += f"({tempString}) , "
        
        insertString = insertString[:-2]

        sqlstr = f"REPLACE INTO dataplatform.solarInverter values {insertString}" # modified dataplatform

        sqlstr = sqlstr.replace("'None'", 'null')

        conn = engine.connect()
        conn.execute(sql.text(sqlstr))

        print(f"[{datetime.datetime.now()}] solar in {time.time()-s} sec")
    except:
        traceback.print_exc(file=sys.stdout)

def toEnv(ain, engine):
    try:
        s = time.time()
        insertString = ""
        for _, row in ain.iterrows():
            tempString = ""
            tempString = f"\
                '{row['ts'].strftime('%Y-%m-%d %H:%M:00')}',\
                '{row['gatewayId']}',\
                '{row['ieee']}',\
                '{row['receivedSync'].strftime('%Y-%m-%d %H:%M:00')}',\
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

        sqlstr = f"REPLACE INTO dataplatform.environment values {insertString}" # modified dataplatform

        sqlstr = sqlstr.replace("'None'", 'null')

        conn = engine.connect()
        conn.execute(sql.text(sqlstr))

        print(f"[{datetime.datetime.now()}] env in {time.time()-s} sec")
    except:
        traceback.print_exc(file=sys.stdout)

def main():
    try:
        #IP
        host = "localhost"
        user = "ecoetl"
        pwd = "ECO4etl"

        tgtHost = "61.31.171.230:52199"
        tgtUser = "ecoetl"
        tgtPwd = "ECO4etl"

        #DB name
        dbdataplatform = "dataplatform"

        # Engine
        engine = sql.create_engine(f"mysql+mysqldb://{user}:{pwd}@{host}/{dbdataplatform}?charset=utf8", pool_recycle=3600*7)
        tgtEngine = sql.create_engine(f"mysql+mysqldb://{tgtUser}:{tgtPwd}@{tgtHost}/{dbdataplatform}?charset=utf8", pool_recycle=3600*7)

        # Table
        solarSQL = "\
            SELECT \
                *\
            FROM\
                dataplatform.solarInverter\
            WHERE\
                gatewayId IN (40 , 42, 44, 45)\
                    AND receivedSync >= NOW() - INTERVAL 5 MINUTE\
            ORDER BY receivedSync DESC;\
        "

        ainSQL = "\
            SELECT \
                *\
            FROM\
                dataplatform.environment\
            WHERE\
                gatewayId IN (40 , 42, 44, 45)\
                    AND receivedSync >= NOW() - INTERVAL 5 MINUTE\
            ORDER BY receivedSync DESC;\
        "
        ain = pd.read_sql(ainSQL, con=engine)
        solar = pd.read_sql(solarSQL, con=engine)
        toSolar(solar, tgtEngine)
        toEnv(ain, tgtEngine)

    except:
        traceback.print_exc(file=sys.stdout)
    finally:
        engine.dispose()
        tgtEngine.dispose()

if __name__ == "__main__":
    main()