import argparse
import sqlalchemy as sql
import pandas as pd
import time
import datetime
import sys, traceback
import json
from multiprocessing import Process

def irradiator(ain, engine):
    try:
        s = time.time()
        ieees = pd.read_sql_query("SELECT ieee FROM iotmgmt.TDevice WHERE gatewayId IN (40 , 41, 42, 44, 45) AND deviceAttribId = 9", con=engine)["ieee"].to_numpy()
        tb1 = ain[ain["ieee"].isin(ieees)]
        tb1 = tb1[tb1["receivedSync"].dt.minute % 2 == 0]
        tb1 = tb1.drop_duplicates(subset=['receivedSync', 'ieee'])
        insertString = ""

        for _, row in tb1.iterrows():
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
                '{row['ain1'] if not pd.isna(row['ain1']) else None}',\
                '{row['value1'] if not pd.isna(row['value1']) else None}',\
                '{row['value2'] if not pd.isna(row['value2']) else None}',\
                '{row['value3'] if not pd.isna(row['value3']) else None}',\
                '{row['value4'] if not pd.isna(row['value4']) else None}',\
                '{row['value1'] if not pd.isna(row['value1']) else None}'"

            # tempString = f"\
            #     '{row['ts']}',\
            #     '{row['gatewayId']}',\
            #     '{row['ieee']}',\
            #     '{row['receivedSync'].strftime('%Y-%m-%d %H:%M:00')}', \
            #     '{row['ain1'] if not pd.isna(row['ain1']) else 'null'}',\
            #     '{row['ain2'] if not pd.isna(row['ain2']) else 'null'}',\
            #     '{row['ain3'] if not pd.isna(row['ain3']) else 'null'}',\
            #     '{row['ain4'] if not pd.isna(row['ain4']) else 'null'}',\
            #     '{row['ain1'] if not pd.isna(row['ain1']) else 'null'}',\
            #     '{row['value1'] if not pd.isna(row['value1']) else 'null'}',\
            #     '{row['value2'] if not pd.isna(row['value2']) else 'null'}',\
            #     '{row['value3'] if not pd.isna(row['value3']) else 'null'}',\
            #     '{row['value4'] if not pd.isna(row['value4']) else 'null'}',\
            #     '{row['value1'] if not pd.isna(row['value1']) else 'null'}'"
            
            insertString += f"({tempString}) , "
        
        insertString = insertString[:-2]

        sqlstr = f"REPLACE INTO dataplatform.environment (`ts`,`gatewayId`, `ieee`, `receivedSync`, `ain1`, `ain2`, `ain3`, `ain4`, `ain5`,\
                    `value1`, `value2`, `value3`, `value4`, `value5`) values {insertString}"

        sqlstr = sqlstr.replace("'None'", 'null')

        conn = engine.connect()
        conn.execute(sql.text(sqlstr))

        print(f"日照計補資料成功! {time.time()-s} 秒")
    except:
        traceback.print_exc(file=sys.stdout)

def temperature(ain, engine):
    try:
        s = time.time()
        ieees = pd.read_sql_query("SELECT ieee FROM iotmgmt.TDevice WHERE gatewayId IN (40 , 41, 42, 44, 45) AND deviceAttribId in (6,10)", con=engine)["ieee"].to_numpy()
        tb1 = ain[ain["ieee"].isin(ieees)]
        tb1 = tb1[tb1["receivedSync"].dt.minute % 2 == 0]
        tb1 = tb1.drop_duplicates(subset=['receivedSync', 'ieee'])
        insertString = ""

        for _, row in tb1.iterrows():
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
                '{row['value1'] if not pd.isna(row['value1']) else None}',\
                '{row['value2'] if not pd.isna(row['value2']) else None}',\
                '{row['value3'] if not pd.isna(row['value3']) else None}',\
                '{row['value4'] if not pd.isna(row['value4']) else None}',\
                '{row['value5'] if not pd.isna(row['value5']) else None}'"

            # tempString = f"\
            #     '{row['ts']}',\
            #     '{row['gatewayId']}',\
            #     '{row['ieee']}',\
            #     '{row['receivedSync'].strftime('%Y-%m-%d %H:%M:00')}', \
            #     '{row['ain1'] if not pd.isna(row['ain1']) else 'null'}',\
            #     '{row['ain2'] if not pd.isna(row['ain2']) else 'null'}',\
            #     '{row['ain3'] if not pd.isna(row['ain3']) else 'null'}',\
            #     '{row['ain4'] if not pd.isna(row['ain4']) else 'null'}',\
            #     '{row['ain5'] if not pd.isna(row['ain5']) else 'null'}',\
            #     '{row['value1'] if not pd.isna(row['value1']) else 'null'}',\
            #     '{row['value2'] if not pd.isna(row['value2']) else 'null'}',\
            #     '{row['value3'] if not pd.isna(row['value3']) else 'null'}',\
            #     '{row['value4'] if not pd.isna(row['value4']) else 'null'}',\
            #     '{row['value5'] if not pd.isna(row['value5']) else 'null'}'"
            
            insertString += f"({tempString}) , "
        
        insertString = insertString[:-2]

        sqlstr = f"REPLACE INTO dataplatform.environment (`ts`,`gatewayId`, `ieee`, `receivedSync`, `ain1`, `ain2`, `ain3`, `ain4`, `ain5`,\
                    `value1`, `value2`, `value3`, `value4`, `value5`) values {insertString}"

        sqlstr = sqlstr.replace("'None'", 'null')

        conn = engine.connect()
        conn.execute(sql.text(sqlstr))

        print(f"溫度計補資料成功! {time.time()-s} 秒")
    except:
        traceback.print_exc(file=sys.stdout)

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
        # startTime = datetime.datetime.now().replace(day=13, hour=5, minute=0, second=0)
        # endTime = datetime.datetime.now().replace(day=13, hour=19, minute=0, second=0)
        receivedSyncRange = f"receivedSync >= '{str(startTime)[:16]}' and receivedSync <= '{str(endTime)[:16]}'"

        ain_sql = f"select * from iotmgmt.ain where {receivedSyncRange} order by receivedSync asc"
        print(ain_sql)
        ain = pd.read_sql(ain_sql, con=engine)
        irradiator(ain, engine)
        temperature(ain, engine)
        engine.dispose()
    except:
        traceback.print_exc(file=sys.stdout)