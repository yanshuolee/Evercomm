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

        DTArr = pd.date_range(start=f'{date} 05:02', end=f'{date} 19', freq='2T')
        for ieee in ieees:
            tb2 = tb1[tb1["ieee"]==ieee]
            DT = pd.DataFrame({"receivedSync":DTArr})
            tb2 = DT.merge(tb2, on="receivedSync", how="left")

            for _, row in tb2.iterrows():
                tempString = ""

                tempString = f"\
                    '{row['ts'] if not pd.isna(row['ts']) else None}',\
                    '{row['gatewayId'] if not pd.isna(row['gatewayId']) else None}',\
                    '{row['ieee'] if not pd.isna(row['ieee']) else ieee}',\
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
            
            insertString += f"({tempString}) , "
        
        insertString = insertString[:-2]

        if datetime.datetime.strftime(datetime.datetime.today(), '%Y-%m-%d') == str(tb1['ts'].to_numpy()[0])[:10]:
            sqlstr = f"REPLACE INTO dataplatform.environment (`ts`,`gatewayId`, `ieee`, `receivedSync`, `ain1`, `ain2`, `ain3`, `ain4`, `ain5`,\
                        `value1`, `value2`, `value3`, `value4`, `value5`) values {insertString}"
        elif datetime.datetime.strftime(datetime.datetime.today(), '%Y-%m') == str(tb1['ts'].to_numpy()[0])[:7]:
            sqlstr = f"REPLACE INTO archiveplatform.environment (`ts`,`gatewayId`, `ieee`, `receivedSync`, `ain1`, `ain2`, `ain3`, `ain4`, `ain5`,\
                        `value1`, `value2`, `value3`, `value4`, `value5`) values {insertString}"
        else:
            YM = (str(tb1['ts'].to_numpy()[0])[:7]).replace('-','')
            sqlstr = f"REPLACE INTO archiveplatform.environment_{YM} (`ts`,`gatewayId`, `ieee`, `receivedSync`, `ain1`, `ain2`, `ain3`, `ain4`, `ain5`,\
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

        DTArr = pd.date_range(start=f'{date} 05:02', end=f'{date} 19', freq='2T')
        for ieee in ieees:
            tb2 = tb1[tb1["ieee"]==ieee]
            DT = pd.DataFrame({"receivedSync":DTArr})
            tb2 = DT.merge(tb2, on="receivedSync", how="left")

            for _, row in tb2.iterrows():
                tempString = ""

                tempString = f"\
                    '{row['ts'] if not pd.isna(row['ts']) else None}',\
                    '{row['gatewayId'] if not pd.isna(row['gatewayId']) else None}',\
                    '{row['ieee'] if not pd.isna(row['ieee']) else ieee}',\
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
            
            insertString += f"({tempString}) , "
        
        insertString = insertString[:-2]

        if datetime.datetime.strftime(datetime.datetime.today(), '%Y-%m-%d') == str(tb1['ts'].to_numpy()[0])[:10]:
            sqlstr = f"REPLACE INTO dataplatform.environment (`ts`,`gatewayId`, `ieee`, `receivedSync`, `ain1`, `ain2`, `ain3`, `ain4`, `ain5`,\
                        `value1`, `value2`, `value3`, `value4`, `value5`) values {insertString}"
        elif datetime.datetime.strftime(datetime.datetime.today(), '%Y-%m') == str(tb1['ts'].to_numpy()[0])[:7]:
            sqlstr = f"REPLACE INTO archiveplatform.environment (`ts`,`gatewayId`, `ieee`, `receivedSync`, `ain1`, `ain2`, `ain3`, `ain4`, `ain5`,\
                        `value1`, `value2`, `value3`, `value4`, `value5`) values {insertString}"
        else:
            YM = (str(tb1['ts'].to_numpy()[0])[:7]).replace('-','')
            sqlstr = f"REPLACE INTO archiveplatform.environment_{YM} (`ts`,`gatewayId`, `ieee`, `receivedSync`, `ain1`, `ain2`, `ain3`, `ain4`, `ain5`,\
                    `value1`, `value2`, `value3`, `value4`, `value5`) values {insertString}"

        sqlstr = sqlstr.replace("'None'", 'null')

        conn = engine.connect()
        conn.execute(sql.text(sqlstr))

        print(f"溫度計補資料成功! {time.time()-s} 秒")
    except:
        traceback.print_exc(file=sys.stdout)

def generateDates(start, end):
    temp = start
    rangeString = ""
    rangeList = []
    _month = start.month
    while temp <= end:
        if temp.month != _month:
            rangeString = rangeString[:-3]
            rangeList.append(rangeString)
            rangeString = ""
            _month = temp.month
        S = temp.replace(hour=5, minute=0, second=0)
        E = temp.replace(hour=19, minute=0, second=0)
        rangeString += f"(receivedSync >= '{str(S)[:16]}' and receivedSync <= '{str(E)[:16]}') or "
        temp = temp + datetime.timedelta(days=1)
        
    rangeString = rangeString[:-3]
    rangeList.append(rangeString)

    return rangeList

if __name__ == "__main__":
    try:
        #IP
        host = "localhost"
        user = "admin"
        pwd = "Admin99"

        #DB name
        dbiotmgmt = "iotmgmt"

        # Engine
        engine = sql.create_engine(f"mysql+mysqldb://{user}:{pwd}@{host}/{dbiotmgmt}?charset=utf8", pool_recycle=3600*7)
        engine_p1 = sql.create_engine(f"mysql+mysqldb://{user}:{pwd}@{host}/{dbiotmgmt}?charset=utf8", pool_recycle=3600*7)
        engine_p2 = sql.create_engine(f"mysql+mysqldb://{user}:{pwd}@{host}/{dbiotmgmt}?charset=utf8", pool_recycle=3600*7)

        # Table
        parser = argparse.ArgumentParser()
        parser.add_argument('--startDate', required=False)
        parser.add_argument('--endDate', required=False)

        args_ = parser.parse_args()
        startTimeStr = args_.startDate
        endTimeStr = args_.endDate
        if startTimeStr is not None:
            if endTimeStr is not None:
                # multiple days
                startTime = datetime.datetime.strptime(startTimeStr, '%Y-%m-%d')
                endTime = datetime.datetime.strptime(endTimeStr, '%Y-%m-%d')
                receivedSyncRange = generateDates(startTime, endTime)
            else:
                # only one day
                startTime = datetime.datetime.strptime(startTimeStr, '%Y-%m-%d').replace(hour=5, minute=0, second=0)
                endTime = datetime.datetime.strptime(startTimeStr, '%Y-%m-%d').replace(hour=19, minute=0, second=0)
                receivedSyncRange = f"receivedSync >= '{str(startTime)[:16]}' and receivedSync <= '{str(endTime)[:16]}'"
        else:
            startTime = datetime.datetime.now().replace(hour=5, minute=0, second=0)
            endTime = datetime.datetime.now().replace(hour=19, minute=0, second=0)
            receivedSyncRange = f"receivedSync >= '{str(startTime)[:16]}' and receivedSync <= '{str(endTime)[:16]}'"

        if isinstance(receivedSyncRange, list):
            for idx, re in enumerate(receivedSyncRange):
                ain_sql = f"select * from iotmgmt.ain where {re} order by receivedSync asc"
                print("[SQL]", ain_sql)
                ain = pd.read_sql(ain_sql, con=engine)
                a = Process(target=irradiator, args=(ain, engine_p1))
                b = Process(target=temperature, args=(ain, engine_p2))

                a.start()
                print("Multiprocessing irradiator")
                b.start()
                print("Multiprocessing temperature")

                print(f"[{idx+1}/{len(receivedSyncRange)}] Waiting for process join")
                a.join()
                b.join() 
        else:
            ain_sql = f"select * from iotmgmt.ain where {receivedSyncRange} order by receivedSync asc"
            print("[SQL]", ain_sql)
            ain = pd.read_sql(ain_sql, con=engine)
            a = Process(target=irradiator, args=(ain, engine_p1))
            b = Process(target=temperature, args=(ain, engine_p2))

            a.start()
            print("Multiprocessing irradiator")
            b.start()
            print("Multiprocessing temperature")

            print("Waiting for process join")
            b.join()
            a.join()
    except:
        traceback.print_exc(file=sys.stdout)
    finally:
        print("Closing connections")
        engine.dispose()
        engine_p1.dispose()
        engine_p2.dispose()