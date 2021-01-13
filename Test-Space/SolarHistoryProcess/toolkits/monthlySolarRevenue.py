import sqlalchemy as sql
import pandas as pd
import numpy as np
import copy
import time
import datetime

def update(currentTS, engine, insert=True):
    conn = engine.connect()
    year = currentTS.year
    month_start = f"{(currentTS.month):02}"
    month_end = f"{(currentTS.month)+1:02}"
    if insert:
        
        # 算歷史的
        update_sql = f"\
            replace into reportplatform.monthlySolarRevenue (operationDate, siteId, realRevenue, budgetRevenue, referenceRevenue)\
            SELECT \
                operationDate,\
                siteId,\
                round(avg(realRevenue), 3) as realRevenue,\
                round(avg(budgetRevenue), 3) as budgetRevenue,\
                round(avg(referenceRevenue), 3) as referenceRevenue\
            FROM\
                reportplatform.dailySolarRevenue\
            WHERE\
                operationDate >= '{year}-{month_start}-01'\
                    AND operationDate < '{year}-{month_end}-01'\
            GROUP BY siteId\
        "
        conn.execute(sql.text(update_sql))

if __name__ == "__main__":
    currentTS = datetime.datetime(2020, 7, 1, 11, 3)
    #IP
    host = "localhost"
    user = "admin"
    pwd = "Admin99"

    #DB name
    dbRPF = "reportplatform"
    reportplatform_engine = sql.create_engine(f'mysql+mysqldb://{user}:{pwd}@{host}/{dbRPF}', pool_recycle=3600*7)
    update(currentTS, reportplatform_engine, insert=True)
    print("Insert successfully.")