import sqlalchemy as sql
import pandas as pd
import numpy as np
import copy
import time
import datetime
import calendar

def update(currentTS, engine, insert=True):
    conn = engine.connect()
    year = currentTS.year
    month_start = f"{(currentTS.month):02}"
    lastDay = calendar.monthrange(currentTS.year, currentTS.month)[1]
    if insert:
        update_sql = f"\
            replace into reportplatform.monthlySolarRevenue (operationDate, siteId, realRevenue, budgetRevenue, referenceRevenue)\
            SELECT \
                DATE_FORMAT(operationDate, '%Y-%m-01') as operationDate,\
                siteId,\
                round(sum(realRevenue), 3) as realRevenue,\
                round(sum(budgetRevenue), 3) as budgetRevenue,\
                round(sum(referenceRevenue), 3) as referenceRevenue\
            FROM\
                (select * from (SELECT * FROM reportplatform.dailySolarRevenue where operationDate < CURRENT_DATE() union SELECT * FROM processplatform.dailySolarRevenue) as U) as a\
            WHERE\
                operationDate >= '{year}-{month_start}-01'\
                    AND operationDate <= '{year}-{month_start}-{lastDay}'\
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