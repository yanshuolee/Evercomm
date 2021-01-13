import sqlalchemy as sql
import time
import datetime

def update(engine, insert=False):
    conn = engine.connect()
    
    if insert:
        truncateList = [
            "TRUNCATE `processplatform`.`dailySolarPowerGeneration`",
            "TRUNCATE `processplatform`.`dailySolarRevenue`",
            "TRUNCATE `processplatform`.`solarGroupPowerGeneration`",
            "TRUNCATE `processplatform`.`solarInvPowerGeneration`",
            "TRUNCATE `processplatform`.`solarMpptPowerGeneration`",
            "TRUNCATE `processplatform`.`solarSitePowerGeneration`",
        ]
        for sql_q in truncateList:
            conn.execute(sql.text(sql_q))
            print(f"[{datetime.datetime.now()}] {sql_q}")
        

if __name__ == "__main__":
    #IP
    host = "localhost"
    user = "ecoetl"
    pwd = "ECO4etl"

    #DB name
    dbProcessPF = "processplatform"
    processplatform_engine = sql.create_engine(f'mysql+mysqldb://{user}:{pwd}@{host}/{dbProcessPF}', pool_recycle=3600*7)
    update(processplatform_engine, insert=True)