import sqlalchemy as sql

#IP
host = "localhost"
user = "ecoetl"
pwd = "ECO4etl"

#DB name
dbData = "dataplatform"
dbUi = "uiplatform"
dbRPF = "reportplatform"
dbARC = "archiveplatform"
dbProcessPF = "processplatform"
dbCWB = "historyDataCWB"

def getEngine():
    uiplatform_engine = sql.create_engine(f"mysql+mysqldb://{user}:{pwd}@{host}/{dbUi}?charset=utf8", pool_recycle=3600*7)
    historyDataCWB_engine = sql.create_engine(f"mysql+mysqldb://{user}:{pwd}@{host}/{dbCWB}?charset=utf8", pool_recycle=3600*7)
    archiveplatform_engine = sql.create_engine(f"mysql+mysqldb://{user}:{pwd}@{host}/{dbARC}?charset=utf8", pool_recycle=3600*7)
    dataplatform_engine = sql.create_engine(f"mysql+mysqldb://{user}:{pwd}@{host}/{dbData}?charset=utf8", pool_recycle=3600*7)
    processplatform_engine = sql.create_engine(f"mysql+mysqldb://{user}:{pwd}@{host}/{dbProcessPF}?charset=utf8", pool_recycle=3600*7)
    reportplatform_engine = sql.create_engine(f"mysql+mysqldb://{user}:{pwd}@{host}/{dbRPF}", pool_recycle=3600*7)

    engine_dict = {
        "uiplatform": uiplatform_engine,
        "historyDataCWB": historyDataCWB_engine,
        "archiveplatform": archiveplatform_engine,
        "dataplatform": dataplatform_engine,
        "processplatform": processplatform_engine,
        "reportplatform": reportplatform_engine
    }

    return engine_dict