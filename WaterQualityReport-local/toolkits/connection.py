import sqlalchemy as sql

class Engines():
    def __init__(self):
        #IP
        host = "localhost"
        user = "ecoetl"
        pwd = "ECO4etl"

        #DB name
        dbReportplatform = "reportplatform"
        dbIotmgmt = "iotmgmt"

        iotmgmt_engine = sql.create_engine(f"mysql+mysqldb://{'ecoetl'}:{'ECO4etl'}@{'192.168.1.34'}/{dbIotmgmt}?charset=utf8", pool_recycle=3600*7)
        reportplatform_engine = sql.create_engine(f"mysql+mysqldb://{user}:{pwd}@{host}/{dbReportplatform}?charset=utf8", pool_recycle=3600*7)

        self.engine_dict = {
            "iotmgmt": iotmgmt_engine,
            "reportplatform": reportplatform_engine
        }

    def close(self):
        for key in self.engine_dict.keys():
            print(f"Closing {key} engine.")
            self.engine_dict[key].dispose()
