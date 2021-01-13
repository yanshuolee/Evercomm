import sqlalchemy as sql

class Engines():
    def __init__(self):
        #IP
        host = "localhost"
        host_34 = "192.168.1.34"
        user = "ecoetl"
        pwd = "ECO4etl"

        #DB name
        dbIotmgmt = "iotmgmt"
        dbIotcomui = "iotcomui"

        iotmgmt_engine = sql.create_engine(f"mysql+mysqldb://{user}:{pwd}@{host_34}/{dbIotmgmt}?charset=utf8", pool_recycle=3600*7)
        iotcomui_engine = sql.create_engine(f"mysql+mysqldb://{user}:{pwd}@{host}/{dbIotcomui}?charset=utf8", pool_recycle=3600*7)

        self.engine_dict = {
            "iotmgmt": iotmgmt_engine,
            "iotcomui": iotcomui_engine
        }

    def close(self):
        for key in self.engine_dict.keys():
            print(f"Closing {key} engine.")
            self.engine_dict[key].dispose()
