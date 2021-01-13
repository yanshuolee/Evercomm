import time
import datetime
import argparse
import sys, traceback
import os
from toolkits import connection, information, calculation

def main():
    try:
        engines = connection.Engines()
        
        TDevice = information.getDevices(engines.engine_dict["iotmgmt"])
        waterQualityData = information.getWaterQualityData(engines.engine_dict["iotmgmt"], TDevice["ieee"].to_numpy(), datetime.date.today())
        calculation.dailyCoolingWaterQuality(TDevice, waterQualityData, engines.engine_dict["reportplatform"], insert=True)
    except:
        print(datetime.datetime.now())
        traceback.print_exc(file=sys.stdout)
        print("================================================")
    finally:
        engines.close()

if __name__ == "__main__":
    main()