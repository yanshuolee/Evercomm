import sqlalchemy as sql
import pandas as pd
import numpy as np
import copy
import time
import datetime
import math
import json
import argparse
import sys, traceback
import os
import itertools
from toolkits import connection, information, calculation

def main(DQ=False, WD=False):
    engines = connection.Engines()
    iotEngine = engines.engine_dict["iotcomui"]
    ieeeListsForDQ = information.getIeeeListsForDQ(iotEngine)
    ieeeListsForWD = information.getIeeeListsForWD(iotEngine)

    # if DQ:
    #     DQ = calculation.dataQualityAlgo(engines, ieeeListsForDQ)
    #     DQ.calculateAllDay()
    #     DQ.calculateAllDayComplicated()
    #     DQ.pushNotify()

    if WD:
        WD = calculation.warningDetector(engines, ieeeListsForWD, insert=True)
        WD.detectAll()
        # WD.pushNotify()

    engines.close()

if __name__ == "__main__":
    DQ = True
    WD = True
    main(DQ, WD)