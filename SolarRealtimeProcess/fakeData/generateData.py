import sqlalchemy as sql
import datetime
import json
import sys
import pathlib
import sys, traceback

#IP
srcHost = "localhost"
srcUser = "admin"
srcPwd = "Admin99"

#DB name
dbDataplatform = "dataplatform"

def truncateTable():
    # Engine
    dataplatform_engine = sql.create_engine(f"mysql+mysqldb://{srcUser}:{srcPwd}@{srcHost}/{dbDataplatform}?charset=utf8", pool_recycle=3600*7)
    conn = dataplatform_engine.connect()

    insertString = "TRUNCATE `dataplatform`.`solarInverter`"
    conn.execute(insertString)
    dataplatform_engine.dispose()

if __name__ == "__main__":
    
    currentTS = datetime.datetime.now()
    currentTS_min = currentTS.minute
    if currentTS_min % 2 != 0:
        print("currentTS_min % 2 == 0")
        sys.exit()

    pwd = str(pathlib.Path("__file__").parent.absolute())
    print(f"Path: {pwd}")

    with open(f"{pwd}/flag.txt") as f: 
        flag = f.read() 

    if flag != '0':
        print("other TWarning running.")
        sys.exit()
    else:
        with open(f"{pwd}/flag.txt", "w") as f: 
            f.write("1")

    try:
        # Engine
        dataplatform_engine = sql.create_engine(f"mysql+mysqldb://{srcUser}:{srcPwd}@{srcHost}/{dbDataplatform}?charset=utf8", pool_recycle=3600*7)
        conn = dataplatform_engine.connect()

        currentTime = str(datetime.datetime.now())[:16]
        ins = ""
        for i in range(1000-1, 1000+4722-1):
            ieee = f'test_{i}'
            ins += f"""('{currentTime}', '2014', """ + f"'{ieee}', " + f"'{currentTime}'," + f""" NULL, '95247.7', NULL, '{json.dumps({"temp1": 44.4, "temp2": 46.6}, ensure_ascii=False)}', '{json.dumps({"voltage1": 667.5, "voltage2": 622.2}, ensure_ascii=False)}', '3702.0', '220.1', '220.5', '220.5', '5.8', '5.8', '5.7', NULL, '3632.0', NULL, '0.9900', '60.05', NULL, NULL, NULL, NULL, '{json.dumps({"current1": 2.2, "current2": 3.2}, ensure_ascii=False)}', NULL) , """

        ins = ins[:-2]

        insertString = f"replace into dataplatform.solarInverter values {ins}"
        conn.execute(insertString)
        print(f"{currentTime} inserted.")
        
    except:
        traceback.print_exc(file=sys.stdout)
    finally:
        with open(f"{pwd}/flag.txt", "w") as f: 
            f.write("0")
        
        dataplatform_engine.dispose()