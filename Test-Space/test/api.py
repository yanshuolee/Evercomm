from flask import Flask, request
import sqlalchemy as sql

#IP
host = "localhost"
user = "admin"
pwd = "Admin99"
#DB name
db = "iotmgmt"

app = Flask(__name__)

@app.route('/gateway', methods = ['GET', 'POST', 'DELETE'])
def gateway():
    engine = sql.create_engine(f"mysql+mysqldb://{user}:{pwd}@{host}/{db}?charset=utf8", pool_recycle=3600)
    conn = engine.connect()
    
    if request.method == 'GET':
        return "GET method Unavailable."
    
    if request.method == 'POST':
        data = request.headers
        print(data)

        identity = request.headers.get("Identity")
        action = request.headers.get("Action")

        if identity == "ecogw" and action == "register":
            print("id=", identity, "action=", action)

            info = request.json
            print(info["serial"])
            print(info["eth0_mac"])
            print(info["wlan0_mac"])

            try:
                conn.execute(sql.text(f"insert into iotmgmt.TGateway (serialId, eth0_mac, wlan0_mac, InsID, InsDT) values ('{info['serial']}', '{info['eth0_mac']}', '{info['wlan0_mac']}', 'api.py', now())"))
                print(f"{info['serial']} inserted!")
            except sql.exc.IntegrityError as e:
                print(str(datetime.datetime.now()))
                print(e)
            return "200"
        else:
            return "Forbidden action!"

    engine.dispose()