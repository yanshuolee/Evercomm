import json
def initJsonObj(start, end):
    jsonObj = {"realPowerGeneration":{}, "budgetPowerGeneration":{}, "referencePowerGeneration":{},
               "stationPowerGeneration":{}, "predictPowerGeneration":{}, "realIrradiation":{}, "realPanelTemperature":{}}
    for hour in range(start, end+1):
        jsonObj["realPowerGeneration"][f"{hour:02}H"] = {"data": 0}
        jsonObj["budgetPowerGeneration"][f"{hour:02}H"] = {"data": 0}
        jsonObj["referencePowerGeneration"][f"{hour:02}H"] = {"data": 0}
        jsonObj["stationPowerGeneration"][f"{hour:02}H"] = {"data": 0}
        jsonObj["predictPowerGeneration"][f"{hour:02}H"] = {"data": 0}
        jsonObj["realIrradiation"][f"{hour:02}H"] = {"data": 0}
        jsonObj["realPanelTemperature"][f"{hour:02}H"] = {"data": 0}
    
    jsonObj["realPowerGeneration"] = json.dumps(jsonObj["realPowerGeneration"])
    jsonObj["budgetPowerGeneration"] = json.dumps(jsonObj["budgetPowerGeneration"])
    jsonObj["referencePowerGeneration"] = json.dumps(jsonObj["referencePowerGeneration"])
    jsonObj["predictPowerGeneration"] = json.dumps(jsonObj["predictPowerGeneration"])
    jsonObj["stationPowerGeneration"] = json.dumps(jsonObj["stationPowerGeneration"])
    jsonObj["realIrradiation"] = json.dumps(jsonObj["realIrradiation"])
    jsonObj["realPanelTemperature"] = json.dumps(jsonObj["realPanelTemperature"])
    return jsonObj

def initdailySRJsonObj(start, end):
    jsonObj = {}
    for hour in range(start, end+1):
        jsonObj[f"{hour:02}H"] = {"data": 0}
    return jsonObj

if __name__ == "__main__":
    initJsonObj(6, 19)
    