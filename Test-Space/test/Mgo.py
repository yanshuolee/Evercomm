import pymongo
import pandas as pd
import time

uri = "mongodb://admin:Admin99@localhost"
client = pymongo.MongoClient(uri)
database = client["archiveplatform"]
collection = database["solarInverter_202009"]
cursor = collection.find(limit=100000)
s = time.time()
df = pd.DataFrame(list(cursor))
print(f"{time.time()-s} sec")
for doc in cursor:

    print()

print()