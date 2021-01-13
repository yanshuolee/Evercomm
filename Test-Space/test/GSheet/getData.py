import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd

scope = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']

credentials = ServiceAccountCredentials.from_json_keyfile_name(
         '/home/admin/test/GSheet/rosy-strata-293907-9858b3057bb5.json', scope) # Your json file here

gc = gspread.authorize(credentials)

# wks = gc.open("NYC subway data").sheet1
wks = gc.open_by_url('https://docs.google.com/spreadsheets/d/1EAvL_val_ju05ad27VHMEclxNCV7e7DsbpetjuhRZOU/edit?usp=sharing').sheet1

data = wks.get_all_values()
headers = data.pop(0)

df = pd.DataFrame(data, columns=headers)
print(df.head())