import gspread
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
import requests
import sys
import json
import datetime
try:
    import information
except:
    from . import information

def GSheet(_data, _type):
    def iter_pd(df):
        for val in df.columns:
            yield val
        for row in df.to_numpy():
            for val in row:
                if pd.isna(val):
                    yield ""
                else:
                    yield val

    def pandas_to_sheets(pandas_df, sheet, clear = True):
        # Updates all values in a workbook to match a pandas dataframe
        if clear:
            sheet.clear()
        (row, col) = pandas_df.shape
        cells = sheet.range("A1:{}".format(gspread.utils.rowcol_to_a1(row + 1, col)))
        for cell, val in zip(cells, iter_pd(pandas_df)):
            cell.value = val
        sheet.update_cells(cells)

    workbookURL = "https://docs.google.com/spreadsheets/d/1x5A7ydvDZ6JKuW6H8S7ejs_w4l7RXECsgjym_bDG0ko/edit?usp=sharing"
    scope = ['https://spreadsheets.google.com/feeds',
            'https://www.googleapis.com/auth/drive']
    auth = information.getGoogleAuth()
    credentials = ServiceAccountCredentials.from_json_keyfile_name(auth, scope)
    gc = gspread.authorize(credentials)

    workbook = gc.open_by_url(workbookURL)
    if _type == "dq":
        sheetTitle = str(datetime.date.today() - datetime.timedelta(days=1))+"-DataQuality"
    elif _type == "wd":
        sheetTitle = str(datetime.date.today() - datetime.timedelta(days=1))+"-WarningLog"
    
    try:
        workbook.add_worksheet(title=sheetTitle, rows="100", cols="20")
    except:
        sheet = workbook.worksheet(sheetTitle)
        workbook.del_worksheet(sheet)
        workbook.add_worksheet(title=sheetTitle, rows="100", cols="20")
    sheet = workbook.worksheet(sheetTitle)

    pandas_to_sheets(_data, sheet)

    return workbookURL

def Line(_data, _type=None):
    gURL = GSheet(_data, _type)
    token = information.getLineToken()
    headers = {
        "Authorization": "Bearer " + token, 
        "Content-Type" : "application/x-www-form-urlencoded"
    }
    data = {"message": gURL}
    r = requests.post("https://notify-api.line.me/api/notify", headers = headers, data = data)
    print(r.status_code)

def Slack(_data):
    url = information.getSlackURL()
    headers = {
        "Content-Type" : "application/json"
    }
    data = {
        "text": _data
    }
    data = json.dumps(data)
    r = requests.post("https://hooks.slack.com/services/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", headers = headers, data = data)
    print(r.status_code)