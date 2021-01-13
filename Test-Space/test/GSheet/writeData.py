import gspread
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
import datetime

"""
This code is a modified version of https://www.danielecook.com/from-pandas-to-google-sheets/
Last access: Oct 28, 2020
Modified by Y.S.L
"""

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

scope = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']

credentials = ServiceAccountCredentials.from_json_keyfile_name('/home/admin/test/GSheet/rosy-strata-293907-9858b3057bb5.json', scope)

gc = gspread.authorize(credentials)

# workbook = gc.open_by_key("<workbook id>")
workbook = gc.open_by_url("https://docs.google.com/spreadsheets/d/1pQwf2npPBjL6TPDh75GjLSJNAB1-tcjcHg-fDCKCR3E/edit?usp=sharing")
sheetTitle = str(datetime.date.today() - datetime.timedelta(days=1))
workbook.add_worksheet(title=sheetTitle, rows="100", cols="20")
sheet = workbook.worksheet(sheetTitle)

# df = pd.read_csv("/home/admin/check-85.csv")
df = pd.read_csv("/home/admin/out.csv")
pandas_to_sheets(df, sheet)