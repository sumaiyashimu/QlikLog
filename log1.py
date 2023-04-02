import pandas as pd
import xlsxwriter
import pyodbc
import json
import os
from datetime import datetime, timedelta
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
import params as params
import requests
import xlrd
from pandas import DataFrame
from sqlalchemy.testing import db
from tabulate import tabulate
from datetime import datetime
from datetime import datetime, timedelta
import requests
from pandas import json_normalize
# API
yesterday = datetime.now() - timedelta(2)

currentDate = datetime.strftime(yesterday, '%Y-%m-%d')
url = "https://bi-erp.ap.qlikcloud.com/api/v1/audits/archive?date=" + \
    str(currentDate)
payload = {}
headers = {
    'Authorization': 'Bearer eyJhbGciOiJFUzM4NCIsImtpZCI6ImYyMDdhYzYxLWZkNzktNGYwNi05MDBlLTQ2M2FjN2I4YzI5MyIsInR5cCI6IkpXVCJ9.eyJzdWJUeXBlIjoidXNlciIsInRlbmFudElkIjoiQldqRVJFcXpBOEtDRDNpQTJhWXl3dWxnU0dUMEhxYXkiLCJqdGkiOiJmMjA3YWM2MS1mZDc5LTRmMDYtOTAwZS00NjNhYzdiOGMyOTMiLCJhdWQiOiJxbGlrLmFwaSIsImlzcyI6InFsaWsuYXBpL2FwaS1rZXlzIiwic3ViIjoiUlBjZi1KM3ZfY3dpUWNnUFVwOFRTZGhCdV9McTkzenAifQ.KfpiKDh37E76spMzqBpKA6Hmh-HDDNTU48UujV56vIq9dGTaK7BasRWHjAFtbzEvbSrA-2E7epdNwx4XZDixHq59zdCXnQUXK7l2z2L1VG7tQF6bygvRfVczzCNT4L2J'
}

response = requests.request("GET", url, headers=headers, data=payload)
respons = response.text
respons = json.loads(respons)
df = json_normalize(respons['data'])

json_object = json.dumps(respons, indent=4)

with open("data.json", "w") as jsonFile:
    jsonFile.write(json_object)


df = df[["source", "contentType", "eventType",
         "eventId", "eventTime", "tenantId", "userId", "data"]]

# Connection
conn = pyodbc.connect('DRIVER={SQL Server};'
                      'SERVER=10.168.2.127;'
                      'DATABASE=QlikSense;'
                      'UID=sa;PWD=erp')
cursor = conn.cursor()

for index, row in df.iterrows():
    cursor.execute("Insert Into QlikSense.dbo.QlikSenseLogData (source, contentType, eventType, eventId, eventTime, tenantId, userId, data) values (?, ?, ?, ?, ?, ?, ?,?)", (str(
        row.source), str(row.contentType), str(row.eventType), str(row.eventId), str(row.eventTime), str(row.tenantId), str(row.userId), str(row.data.id)))
conn.commit()
cursor.close()

print('Data Has been Saved into DATABASE...')

with pd.ExcelWriter("EXCEL/QlikSenseLogData.xlsx", engine="xlsxwriter", options={'strings_to_numbers': True, 'strings_to_formulas': False}) as writer:
    try:
        df = pd.read_sql(""" Select * from QlikSenseLogData """, conn)
        df.to_excel(writer, sheet_name="Sheet1", header=True, index=False)
        print("File saved successfully!")
    except:
        print("There is an  database error")