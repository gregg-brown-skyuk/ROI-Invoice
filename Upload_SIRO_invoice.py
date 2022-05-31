#%%
# Import Libraries

from selenium.webdriver import Firefox, FirefoxOptions
from selenium.webdriver.common.by import By
from cryptography.fernet import Fernet
from datetime import datetime as dt
import json, pandas as pd, numpy as np
from tkinter.filedialog import askdirectory
from time import sleep

#%%
# Get invoice date

tableDate = dt.strftime(dt.now(), "%b_%y").lower()
fileDate = dt.strftime(dt.now(), "%Y%m01")
insertDate = dt.strftime(dt.now(), '01/%m/%Y')

print(f'Running SIRO upload for {insertDate}...')
#%%
# get login details from file

fernet = Fernet(open('filekey.key', 'rb').read())
with open('invoice_credentials.enc', 'rb') as creds:
    login = json.loads(fernet.decrypt(creds.read()))['CartesianISR']

#%%
# Login to Cartesian ISR and retrive data

options = FirefoxOptions()
options.add_argument('--headless')
browser = Firefox(options=options)
url = {
    'login' : 'http://uptom010.bskyb.com:8080/web/mainPage.do',
    'file' : f'http://uptom010.bskyb.com:8080/servlet/LogFileBrowserServlet?file=/staging/bbfinance/bti_billing/{fileDate}000000_1234_recurringcharges.csv.gz&'
    }

xpath = {
    'openlogn' : '/html/body/table/tbody/tr[1]/td/table/tbody/tr/td/table/tbody/tr[2]/td[2]/div/div/div/div/div/table/tbody/tr/td[2]/div/table/tbody/tr/td[2]',
    'username' : '/html/body/div[2]/div/div/div/div/table/tbody/tr/td[2]/div/form/table/tbody/tr[1]/td[2]/input',
    'password' : '/html/body/div[2]/div/div/div/div/table/tbody/tr/td[2]/div/form/table/tbody/tr[2]/td[2]/input',
    'loginbtn' : '/html/body/div[2]/div/div/div/div/table/tbody/tr/td[2]/div/form/table/tbody/tr[3]/td/input'
}

print('Getting data from Catesian ISR...')
browser.get(url['login'])
browser.find_element(By.XPATH, xpath['openlogn']).click()
browser.find_element(By.XPATH, xpath['username']).send_keys(login['username'])
browser.find_element(By.XPATH, xpath['password']).send_keys(login['password'])
browser.find_element(By.XPATH, xpath['loginbtn']).click()
browser.get(url['file'])

fileText = browser.find_element(By.TAG_NAME, 'pre').text
webData = pd.DataFrame([row.split(',') for row in fileText.split('\n')][1:-1])

browser.quit()
#%%
# Format Data

print('Formatting data...')
fileData = webData.drop(columns=[3,4,9,12,13,14,17,19,20])
fileData.insert(0,'00',insertDate)

gcpSchema = [
    {'name': 'advanced_billing_date', 'type': 'date'},
    {'name': 'invoice', 'type': 'string'},
    {'name': 'detail', 'type': 'string'},
    {'name': 'order_detail', 'type': 'string'},
    {'name': 'record_type', 'type': 'string'},
    {'name': 'notification_type', 'type': 'string'},
    {'name': 'billing_point_date', 'type': 'date'},
    {'name': 'billing_point_end', 'type': 'date'},
    {'name': 'notification_item', 'type': 'string'},
    {'name': 'directory_number', 'type': 'string'},
    {'name': 'notification_item_description', 'type': 'string'},
    {'name': 'quantity', 'type': 'int64'},
    {'name': 'days', 'type': 'int64'},
    {'name': 'cost', 'type': 'float64'}
    ]
colSchema = {col['name']:col['type'] for col in gcpSchema}
#%%

renameCols = {oldCol:newCol for oldCol,newCol in zip(fileData.columns, colSchema)}
fileData = fileData.rename(columns=renameCols)

for col,colFrmt in colSchema.items():
    if colFrmt != 'string':
        fileData[col] = fileData[col].replace(r'^\s*$', np.nan, regex=True)
        if colFrmt == 'date':
            fileData[col] = pd.to_datetime(fileData[col], format='%d/%m/%Y')
        elif colFrmt in ['int64', 'float64']:
            fileData[col].fillna(0, inplace=True)
            fileData[col] = fileData[col].astype(colFrmt)

#%%
# Save Dataframe as CSV
print('Saving CSV file')
saveDir = askdirectory(title="Choose where to save downloaded data...", mustexist=True)
if saveDir != '':
    fileData.to_csv(f'{saveDir}/SIRO_invoice_{tableDate}.csv')
else:
    print('CSV file not saved')

#%%
# Upload to GCP

project = 'skyuk-uk-csgbillanalysis-dev'
dataset = 'roi_rental'
table = f'siro_{tableDate}'
print(f'Uploading data to {dataset}.{table}')
try:
    fileData.to_gbq(destination_table=f'{dataset}.{table}',
                    project_id=project,
                    if_exists='replace',
                    table_schema=gcpSchema)
    print('Upload complete!')
    sleep(5)
except:
    print('Upload failed!')
    sleep(5)
