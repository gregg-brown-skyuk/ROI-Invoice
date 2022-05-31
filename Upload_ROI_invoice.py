#%%
# Importing libraries
import gzip, shutil, pandas as pd, json, os
from datetime import datetime as dt
from time import sleep
from selenium.webdriver import Firefox, FirefoxOptions
from selenium.webdriver.common.by import By
from cryptography.fernet import Fernet
from tkinter.filedialog import askdirectory
from pathlib import Path


# %%
class DownloadException(Exception):
  def __init__(self, message) -> None:
      super().__init__(message)
#%%
# Auto download from BTI server
def DownloadFile():
  print('Logging into BTI Reports Server...')
  fernet = Fernet(open('filekey.key', 'rb').read())
  with open('invoice_credentials.enc', 'rb') as creds:
      login = json.loads(fernet.decrypt(creds.read()))['BTIreports']

  saveDir = str(Path.home() / "Downloads")
  dlDir = askdirectory(title='Select folder to save file...')
  if dlDir == '':
    return dlDir

  options = FirefoxOptions()
  options.set_preference('security.tls.version.enable-deprecated', True)
  options.set_preference('browser.download.folderList', 2)
  options.set_preference('browser.download.dir', saveDir)
  options.set_preference('browser.download.manager.showWhenStarting', False)
  options.set_preference('browser.helperApps.neverAsk.saveToDisk', 'application/x-gzip')
  options.add_argument('--headless')

  print('Downloading gz file from server...')
  driver = Firefox(options=options)
  driver.get(f'https://{login["username"]}:{login["password"]}@reports.sns.sky.com/btb/bti/bills/')
  
  try:
    fileDate = f'{dt.strftime(dt.now(), "%Y%m")}01'
    fileName = f'{fileDate}000000_000083758_recurringcharges.csv.gz'
    gzFile = f'{saveDir}/{fileName}'
    csvFile = f'{dlDir}/BTI_Invoice_{dt.strftime(dt.now(), "%b_%y").lower()}.csv'
    driver.find_element(By.LINK_TEXT, fileName).click()
  except:
    driver.quit()
    raise DownloadException('No data found for this month')

  print('Waiting for download to complete...')
  while True:
    try:
      with gzip.open(gzFile, 'rb') as f_in:
        with open(csvFile, 'wb') as f_out:
            print('Decrompressing and copying file...')
            shutil.copyfileobj(f_in, f_out)
      driver.close()
      driver.quit()
      break
    except:
      pass
  print('Removing extra file...')
  sleep(2)
  os.remove(gzFile)
  return csvFile

# Extract from CSV and Upload to GCP
def ExtractAndUpload():

  print(f'Reading data from CSV\n\t{csvFile}')
  df = pd.read_csv(csvFile, skiprows=1, skipfooter=1, header=None,
        engine='python', dtype=str).drop(columns=[12,13,16])

  print('Formatting data...')
  df.insert(0,'00',dateStr)

  renameCols = {i:j for i,j in zip(df.columns, colSchema)}
  df = df.rename(columns=renameCols)
  for col in colSchema:
    if colSchema[col] != 'string':
      if colSchema[col] in ['int64', 'float64']:
        df[col].fillna(0, inplace=True)
      try: 
        df[col] = pd.to_datetime(df[col], format='%d/%m/%Y')
      except:
        df[col] = df[col].astype(colSchema[col])

  print(f'Uploading data to GCP...')
  try:
    df.to_gbq(destination_table=f'{dataset}.{table}',
              project_id=project,
              if_exists='replace',
              progress_bar=False, table_schema=gcpSchema)
    return True
  except:
    return False

# Reformat dates
def GetDates():
  # fileDate = dt.strftime(dateStr, '%Y%m01')
  dateStr = dt.strftime(dt.now(), '01/%m/%Y')
  table = f'roi_invoice_{dt.strftime(dt.now(), "%b_%y").lower()}'
  return dateStr, table
#%%
# Main process

if __name__ == '__main__':

  csvFile = DownloadFile()

  if csvFile != '':
    gcpSchema = [
      {'name':'advanced_billing_date', 'type': 'date'},
      {'name':'reseller_name', 'type': 'string'},
      {'name':'internal_number', 'type': 'string'},
      {'name':'order_number', 'type': 'string'},
      {'name':'original_ptt_no', 'type': 'float64'},
      {'name':'new_ptt_no', 'type': 'string'},
      {'name':'record_type', 'type': 'string'},
      {'name':'notification_type', 'type': 'string'},
      {'name':'billing_point_date', 'type': 'date'},
      {'name':'billing_end_date', 'type': 'date'},
      {'name':'number_of_clis', 'type': 'int64'},
      {'name':'notification_item', 'type': 'string'},
      {'name':'lead_cli', 'type': 'float64'},
      {'name':'reference_cli', 'type': 'float64'},
      {'name':'notification_item_description', 'type': 'string'},
      {'name':'chargeable_quantity', 'type': 'float64'},
      {'name':'number_of_days', 'type': 'int64'},
      {'name':'daily_amount', 'type': 'float64'},
      {'name':'full_period_amount', 'type': 'float64'},
      {'name':'signed_number', 'type': 'float64'}
      ]
    colSchema = {col['name']:col['type'] for col in gcpSchema}
    project = 'skyuk-uk-csgbillanalysis-dev'
    dataset = 'roi_rental'

    dateStr, table = GetDates()

    if ExtractAndUpload():
      print(f'Data uploaded to {dataset}.{table} for {dateStr}')
    else:
      print('Upload failed')
  else:
    print('Aborted without upload')
  sleep(5)