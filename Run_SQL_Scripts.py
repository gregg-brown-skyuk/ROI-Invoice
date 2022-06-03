# %%
from google.cloud import bigquery as bq
from datetime import datetime as dt
import pandas_gbq as pdgbq

def InitializeVariables():
    monthName = dt.strftime(dt.now(), '%b_%y').lower()
    monthFolder = dt.strftime(dt.now(), '%B %Y')
    saveDir = fr'\\skyshare.intranet.sky\sky\Cost Assurance\07 ROI Invoice\{monthFolder}'
    project_id = 'skyuk-uk-csgbillanalysis-dev'
    dataset_id = 'roi_rental'
    client = bq.Client(project=project_id, location='EU')
    return monthName, monthFolder, saveDir, client, project_id, dataset_id

monthName, monthFolder, saveDir, client, project_id, dataset_id = InitializeVariables()

def GetSQL(sqlFile):
    return open(fr'SQL\{sqlFile}.sql').read().replace('#TABLEMONTH#', monthName)

def CreateInvestigations(products=['BB', 'Talk']):
    for product in products:
        print(f'Creating Investigations File for {product}')
        qry = GetSQL(fr'{product}_investigations(view)')
        saveFile = fr'{saveDir}\{product}_to_be_investigated_{monthName}.xlsx'
        result = pdgbq.read_gbq(query_or_table=qry, project_id=project_id)
        result.to_excel(saveFile, sheet_name='Total', index=False)
    print('Completed')

def RunQuery(sqlFile):
    qry = GetSQL(sqlFile)
    client.query(query=qry)

def CreateInvoiceSummary(products=['BB', 'Talk', 'SIRO']):
    for product in products:
        print(f'Creating Invoice Summary for {product}')
        qry = f'SELECT * FROM `{project_id}.{dataset_id}.invoice_summary_{product.lower()}`'
        saveFile = fr'{saveDir}\invoice_summary_{product}.csv'
        result = pdgbq.read_gbq(query_or_table=qry, project_id=project_id)
        result.to_csv(saveFile, index=False)
    print('Completed')

def CreateConnectionsCheck():
    print('Creating Connections Check')
    qry = 'SELECT * FROM `skyuk-uk-csgbillanalysis-dev.roi_rental.roi_connections_check`'
    saveFile = fr'{saveDir}\connections_check.csv'
    result = pdgbq.read_gbq(query_or_table=qry, project_id=project_id)
    result.to_csv(saveFile, index=False)
    print('Completed')

def CreateNBIRentals():
    print('Creating NBI Rentals')
    qry = GetSQL('8.nbi_rentals_breakdown')
    saveFile = fr'{saveDir}\nbi_rentals.csv'
    result = pdgbq.read_gbq(query_or_table=qry, project_id=project_id)
    result.to_csv(saveFile, index=False)
    print('Completed')

def CreateCostAssurance(products=['BB', 'Talk']):
    for product in products:
        print(f'Creating Cost Assurance for {product}')
        qry = GetSQL(fr'5.cost_assurance_{product.lower()}')
        saveFile = fr'{saveDir}\cost_assurance_{product}.csv'
        result = pdgbq.read_gbq(query_or_table=qry, project_id=project_id)
        result.to_csv(saveFile, index=False)
    print('Completed')
# %%
if __name__ == '__main__':
    CreateCostAssurance()
# %%
