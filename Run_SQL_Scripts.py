# %%
from google.cloud import bigquery as bq
from datetime import datetime as dt

def InitializeVariables():
    monthName = dt.strftime(dt.now(), '%b_%y').lower()
    monthFolder = dt.strftime(dt.now(), '%B %Y')
    project_id = 'skyuk-uk-csgbillanalysis-dev'
    client = bq.Client(project=project_id, location='EU')
    saveDir = fr'\\skyshare.intranet.sky\sky\Cost Assurance\07 ROI Invoice\{monthFolder}'
    return monthName, monthFolder, saveDir, client

def GetSQL(sqlFile):
    return open(sqlFile).read().replace('#TABLEMONTH#', monthName)

def CreateInvestigations(products):
    for product in products:
        print(f'Running query for {product}')
        qry = GetSQL(fr'SQL\{product}_investigations(view).sql')
        saveFile = fr'{saveDir}\{product}_to_be_investigated_{monthName}.xlsx'
        result = client.query(query=qry).to_dataframe()
        result.to_excel(saveFile, sheet_name='Total', index=False)
    print('Completed')

# %%
if __name__ == '__main__':
    monthName, monthFolder, saveDir, client = InitializeVariables()
    CreateInvestigations(['BB', 'Talk'])

# # qry = open(r'SQL\4.create_bb_rentals_table.sql').read().replace('#TABLEMONTH#', monthName)
# client.query(query=qry)
# while True:
#     try:
#         df = client.list_rows(f'roi_rental.roi_{monthName}_talk_rentals').to_dataframe()
#         break
#     except:
#         pass

