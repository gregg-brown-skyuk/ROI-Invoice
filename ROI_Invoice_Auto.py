import Upload_ROI_invoice as ROI
import Upload_SIRO_invoice as SIRO
import pandas_gbq as pdgbq
from datetime import datetime as dt
from google.cloud.bigquery import Client
from os import mkdir
from Run_SQL_Scripts import *

if __name__ == '__main__':
    monthName = dt.strftime(dt.now(), '%b_%y').lower()
    monthFolder = dt.strftime(dt.now(), '%B %Y')
    saveDir = fr'\\skyshare.intranet.sky\sky\Cost Assurance\07 ROI Invoice\{monthFolder}'
    try:
        mkdir(saveDir)
    except FileExistsError:
        pass
    
    ROI.ExtractAndUpload()
    SIRO.ExtractAndUpload()
    RunQuery('1.union_backup')
    RunQuery('2.union_create')
    CreateInvoiceSummary()
    CreateConnectionsCheck()
    CreateCostAssurance()
    CreateConnectionsMissing()
    CreateInvestigations()
    CreateNBIRentals()