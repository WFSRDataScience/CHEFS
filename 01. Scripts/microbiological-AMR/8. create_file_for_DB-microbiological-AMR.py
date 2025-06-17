# -*- coding: utf-8 -*-
"""
Created on Sat Aug 31 06:52:32 2024

@author: hoend008
"""

import pandas as pd
import numpy as np
import pathlib

import sys
import os

import seaborn as sns 
import matplotlib.pyplot as plt

sys.path.insert(1, 'W:/WFSR/Projects/72250_Statistics_NP_Feed/11. Libs')
from WH_data_wrangling_functions import cleancolumns, df_tolower, df_trim
from PostgresDatabasev2 import PostgresDatabase


"""
GLOBALS
"""
# set output path
WORKDIR = pathlib.Path("C:/Users/hoend008/HOLIFOOD")

AMRPATH = pathlib.Path(WORKDIR, "03. AMR data/amr_v1.csv")

METAFILEPATH = pathlib.Path(WORKDIR, "column-meta-info-microbiological.xlsx")

SAVEPATH = pathlib.Path(WORKDIR, "03. AMR data/AMR_for_db.csv")

DB_USER = os.environ.get('DB_USER')
DB_PASSWORD = os.environ.get('DB_PASSWORD')


"""
HELPER FUNCTIONS
"""
def basic_processing(df: pd.DataFrame) -> pd.DataFrame:
    
    df = df_tolower(df)
    df = df_trim(df)
    df.columns = cleancolumns(df.columns)
    return df


"""
GET AMR DATA
"""
df = pd.read_csv(AMRPATH)

df_meta = pd.read_excel(METAFILEPATH)


"""
CREATE DICT FOR RENAME COLUMNS
"""
RENAME_DICT = {}
for i, row in df_meta.iterrows():
    RENAME_DICT[row['columnname']] = row['columnname_db']
    
    
"""
RENAME COLUMNS
"""
df_meta = df_meta.rename(columns={"merge": "new"})
df_meta['columnname'] = np.where(df_meta.new == df_meta.new, df_meta.new, df_meta.columnname)


"""
REMOVE DUPLICATES FROM df_meta
"""
df_meta = df_meta.drop_duplicates(subset=['columnname'])


"""
CREATE LIST OF ORDERED COLUMNS
"""
COLUMNS_ORDERED = list(df_meta.sort_values(by=['volgorde'])['columnname_db'])


"""
CONVERT CATALOGUE VALUES TO CATALOGUE DB ID's
"""
for i, row in df_meta.query("catalogue == catalogue").iterrows():
    col = row['columnname']
    cat = row['catalogue']
    print(col, " ", cat)

    # get db data
    with PostgresDatabase('owfsr', DB_USER, DB_PASSWORD) as db:
        dfdb = db.querydf(f"SELECT id, termcode FROM ontologies_efsa.{cat.lower()};")
        
    # replace values in col for dfdb
    dfdb_dict = {}
    for i, row in dfdb.iterrows():
        dfdb_dict[row['termcode']] = row['id']
        
    df = df.replace({col: dfdb_dict})
    

"""
RENAME COLUMNS
"""
df = df.rename(columns=RENAME_DICT)


"""
DROP COLUMNS
"""
COLUMNS_DROP = list(df_meta.query("drop == True")['columnname_db'])
COLUMNS_KEEP = [x for x in df.columns if x not in COLUMNS_DROP or x == 'filename']
df = df[COLUMNS_KEEP]


"""
ORDER COLUMNS
"""    
COLUMNS_ORDERED = [x for x in COLUMNS_ORDERED if x in df.columns or x == 'filename']
df = df[COLUMNS_ORDERED]


"""
GET DATABASE Tbl QUERY
"""
# for col in df.columns:
#     r = df_meta.query("columnname_db == @col")['sql'].reset_index()
#     print(str(r.sql[0]))
    
    
"""
SAVE AS CSV
"""
df.to_csv(SAVEPATH, index=False)


"""
PRINT COPY QUERY
"""
copy_query = f"\copy tmp_amr({','.join(df.columns)}) from '{SAVEPATH}' (header true, delimiter ',', format csv, encoding 'UTF-8');"
print(copy_query)
    

#"""
#\copy efsa.amr(seqtech_name,labtotisol,esbl_code,highest_code,matrix_code,syntestfep,sampcontext_code,syntestctx,permlst,totunitstested,analysism,progcode,totunitspositive,seqd,sampler_code,sampstage_code,syntestcaz,analysisd,seqtech_code,isolm,samporig_code,zoonosiscc,cc,percc,progsampstrategy_code,totsampunitstested,lowest_code,repyear,sampd,sampy,ampc_name,tracescode_code,zoonosis_code,progcode_code,mic_code,seqy,zoonosist,t,sampunittype_code,isoly,resultcode,samptype_code,totsampunitspositive,isold,zoonosisst,tracescode_name,anmethcode_code,cutoffvalue,repcountry_code,st,substance_code,labisolcode,sampm,seqm,ampc_code,analysisy,carbapenem_code) from 'C:\Users\hoend008\HOLIFOOD\03. AMR data\AMR_for_db.csv' (header true, delimiter ',', format csv, encoding 'UTF-8');
#"""