# -*- coding: utf-8 -*-
"""
Created on Mon Aug 26 06:50:03 2024

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

CATELOGUE_DIR = pathlib.Path("W:/WFSR/Projects/74141-HOLiFOODWP1/Data/Harmonized terminology for scientific research/version v8/DCF_catalogues")

AMRPATH = pathlib.Path(WORKDIR, "03. AMR data/amr_v1.csv")

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
GET CATALOGUE DATA
"""
dictsCatalogues = {}
# get all zip files with 'amr' in the filename
for xlsxfile in list(CATELOGUE_DIR.rglob("*.xlsx")):
    
    try:
        tmp_df = pd.read_excel(xlsxfile, sheet_name='term')
        print(xlsxfile.name)
        tmp_df = basic_processing(tmp_df)
        tmp_df = tmp_df[['termcode', 'termextendedname']]
        dictsCatalogues[xlsxfile.name] = tmp_df
    except:
        print(f"ERROR for {xlsxfile.name}")
        
# print nr of rows per catalogue
for key, value in dictsCatalogues.items():
    print(f"{key} nr of rows \t {value.shape[0]}")
    
"""
GET AMR DATA
"""
df = pd.read_csv(AMRPATH)
    

"""
DETERMINE WHICH CATALOGUE FOR A COLUMN
"""
# determine columns to be checked for catalogue
COLUMNS = ['anmethcode_code', 'highest_code', 'lowest_code', 'matrix_code', 'mic_code',
           'progsampstrategy_code', 'repcountry_code', 'sampcontext_code', 'sampler_code',
           'sampstage_code', 'samptype_code', 'sampunittype_code', 'substance_code',
           'zoonosis_code', 'ampc_code', 'carbapenem_code', 'esbl_code', 'progcode_code',
           'seqtech_code', 'tracescode_code']

# create result list
result_list = []

# loop over the columns and for every column merge the aggregated data from df with every catalogue to 
# determine the percentage of coverage. Append the result to result_list
for column in COLUMNS:
    print(column)
    columnValues = df[column].value_counts().reset_index().rename(columns={"index": "termcode"})
    
    resultDict = []
    for key, value in dictsCatalogues.items():
        tmp_df = columnValues.merge(value, on=['termcode'], how='left')
        tmp_df['match'] = np.where(tmp_df.termextendedname == tmp_df.termextendedname, 1, 0)
        percentage = sum(tmp_df.match) / tmp_df.shape[0]
        resultDict.append({"file": key, "percentage": percentage})
    
    tmp_result_df = pd.DataFrame(resultDict)
    tmp_result_df['column'] = column
    
    result_list.append(tmp_result_df)

# create a dataframe    
df_result = pd.concat(result_list)

# remove zeros
df_result = df_result.query("percentage > 0")

# sort
df_result = df_result.sort_values(by=['column', 'percentage'], ascending=[True, False])
df_result = df_result.query("percentage == 1")
