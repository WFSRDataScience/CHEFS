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

METAFILEPATH = pathlib.Path(WORKDIR, "column-meta-info-microbiological.xlsx")

SAVEPATH = pathlib.Path(WORKDIR, "03. AMR data/amr_v1.csv")

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

def replaceZero(x: int) -> float:
    """replace zero with np.nan"""
    if x == 0:
        return np.nan
    else:
        return float(x)
    
    
"""
GET META DATA
"""
df_meta = pd.read_excel(METAFILEPATH, sheet_name="meta-data")

# create dict to rename variables
renameVars = {}
for i, row in df_meta.iterrows():
    newName = row['merge']
    oldName = row['columnname']
    if newName == newName and newName != oldName:
        renameVars[oldName] = newName

# create list with variables to keep
keepColumns = list(df_meta.query("drop == False")['columnname'])

# list of columns for which 0.0 should be np.nan
dateColumnsToAdjust = ['analysisd', 'analysism', 'analysisy', 'isold', 'isolm', 'isoly', 'sampd', 'sampm', 'sampy']


"""
CREATE ONE DATAFRAME
"""
list_dfs = []
# get all zip files with 'amr' in the filename
for csvfile in list(WORKDIR.rglob("*AMR_PUB*.csv")):
    
    print(csvfile)
    
    # read data and basic processing
    tmp_df = pd.read_csv(csvfile)
    tmp_df = basic_processing(tmp_df)
    
    # replace n_a with np.nan
    tmp_df = tmp_df.replace('n_a', np.nan)
    
    # replace 0.0 for np.nan
    for col in dateColumnsToAdjust:
        tmp_df[col] = tmp_df.apply(lambda row : replaceZero(row[col]), axis=1)
    
    # select columns
    keepCols = [x for x in keepColumns if x in tmp_df.columns]
    tmp_df = tmp_df[keepCols]

    # add filename and year from filename
    tmp_df['filename'] = csvfile.name
    
    # rename
    tmp_df = tmp_df.rename(columns=renameVars)
    
    # append to list_dfs
    list_dfs.append(tmp_df)
    
# create one dataframe
df = pd.concat(list_dfs)


"""
ANALYZE MISSINGS AND CREATE HEATMAP
"""
# calculate missings over df
df_missings = df.isna().sum().reset_index().rename(columns={0:"n", "index": "columnname"})
df_missings['perc'] = df_missings['n'] / df.shape[0]  

# calculate missing per file in df
list_missings = []
listFiles = set(df.filename)
for file in listFiles:
    tmp_df = df.query("filename == @file")
    # calculate missings and append to list_missings
    tmp_df_missings = tmp_df.isna().sum().reset_index().rename(columns={0:"n", "index": "columnname"})
    tmp_df_missings['perc'] = tmp_df_missings['n'] / tmp_df.shape[0]
    tmp_df_missings['filename'] = file
    tmp_df_missings['fileYear'] = int(''.join([str(i) for i in file if i.isdigit()]))
    list_missings.append(tmp_df_missings)
    
df_missings_perfile = pd.concat(list_missings)

# draw heatmap
df_heatmap = df_missings_perfile.pivot(index=["fileYear", "filename"], columns="columnname", values="perc")
df_heatmap = df_heatmap.sort_values(by=['fileYear'])
sns.heatmap(df_heatmap)
plt.show() 


"""
SAVE AS CSV
"""
df.to_csv(SAVEPATH, index=False)