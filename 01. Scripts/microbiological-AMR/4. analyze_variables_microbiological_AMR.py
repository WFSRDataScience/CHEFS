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
#WORKDIR = pathlib.Path('W:/WFSR/Projects/74141-HOLiFOODWP1/Data/EFSA (zenodo)/')
WORKDIR = pathlib.Path("C:/Users/hoend008/HOLIFOOD")

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
CREATE ONE DATAFRAME
"""
list_dfs = []
# get all zip files with 'amr' in the filename
for csvfile in list(WORKDIR.rglob("*AMR_PUB*.csv")):
    
    print(csvfile)
    
    # read data and basic processing
    df = pd.read_csv(csvfile)
    df = basic_processing(df)
    
    # replace n_a with np.nan
    df = df.replace('n_a', np.nan)
    
    # add filename
    df['filename'] = csvfile.name
    
    # append to list_dfs
    list_dfs.append(df)
    
# create one dataframe
df = pd.concat(list_dfs)


"""
COMPARE COLUMNS
"""
col1 = 'zoonosis_code'
col2 = f"{col1}_code"
query1 = f"{col1} == {col1}"
query2 = f"{col2} == {col2}"
COLUMNS = [col1, col2]

print(df.query(query1).shape, "\t\t ", round(df.query(query1).shape[0] / df.shape[0], 6) * 100, " %")
print(df.query(query1)[col1].head())

print(df.query(query2).shape, "\t\t ", round(df.query(query2).shape[0] / df.shape[0], 6) * 100, " %")
print(df.query(query2)[col2].head())

print(set(df[col1]) - set(df[col2]))
print(set(df[col2]) - set(df[col1]))

print(df[col1].value_counts())
print(df[col2].value_counts())


