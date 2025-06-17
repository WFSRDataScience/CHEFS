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
FILEPATH_LISTFILES = pathlib.Path(WORKDIR, "missings-AMR.csv")

MISSINGS_SAVEPATH = pathlib.Path(WORKDIR, "missingCountsVariables-AMR.xlsx")
MISSINGS_PIVOT_SAVEPATH = pathlib.Path(WORKDIR, "missingCountsVariablesPivot-AMR.xlsx")


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
LOOP OVER THE EXTRACTED CSV FILES AND CALCULATE THE MISSING VALUES PER VARIABLE PER FILE
PLOT THE OUTPUT IN A HEATMAP
"""
list_dfs = []
def main():  
    
    """
    LIST FILETYPE PER FILE
    - loop over all AMR_PUB csv files
    - calculate missing values per variable per file and save result in csv
    - plot heatmap
    """
    # get all zip files with 'amr' in the filename
    for csvfile in list(WORKDIR.rglob("*AMR_PUB*.csv")):
        
        print(csvfile)
        
        # read data and basic processing
        df = pd.read_csv(csvfile)
        df = basic_processing(df)
        
        # replace n_a with np.nan
        df = df.replace('n_a', np.nan)
        
        # calculate missings
        df_missings = df.isna().sum().reset_index().rename(columns={0:"n", "index": "columnname"})
        df_missings['perc'] = df_missings['n'] / df.shape[0]   
        
        # add filename
        df_missings['filename'] = csvfile.name
        df_missings['year'] =  int(''.join([str(i) for i in csvfile.name if i.isdigit()]))
        
        # append to list_dfs
        list_dfs.append(df_missings)
        
    # create one dataframe
    df_missings_total = pd.concat(list_dfs)
    
    # length filename
    df_missings_total['columnnameLenght'] = df_missings_total.columnname.str.len()

    return df_missings_total


"""
execute program
"""
df_missings_total = main()


"""
DRAW HEATMAP AND SAVE AS PNG
"""
df_heatmap = df_missings_total.pivot(index=["year", "filename"], columns="columnname", values="perc")
df_heatmap = df_heatmap.sort_values(by=['year'])
sns.heatmap(df_heatmap)
plt.show() 


"""
SAVE data RESULT AS CSV
"""
df_missings_total.to_excel(MISSINGS_SAVEPATH, index=False)
df_heatmap.to_excel(MISSINGS_PIVOT_SAVEPATH)