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
GET AMR DATA
"""
df = pd.read_csv(AMRPATH)


"""
ANALYZE DATA-STRUCTURE
"""
GROUPBY_COLUMN = ['filename', 'labisolcode']
OTHER_COLUMNS = [x for x in df.columns if x not in GROUPBY_COLUMN]
GROUP_DICT = {x: ['nunique', 'size'] for x in OTHER_COLUMNS}
r = df.groupby(GROUPBY_COLUMN).agg(GROUP_DICT).reset_index()

# rename columns
r.columns = [x[0] + "_" + x[1] if x[1] != '' else x[0] for x in r.columns]

# all columns with max nunique = 1
r['id'] = 1
R2_COLUMNS = [x for x in r.columns if 'nunique' in x]
r2 = r.groupby(['id'])[R2_COLUMNS].max().reset_index()
r2 = r2.melt(id_vars=['id'])

a = r.query("progcode_nunique == 3")

b = df.query("labisolcode == '33423143434241353039314342313844'")