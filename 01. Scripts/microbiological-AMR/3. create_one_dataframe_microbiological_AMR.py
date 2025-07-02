"""
Take all csv raw data files and create one dataframe
Save that dataframe to a csv
"""
import pandas as pd
import numpy as np
import pathlib
import os
from utils import df_trim, df_tolower, cleancolumns


"""
SETTINGS
"""
import warnings
warnings.filterwarnings('ignore')


"""
GLOBALS
"""
WORKDIR = "../../"

FULLPATH_MAIN_DIR = os.getcwd()
FULLPATH_MAIN_DIR = os.path.abspath(os.path.join(FULLPATH_MAIN_DIR ,"../.."))

META_DATA_DIR = pathlib.Path(WORKDIR, "06. Meta data")
FILE_DATA_DIR = pathlib.Path(WORKDIR, "07. Data Files")

METAFILEPATH = pathlib.Path(META_DATA_DIR, "column-meta-info-microbiological.xlsx")

SAVEPATH = pathlib.Path(WORKDIR, "03. AMR data/amr_v1.csv")


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
for csvfile in list(pathlib.Path(FULLPATH_MAIN_DIR).rglob("*AMR_PUB*.csv")):
    
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
SAVE AS CSV
"""
df.to_csv(SAVEPATH, index=False)