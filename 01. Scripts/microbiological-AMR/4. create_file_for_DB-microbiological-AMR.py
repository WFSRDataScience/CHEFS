"""
Create csv ready for import into db
"""

import pandas as pd
import numpy as np
import pathlib
import os
from utils import cleancolumns, df_tolower, df_trim
from DBconnection import PostgresDatabase
from DBcredentials import DB_USER, DB_PASSWORD, DB_NAME, HOST


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

AMR_DATA_DIR = pathlib.Path(WORKDIR, "03. AMR data")
META_DATA_DIR = pathlib.Path(WORKDIR, "06. Meta data")

AMRPATH = pathlib.Path(AMR_DATA_DIR, "amr_v1.csv")

METAFILEPATH = pathlib.Path(META_DATA_DIR, "column-meta-info-microbiological.xlsx")

SAVEFILENAME = "AMR_for_db.csv"
SAVEPATH = pathlib.Path(AMR_DATA_DIR, SAVEFILENAME)

FILENAME_FILES_SAVE =  "copyqueries_amr.txt"
FILEPATH_COPYQUERIES_FILES = pathlib.Path(AMR_DATA_DIR, FILENAME_FILES_SAVE)


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
    with PostgresDatabase(DB_NAME, DB_USER, DB_PASSWORD, HOST) as db:
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
SAVE AS CSV
"""
df.to_csv(SAVEPATH, index=False)


"""
SAVE COPY QUERY
"""
# construct save Path
full_sample_dir_path = pathlib.Path(FULLPATH_MAIN_DIR, "03. AMR data")
full_sample_file_path = pathlib.Path(full_sample_dir_path, SAVEFILENAME) 

# create copy query
copy_query = f"\copy tmp_amr({','.join(df.columns)}) from '{full_sample_file_path}' (header true, delimiter ',', format csv, encoding 'UTF-8');"
with open(FILEPATH_COPYQUERIES_FILES, 'a') as f:
    f.write(copy_query + "\n")   
