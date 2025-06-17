"""
Creates csv files with sample core data for import into DB
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
WORKDIR = "../"

FULLPATH_MAIN_DIR = os.getcwd()
FULLPATH_MAIN_DIR = os.path.abspath(os.path.join(FULLPATH_MAIN_DIR ,".."))

META_DATA_DIR = pathlib.Path(WORKDIR, "06. Meta data")
FILE_DATA_DIR = pathlib.Path(WORKDIR, "07. Data Files")

FILE_COLUMNS = "column-meta-info.xlsx"
FILEPATH_COLUMNS = pathlib.Path(META_DATA_DIR, FILE_COLUMNS)

PATH_INVALID_VALUES = pathlib.Path(FILE_DATA_DIR, "invalid_values_core.csv")

FILENAME_SAVE = "sample_core.csv"
CSV_SAVE_PATH = pathlib.Path(FILE_DATA_DIR, FILENAME_SAVE)
FILEPATH_COPYQUERIES_CORE_SAMPLE = pathlib.Path(FILE_DATA_DIR, "copyqueries_core_sample.txt")


"""
HELPER FUNCTIONS
"""
def basic_processing(df: pd.DataFrame) -> pd.DataFrame:

    # all data to lower and trim
    df = df_tolower(df)
    df = df_trim(df)
     
    # clean columnsnames (lowercase, remove special characters etc)
    df.columns = cleancolumns(df.columns)
    
    # return df
    return df
    

"""
GET RELEVANT COLUMNS
"""
df_cols = pd.read_excel(FILEPATH_COLUMNS, sheet_name="column_metaInfo")
df_cols = basic_processing(df_cols)

# select only sample columns
df_cols = df_cols.query("sample_measurement == 's'")

# for sample table
SAMPLE_COLUMNS_DICT = dict(zip(df_cols.columnname, df_cols.datatype))
SAMPLE_COLUMNS_LIST = list(df_cols.columnname)

# get sample core columns
df_cols_core = df_cols.query("core_sample == 1")

# create a dict with core columns and corresponding catalogues. Create a list with core columns
CORE_COLUMNS_DICT = dict(zip(df_cols_core.columnname, df_cols_core.catalogue))
CORE_COLUMNS_LIST = list(df_cols_core.columnname)


"""
GET ALL DIRECTORIES IN WORKDIR
"""
DIRECTORIES = [{'country': dI, 'path': pathlib.Path(FILE_DATA_DIR,dI)} for dI in os.listdir(FILE_DATA_DIR) if os.path.isdir(os.path.join(FILE_DATA_DIR,dI))]   
DIRECTORIES = [x for x in DIRECTORIES if len(x['country']) == 2]


"""
READ IN ALL SAMPLE CSV FILES AND CONCAT THEM INTO 1 FILE THAT HAS THE SAMPLE CORE INFORMATION
"""
def main():

    # store individual dataframes in list_dfs
    list_dfs = []

    # loop over directories, read in and process sample data
    for DIR in DIRECTORIES:
        
        COUNTRY = DIR['country']
        print("------------------------------------------------")
        print(COUNTRY)
        
        # get sample dir
        sample_dir = pathlib.Path(DIR['path'], 'sample')
        
        # get sample csv in sample dir
        CSV_PATH = pathlib.Path(sample_dir, f"sample_{COUNTRY}.csv")
        
        # skip country if a sample csv file does not exist
        fileExist = os.path.exists(CSV_PATH)
        if not fileExist:
            print(f"Skip {COUNTRY}. sample file does not exist")
            continue
        
        # read in sample csv file
        tmp_df = pd.read_csv(CSV_PATH)

        # get only core sample columns from tmp_df
        # add a file column
        tmp_df_columns = [x for x in CORE_COLUMNS_LIST if x in tmp_df.columns]
        tmp_df_columns.sort()
        tmp_df_columns = tmp_df_columns + ['file']
        tmp_df = tmp_df[tmp_df_columns]
        
        # add to list
        list_dfs.append(tmp_df)
         
    # append to one df
    df = pd.concat(list_dfs)
    
    # convert repyear to float
    df['repyear'] = df['repyear'].astype(float)

    # add core columns in case they are not yet in df
    missing_core_columns = list(set(CORE_COLUMNS_LIST) - set(df.columns))
    for m in missing_core_columns:
        print(f"adding core column {m}")
        df[m] = np.nan

    # column check
    #print(set(df.columns) - set(SAMPLE_COLUMNS_LIST))
    #print(set(SAMPLE_COLUMNS_LIST) - set(df.columns))
    
    # delete list_dfs
    del list_dfs
    del tmp_df
    
    # write to csv
    df_columns = list(df.columns)
    df_columns.sort()
    df = df[df_columns]
    df.to_csv(CSV_SAVE_PATH, index=False)

    # sql query
    full_sample_dir_path = pathlib.Path(FULLPATH_MAIN_DIR, "07. Data Files")
    full_sample_file_path = pathlib.Path(full_sample_dir_path, FILENAME_SAVE)  

    copy_query = f"\copy efsa.tmp_sample_core({','.join(df.columns)}) from '{full_sample_file_path}' (header true, delimiter ',', format csv, encoding 'UTF-8');"
    with open(FILEPATH_COPYQUERIES_CORE_SAMPLE, 'a') as f:
        f.write(copy_query + "\n")

    return df
        

"""
ANALYZE CORE SAMPLE COLUMNS THAT HAVE A CATALOGUE FOR VALID VALUES
"""

def analyzeCoreSampleColumns(df: pd.DataFrame) -> None:
    
    # loop over all core columns, and for those with a catalogue check values with DB
    for column, catalogue in CORE_COLUMNS_DICT.items():
        
        # skip to next column in case of no catalogue.
        if catalogue == '0':
            continue
            
        # create a set with unique values (without nan) for the core column from df
        df_values = list(df[column].copy())
        df_values = [x for x in df_values if x == x]
        df_values = set(df_values)
        
        # get values from db
        if catalogue == 'mtx':
            with PostgresDatabase(DB_NAME, DB_USER, DB_PASSWORD, HOST) as db:
                dfdb = db.querydf(f"SELECT id, termcode, 'mtx' as catalogue FROM ontologies_efsa.mtx union select id, termcode, 'matrix' AS catalogue from ontologies_efsa.matrix;")        
        else:
            with PostgresDatabase(DB_NAME, DB_USER, DB_PASSWORD, HOST) as db:
                dfdb = db.querydf(f"SELECT id, termcode FROM ontologies_efsa.{catalogue};")
    
        # check if all values in df_values are also in dfdb['termcode']
        values_not_in_dfdb = pd.DataFrame(df_values - set(dfdb['termcode']))
        values_not_in_dfdb['column'] = column
        values_not_in_dfdb['catalogue'] = catalogue
        
        # print values_not_in_dfdb if present
        if values_not_in_dfdb.shape[0] > 0:
            print(f"INVALID VALUES FOR {column} --------------------------------------")
            values_not_in_dfdb.to_csv(PATH_INVALID_VALUES, mode='a', index=False, header=False)
        else:
            print(f"{column} VALID")
            
        del df_values
        del dfdb
        del values_not_in_dfdb
        

if __name__ == "__main__":
    df = main()

    print("\nChecking validity of data with catalogues")
    
    analyzeCoreSampleColumns(df)