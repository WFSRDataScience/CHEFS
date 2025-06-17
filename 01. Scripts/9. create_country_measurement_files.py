"""
Creates csv files with measurement core and rest data for import into DB
"""

from typing import List
import numpy as np
import pandas as pd
import pathlib
import os
from zipfile import ZipFile
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
OVERWRITE = False

WORKDIR = "../"

FULLPATH_MAIN_DIR = os.getcwd()
FULLPATH_MAIN_DIR = os.path.abspath(os.path.join(FULLPATH_MAIN_DIR ,".."))

META_DATA_DIR = pathlib.Path(WORKDIR, "06. Meta data")
FILE_DATA_DIR = pathlib.Path(WORKDIR, "07. Data Files")

FILE_COLUMNS = "column-meta-info.xlsx"
FILEPATH_COLUMNS = pathlib.Path(META_DATA_DIR, FILE_COLUMNS)

FILEPATH_COPYQUERIES_CORE_MEASUREMENT = pathlib.Path(FILE_DATA_DIR, "copyqueries_measurement_core.txt")
FILEPATH_COPYQUERIES_REST_MEASUREMENT = pathlib.Path(FILE_DATA_DIR, "copyqueries_measurement_rest.txt")

CHUNK_SIZE = 2000000


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

def setDatatypes(df: pd.DataFrame, exclude: List[str] = None) -> pd.DataFrame:
    """Takes in a dataframe and converts datatypes to what is specified in COLUMNS_DICT"""
    if exclude:
        columns = [x for x in df.columns if x not in exclude]
    else:
        columns = list(df.columns)
    
    datatype_dict = {key: COLUMNS_DICT[key] for key in columns}
    for k, v in datatype_dict.items():
        if 'number' in v:
            if 'int' not in str(df[k].dtypes):
                df[k] = df[k].astype(float)
        elif 'text' in v or 'catalogue' in v:
            df[k] = df[k].astype(str)
            if k == 'anportseq':
                df[k] = df[k].str.replace('\.0', '')
        else:
            print(f"no datatype for column {k}")

    # convert text 'nan' to np.nan
    df = df.replace("nan", np.nan)
    
    return df
            
            
"""
GET RELEVANT COLUMNS
"""
df_cols = pd.read_excel(FILEPATH_COLUMNS, sheet_name="column_metaInfo")
df_cols = basic_processing(df_cols)
df_cols_measure = df_cols.query("sample_measurement == 'm'")[["columnname", "datatype", "core_measurement"]]

# get datatype dict
COLUMNS_DICT = dict(zip(df_cols.columnname, df_cols.datatype))

# get sample columns that unique identify sample
df_cols_unique = df_cols.query("unique_identifier == 1")
UNIQUE_IDENTIFIER_COLUMNS = list(df_cols_unique.columnname)

# get measurement information
MEASUREMENT_COLUMNS_DICT = dict(zip(df_cols_measure.columnname, df_cols_measure.datatype))
MEASUREMENT_COLUMNS_LIST = list(df_cols_measure.columnname)
MEASUREMENT_CORE_COLUMNS_LIST = list(df_cols_measure.query("core_measurement == 1")['columnname'])
MEASUREMENT_REST_COLUMNS_LIST = [x for x in MEASUREMENT_COLUMNS_LIST if x not in MEASUREMENT_CORE_COLUMNS_LIST]


"""
GET ALL DIRECTORIES IN WORKDIR
"""
DIRECTORIES = [{'country': dI, 'path': pathlib.Path(FILE_DATA_DIR,dI)} for dI in os.listdir(FILE_DATA_DIR) if os.path.isdir(os.path.join(FILE_DATA_DIR,dI))]   
DIRECTORIES = [x for x in DIRECTORIES if len(x['country']) == 2]


"""
LOOP OVER ALL DIRECTORIES, OPEN ALL ZIPFILES IN THAT DIRECTORY, PROCESS TO GET MEASUREMENT INFO AND SAVE AS gzip
"""    
def main():
    
    for DIR in DIRECTORIES:
        
        COUNTRY = DIR['country']

        print("------------------------------------------------")
        print(COUNTRY)
        
        # create dir if not exists. If dir already exists and OVERWRITE = False, do nothing for that country
        measurement_dir = pathlib.Path(DIR['path'], 'measurement')
        dirExist = os.path.exists(measurement_dir)
        if not dirExist:
            os.makedirs(measurement_dir)
            print("Directory created!")
        else:
            if not OVERWRITE:
                continue   
            
        # loop over zip files in directory, open them, rename column, select columns, and then save to disk
        for zipfile in list(DIR['path'].rglob("*.ZIP")):

            # skip downloaded ZIP file
            if 'contaminants' in zipfile.name:
                continue
            
            if 'pesticides' in zipfile.name:
                continue
            
            if 'veterinary' in zipfile.name:
                continue   

            # in case AMR data is saved in the same folder, skip that for now (you need another script to process AMR data)
            if 'AMR' in zipfile.name:
                continue

            print("\t" + zipfile.name)

            # check if there already is a measurement file, if so and OVERWRITE is not True, then continue to next iteration
            zipfile_noExtension = zipfile.name.lower().replace('.zip', '')
            SAVE_PATH = pathlib.Path(measurement_dir, f"measurement_{zipfile_noExtension}.csv")
            
            fileExist = os.path.exists(SAVE_PATH)
            if fileExist:
                if not OVERWRITE:
                    print(f"Skipped {zipfile.name}. measurement file already exists")
                    continue

            # get sample info from DB
            zipname = zipfile.name.lower()
            with PostgresDatabase(DB_NAME, DB_USER, DB_PASSWORD, HOST) as db:
                dfdb = db.querydf(f"SELECT * FROM efsa.vw_sample_core_termcode WHERE file = '{zipname}';")
                file_id = db.query(f"SELECT id FROM efsa.files WHERE filename = '{zipname}';")[0][0]
            dfdb = dfdb.fillna(value=np.nan)
            dfdb = setDatatypes(dfdb, ['file', 'sample_core_id'])
            dfdb['anportseq'] = dfdb['anportseq'].astype(float)

            # exception for anportsizeunit
            dfdb['anportsizeunit'] = dfdb['anportsizeunit'].fillna('')

            # read data in chunks
            i = 0
            zf = ZipFile(zipfile)
            csv_in_zf = [csv_file.filename for csv_file in zf.infolist() if csv_file.filename.lower().endswith('.csv')]
            for tmp_df in pd.read_csv(zf.open(csv_in_zf[0]), chunksize=CHUNK_SIZE):
                
                # add column with unique nr per row
                tmp_df['measurement_nr'] = tmp_df.index + 1
                
                # clean column names
                tmp_df.columns = cleancolumns(tmp_df.columns)
                
                # rename columns
                tmp_df.columns = [x.replace('_a', '') for x in tmp_df.columns]
                tmp_df = tmp_df.rename(columns={
                    'resevaluation':'evalcode',
                    'paramcode':'paramcodebaseparam',
                    'resultcode':'resid',
                    'anmethcode':'anmethcodebasemeth',
                    'reslegallimittype':'evallimittype',
                    'exprres':'exprrestype',                    
                    'prodcode':'sampmatcodebasebuilding',
                    'labsampcode':'sampid',
                    'expiryd':'sampmatinfoexpiryd',
                    'expirym':'sampmatinfoexpirym',
                    'expiryy':'sampmatinfoexpiryy',
                    'fatperc':'exprrespercfatperc',
                    'labcode':'labid',
                    'localorg':'localorgid',
                    'moistperc':'exprrespercmoistperc',
                    'prodbrandname':'sampmatinfobrandname',
                    'prodcom':'sampmatinfocom',
                    'prodd':'sampmatinfoprodd',
                    'prodm':'sampmatinfoprodm',
                    'prodmanuf':'sampmatinfomanuf',
                    'prodprodmeth':'sampmatcodeprod',
                    'prodtext':'sampmattext',
                    'prody':'sampmatinfoprody',
                    'progcode':'progid',
                    'progsampstrategy':'sampstrategy'})
        
                # select measurement columns + identifier columns for unique sample
                columns_in_tmp_df = [x for x in tmp_df.columns if x in MEASUREMENT_COLUMNS_LIST + UNIQUE_IDENTIFIER_COLUMNS + ["measurement_nr"]]
                tmp_df = tmp_df[columns_in_tmp_df]
                
                # all data to lower and trim and clean columnsnames
                tmp_df = basic_processing(tmp_df)

                # replace N_A with np.nan
                tmp_df = tmp_df.replace("n_a", np.nan) 
                tmp_df = tmp_df.replace("nan", np.nan) 
       
                # replace 0 with np.nan
                tmp_df['analysisy'] = tmp_df['analysisy'].replace(0, np.nan)
                tmp_df['analysism'] = tmp_df['analysism'].replace(0, np.nan)
                tmp_df['analysisd'] = tmp_df['analysisd'].replace(0, np.nan)
        
                # check if all columns are present, if not then create them with empty values
                core_columns_for_tmp_df = UNIQUE_IDENTIFIER_COLUMNS + MEASUREMENT_CORE_COLUMNS_LIST
                core_columns_for_tmp_df.sort()
                for col in core_columns_for_tmp_df:
                    if col not in list(tmp_df.columns):
                        tmp_df[col] = np.nan
                        
                # convert columns to correct datatype: convert number columns to float if they are not already integer.
                tmp_df = setDatatypes(tmp_df, ['measurement_nr'])
                tmp_df['anportseq'] = tmp_df['anportseq'].astype(float)

                # exception for anportsizeunit
                tmp_df['anportsizeunit'] = tmp_df['anportsizeunit'].fillna('')

                # ADD FILETYPE
                tmp_df['file_id'] = file_id
                tmp_df['file'] = zipname

                # add sample_core_id from database
                nr_rows_before = tmp_df.shape[0]
                tmp_df = tmp_df.merge(dfdb, on=UNIQUE_IDENTIFIER_COLUMNS + ['file'], how='inner')
                nr_rows_after = tmp_df.shape[0]

                if nr_rows_before - nr_rows_after != 0:
                    print(f"Wrong merge for {zipname}")
                    print(f"   rows before {nr_rows_before} rows after {nr_rows_after}")
                    print(f"number of rows in dfdb {len(dfdb)}")
                    # analyze why merge failed
                    for col in UNIQUE_IDENTIFIER_COLUMNS:
                        list_values = list(set(tmp_df[col]) - set(dfdb[col]))
                        if len(list_values) > 0:
                            print(f"column {col} has problems")
                            print(list_values)
                    print("ERROR: Aborted!")
                    return
                
                # --------------------- SAVE MEASUREMENT CORE COLUMNS
                FILENAME_SAVE = f"measurement_{zipfile_noExtension}_core_{i}.csv"
                SAVE_PATH_CORE = pathlib.Path(measurement_dir, FILENAME_SAVE)
                tmp_df[['sample_core_id'] + MEASUREMENT_CORE_COLUMNS_LIST + ['file_id', 'measurement_nr']].to_csv(SAVE_PATH_CORE, index=False)
                
                # write copy query to txt file_id
                full_measurement_dir_path = pathlib.Path(FULLPATH_MAIN_DIR, str(measurement_dir)[3:])
                full_measurement_file_path = pathlib.Path(full_measurement_dir_path, FILENAME_SAVE)  

                copy_query = f"\copy efsa.tmp_measurement_core({','.join(tmp_df[['sample_core_id'] + MEASUREMENT_CORE_COLUMNS_LIST + ['file_id', 'measurement_nr']].columns)}) from '{full_measurement_file_path}' (header true, delimiter ',', format csv, encoding 'UTF-8');"
                with open(FILEPATH_COPYQUERIES_CORE_MEASUREMENT, 'a') as f:
                    f.write(copy_query + "\n")
                
                # --------------------- PROCESS AND SAVE MEASUREMENT REST COLUMNS
                # get rest columns
                rest_columns_for_tmp_df = [x for x in MEASUREMENT_REST_COLUMNS_LIST if x in tmp_df.columns]
                tmp_df = tmp_df[['sample_core_id', 'file_id', 'measurement_nr'] + rest_columns_for_tmp_df]
                
                # reshape from wide to long
                tmp_df = tmp_df.melt(id_vars=['sample_core_id', 'file_id', 'measurement_nr'])
    
                # remove rows with no value
                tmp_df = tmp_df.dropna(subset=['value'])
                
                # order columns just to make sure
                tmp_df = tmp_df[['sample_core_id', 'file_id', 'measurement_nr'] + ['variable', 'value']]
                
                # rename variable to columnname
                tmp_df = tmp_df.rename(columns={'variable': 'columnname'})
                
                # save to csv
                FILENAME_REST_SAVE = f"measurement_{zipfile_noExtension}_rest_{i}.csv"
                SAVE_PATH_REST = pathlib.Path(measurement_dir, FILENAME_REST_SAVE)
                tmp_df.to_csv(SAVE_PATH_REST, index=False)
    
                # write copy query to txt file_id
                full_measurement_rest_file_path = pathlib.Path(full_measurement_dir_path, FILENAME_REST_SAVE)  
                copy_query = f"\copy efsa.measurement_rest({','.join(tmp_df.columns)}) from '{full_measurement_rest_file_path}' (header true, delimiter ',', format csv, encoding 'UTF-8');"
                with open(FILEPATH_COPYQUERIES_REST_MEASUREMENT, 'a') as f:
                    f.write(copy_query + "\n")
                    
                # remove tmp_df
                del tmp_df
                i = i + 1
        
if __name__ == "__main__":
    main()