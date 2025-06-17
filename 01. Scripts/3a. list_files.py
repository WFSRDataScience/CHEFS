"""
Counts all rows and columns for all CSV files in zip files
"""

import os
import pandas as pd
import pathlib
from csv import writer
import re
from zipfile import ZipFile
from utils import cleancolumns, df_tolower, df_trim


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

FILE_DATA_DIR = pathlib.Path(WORKDIR, "07. Data Files")
FILEPATH_LISTFILES = pathlib.Path(FILE_DATA_DIR, "listFiles.csv")

FILENAME_FILES_SAVE =  "copyqueries_files.txt"
FILEPATH_COPYQUERIES_FILES = pathlib.Path(FILE_DATA_DIR, FILENAME_FILES_SAVE)


"""
HELPER FUNCTIONS
"""
def getFileType(string: str) -> str:
    if "MOPER" in string:
        return 'pesticides'
    elif "OCC" in string:
        return 'chemical'
    elif "VMPR" in string:
        return 'vmpr'
    else:
        return ''
    
def basic_processing(df: pd.DataFrame) -> pd.DataFrame:

    # all data to lower and trim
    df = df_tolower(df)
    df = df_trim(df)
     
    # clean columnsnames (lowercase, remove special characters etc)
    df.columns = cleancolumns(df.columns)
    
    # return df
    return df


def main():  
    """
    LIST FILETYPE PER FILE
    """
    for zipfile in list(FILE_DATA_DIR.rglob("*.ZIP")):
        
        # extract filetype from filename
        filename = str(zipfile.name)
        filetype = getFileType(filename)
        
        # skip downloaded ZIP file
        if 'contaminants' in zipfile.name:
            continue
        
        if 'pesticides' in zipfile.name:
            continue
        
        if 'veterinary' in zipfile.name:
            continue

        print(filename)

        # extract year and country-code from filename
        filename_endpart = filename.replace(".ZIP", "").replace(".zip", "")[-8:]
        try:
            year = int(''.join([str(i) for i in filename_endpart if i.isdigit()]))
            country_code = "".join(re.split("[^a-zA-Z]*", filename_endpart))
        except:
            filename_endpart = filename.replace(".ZIP", "").replace(".zip", "")[-13:]
            year = int(''.join([str(i) for i in filename_endpart if i.isdigit()]))
            country_code = "".join(re.split("[^a-zA-Z]*", filename_endpart[-7:]))        
       
        # read file to determine number of columns
        zf = ZipFile(zipfile)
        csv_in_zf = [csv_file.filename for csv_file in zf.infolist() if csv_file.filename.lower().endswith('.csv')]
        df = pd.read_csv(zf.open(csv_in_zf[0]))
        nr_cols = df.shape[1]
        nr_rows = df.shape[0]
        
        # write to excel
        with open(FILEPATH_LISTFILES, 'a', newline='') as f:
            writer_object = writer(f, delimiter=',')
            writer_object.writerow([filename, filetype, year, country_code, nr_cols, nr_rows])

    # -- sql query
    # construct save Path
    full_sample_dir_path = pathlib.Path(FULLPATH_MAIN_DIR, "07. Data Files")
    full_sample_file_path = pathlib.Path(full_sample_dir_path, "listFiles.csv")            

    copy_query = f"\copy efsa.tmp_files(filename, filetype, year, country_code, nr_cols, nr_rows) from '{full_sample_file_path}' (header false, delimiter ',', format csv, encoding 'UTF-8');"
    with open(FILEPATH_COPYQUERIES_FILES, 'a') as f:
        f.write(copy_query + "\n")
    

if __name__ == "__main__":
    main()