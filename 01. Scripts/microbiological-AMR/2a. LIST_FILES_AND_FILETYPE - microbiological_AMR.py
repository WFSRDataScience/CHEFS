"""
Extract zip files
convert all to csv files
create a meta file with information on all csv files
"""

import os
import xlrd
import csv
import pandas as pd
import pathlib
from csv import writer
from zipfile import ZipFile
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

FILE_DATA_DIR = pathlib.Path(WORKDIR, "07. Data Files")

FILEPATH_LISTFILES = pathlib.Path(FILE_DATA_DIR, "listFiles-AMR.csv")

FILENAME_FILES_SAVE =  "copyqueries_files.txt"
FILEPATH_COPYQUERIES_FILES = pathlib.Path(FILE_DATA_DIR, FILENAME_FILES_SAVE)


"""
HELPER FUNCTIONS
"""
def getFileType(string: str) -> str:
    if "AMR" in string:
        return 'amr'
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


def csv_from_excel(p: pathlib.Path) -> pathlib.Path:
    print(p)
    wb = xlrd.open_workbook(p)
    sh = wb.sheet_by_index(0)
    csvPath = pathlib.Path(p.parent, p.stem + ".csv")
    your_csv_file = open(csvPath, 'w', encoding='utf8')
    wr = csv.writer(your_csv_file, quoting=csv.QUOTE_ALL)

    for rownum in range(sh.nrows):
        wr.writerow(sh.row_values(rownum))

    your_csv_file.close()
    
    #df = pd.read_excel(p)
    #df.to_csv(csvPath)
    return csvPath
    
    
def main():  
    
    """
    LIST FILETYPE PER FILE
    - loop over all AMR zipfiles, extract the content (either csv or xlsx), and convert that to a 
      proper working csv file with a correct name (delete the original file)
    """
    # get all zip files with 'amr' in the filename
    for zipfile in list(FILE_DATA_DIR.rglob("*AMR_PUB*.zip")):        
        
        print(zipfile)
        
        # determine final csv name and skip procedure if that file already exists
        finalCSVpath = pathlib.Path(zipfile.parent, zipfile.name.replace('.zip', '.csv'))
        if os.path.exists(finalCSVpath):
            continue
            
        filename = str(zipfile.name)
        countrycode = filename[:2]
        year = int(''.join([str(i) for i in filename if i.isdigit()]))
        
        with ZipFile(zipfile, 'r') as zObject: 

            # extract zipfile into DIR
            DIR = zipfile.parent
            zObject.extractall(path=DIR)            
            
            # rename extracted file
            for zip_info in zObject.infolist():
                filename = zip_info.filename
                if filename.split('.')[1] == 'zip':
                    continue
                fileExtension = filename.split('.')[1]
                oldName = pathlib.Path(DIR, filename)
                newName = pathlib.Path(DIR, f"{countrycode}_AMR_PUB_{year}.{fileExtension}")
                
                # if new file not yet exists
                if not os.path.exists(newName):
                    os.rename(oldName, newName)
        
        # convert xlsx to csv because xlsx cannot be opened in pandas due to an error I do not understand
        # then read in data
        if fileExtension == 'xlsx':
            csvPath = csv_from_excel(newName)            
            df = pd.read_csv(csvPath)
        else:
            csvPath = newName
            df = pd.read_csv(csvPath, delimiter='\t')
            df.to_csv(csvPath, index=False)

        # determine number of columns
        nr_cols = df.shape[1]
        nr_rows = df.shape[0]
        
        # write to excel
        with open(FILEPATH_LISTFILES, 'a', newline='') as f:
            writer_object = writer(f, delimiter=',')
            writer_object.writerow([newName.name, "amr", year, countrycode, nr_cols, nr_rows])
    
    
    # construct save Path
    full_sample_dir_path = pathlib.Path(FULLPATH_MAIN_DIR, "07. Data Files")
    full_sample_file_path = pathlib.Path(full_sample_dir_path, "listFiles-AMR.csv") 

    # create copy query
    copy_query = f"\copy efsa.tmp_files(filename, filetype, year, country_code, nr_cols, nr_rows) from '{full_sample_file_path}' (header false, delimiter ',', format csv, encoding 'UTF-8');"
    with open(FILEPATH_COPYQUERIES_FILES, 'a') as f:
        f.write(copy_query + "\n")        

if __name__ == "__main__":
    main()
