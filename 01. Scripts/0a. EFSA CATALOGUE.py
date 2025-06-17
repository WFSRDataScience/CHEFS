
import pandas as pd
import pathlib
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
OVERWRITE = True

WORKDIR = "../"
CATALOGUE_DATA_DIR = pathlib.Path(WORKDIR, "08. EFSA Catalogues/version v9/DCF_catalogues")


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

def create_sql_query(df: pd.DataFrame, tbl_name) -> str:
   
    query_start = f"CREATE TABLE IF NOT EXISTS ontologies_efsa.{tbl_name} ( id SERIAL PRIMARY KEY, "
    query_end = ");"
    query_middle = ""

    for c in df.columns:
        
        dtype = str(df[c].dtype)
        
        if c in ['lastupdate', 'validfrom', 'validto']:
            datatype = 'DATE'
        elif dtype == 'object':
            datatype = 'VARCHAR'
        elif 'float' in dtype:
            datatype = 'NUMERIC'
        elif 'int' in dtype:
            datatype = 'INTEGER'
        else:
            datatype = 'VARCHAR'
            
        c_query = f"{c} {datatype}"
        
        if c == 'termcode':
            c_query = c_query + " UNIQUE,"
        else:
            c_query = c_query + ","
            
        query_middle = query_middle + c_query

    # remove last comma
    query_middle = query_middle[:-1]

    query = query_start + query_middle + query_end    
    
    return query    


"""
GET ALL EXCEL FILES
"""
excel_list = list(CATALOGUE_DATA_DIR.rglob("*.xlsx"))


"""
FOR EACH EXCEL FILE, CREATE TABLE IN DATABASE IF NOT ALREADY EXISTS AND THEN INSERT VALUES
"""
# get all tables from DB
with PostgresDatabase(DB_NAME, DB_USER, DB_PASSWORD, HOST) as db:
    tables_in_db = db.query("SELECT table_name FROM information_schema.tables WHERE table_schema = 'ontologies_efsa' AND table_type = 'BASE TABLE';")
tables_in_db = [x[0] for x in tables_in_db]


for e in excel_list:
    
    # get excelfile name and table name
    excelname = e.name.lower()
    tbl_name = excelname.replace('.xlsx', '')
    
    # skip if table already exists in db
    if tbl_name in tables_in_db:
        print(f"{tbl_name} already exists. Skipping...")
        continue

    # try to read the data, create a table definition, create the table in DB, and import data
    try:
        df = pd.read_excel(e, sheet_name="term")
      
        df = basic_processing(df)

        if 'termcode' not in df.columns:
          print("column termcode not found")
          continue
        
        if 'termextendedname' not in df.columns:
          print("column termextendedname not found")    
          continue

        if 'lastupdate' in df.columns:
            df['lastupdate'] = pd.to_datetime(df['lastupdate'])
        if 'validfrom' in df.columns:
            df['validfrom'] = pd.to_datetime(df['validfrom'])
        if 'validto' in df.columns:
            df['validto'] = pd.to_datetime(df['validto'])

        query = create_sql_query(df, tbl_name)

        # create table in DB
        with PostgresDatabase(DB_NAME, DB_USER, DB_PASSWORD, HOST) as db:
            db.execute(query)

        # populate table
        db_columnnames = ', '.join(["{}".format(value) for value in df.columns])
        with PostgresDatabase(DB_NAME, DB_USER, DB_PASSWORD, HOST) as db:
            for i, row in df.iterrows():
                insert_values = dict(row[df.columns])
                insert_values = {key : value if pd.notnull(value) else None for key, value in insert_values.items()}              
                insert_query = f"INSERT INTO ontologies_efsa.{tbl_name}(" + db_columnnames + ") VALUES (" + "%s," * (len(insert_values)-1) + "%s)"
                db.execute(insert_query, list(insert_values.values()))

        print(f"{excelname} \t\t\t inserted into db: {len(df)}")

    except:
        print(f"{e.name} \t\t FAILED reading data")
