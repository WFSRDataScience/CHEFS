import pandas as pd

############################################################ PROCESS DATAFRAME
def df_trim( i_df ):

    # select all columns that are object. Then run .strip() on all values
    for col in i_df.select_dtypes(include ='object'):
        i_df[col] = [str(x).strip() if pd.notnull(x) else x for x in i_df[col]]      

    return i_df
    
def df_tolower( i_df ):

    # select all columns that are object. Then run .lower() on all values
    for col in i_df.select_dtypes(include ='object'):
        i_df[col] = [str(x).lower() if pd.notnull(x) else x for x in i_df[col]]
    
    return i_df    

def cleancolumns( cols ):
    """
    all column names to lowercase. whitespaces are replaced with underscores. All non-word-number-underscore characters are removed
    use in this way:    df.columns = cleancolumns( df.columns )
    """
    pattern = r'[^\w\s]'
    
    #return cols.str.strip().str.replace('\s', '_').str.replace(r'\W' , '').str.lower()
    cols = cols.str.strip().str.lower()
    return cols.str.replace(pattern, '', regex=True).str.replace(' ', '_')