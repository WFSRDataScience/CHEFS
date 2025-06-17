import psycopg2
import numpy as np
import pandas as pd

# from psycopg2.extensions import register_adapter, AsIs
psycopg2.extensions.register_adapter(np.int64, psycopg2._psycopg.AsIs)

class PostgresDatabase:
    def __init__(self, db_name: str, db_user: str, db_password: str, host: str):
        #print(f"connection to {host}")
        self.connection = psycopg2.connect(user = db_user,
                                           password = db_password,
                                           host = host,
                                           port = "5432",
                                           database = db_name)
        self.cursor = self.connection.cursor()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def commit(self):
        self.connection.commit()
    
    def rollback(self):
        self.connection.rollback()
            
    def close(self, commit=True):
        if commit:
            self.commit()
        self.cursor.close()
        self.connection.close()

    def execute(self, sql: str, params=None):
        self.cursor.execute(sql, params or ())

    def fetchall(self):
        return self.cursor.fetchall()

    def fetchone(self):
        return self.cursor.fetchone()

    def query(self, sql: str, params=None):
        self.cursor.execute(sql, params or ())
        return self.fetchall()

    def querydf(self, sql: str) -> pd.DataFrame:
        return pd.read_sql_query(sql, self.connection)