import pandas as pd
from sqlalchemy import create_engine
import psycopg2 as pg
import sys
import os
import datetime

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from config import db_key

def create_tables(df_list, table_name_list):
    """ Connects to a Postgres database using sqlalchemy library, creating
    SQL tables for each DataFrame. Commits the changes and closes the connection
    after the tables are created.

    Parameters:
     - df_list : List of DataFrames for which an SQL table is created.
     - table_name_list : List of table names, following the order of DataFrames.
    """
    print(datetime.datetime.now(), "Creating tables . . .")
    connection_string = f"postgresql://postgres:{db_key}@localhost:5432/travel_destination"
    engine = create_engine(connection_string)

    for df, table_name in zip(df_list, table_name_list):
        print(datetime.datetime.now(), f"Creating table: {table_name}")
        create_table(df, table_name, engine)
        print(datetime.datetime.now(), f"Table created: {table_name}")

    engine.dispose()

def create_table(df, table_name, engine):
    """ Creates a database table based on the parameters.
    
    Parameters:
     - df : DataFrame
     - table_name : Name for the table.
     - engine : Engine that established the database connection.
    """
    df.to_sql(table_name, engine, if_exists='append', index=False)

def set_primary_keys(table_names_list, primary_key_list):
    """ Sets primary keys for the tables in Postgres database.

    Notice: Configure the lists in the same order (table_name, primary_key pairs).

    Parameters:
     - table_names_list : List of the names of the tables for which primary
                          key is to be set.
     - primary_key_list : List of column names which will be the primary keys.
    
    """
    print(datetime.datetime.now(), "Setting primary keys . . .")
    conn = pg.connect(host="localhost", dbname="travel_destination", user="postgres", \
                            password=db_key, port=5432)
    cur = conn.cursor()

    for table_name, primary_key in zip(table_names_list, primary_key_list):
        print(datetime.datetime.now(), f"Setting key for: {table_name}")
        # Construct the SQL statement with interpolated values
        sql = f"ALTER TABLE {table_name} ADD PRIMARY KEY ({primary_key});"

        # Execute the SQL statement
        cur.execute(sql)
        print(datetime.datetime.now(), f"Key set for: {table_name}")


    conn.commit()
    cur.close()
    conn.close()
    