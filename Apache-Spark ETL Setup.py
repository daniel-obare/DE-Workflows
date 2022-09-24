#!/usr/bin/env python
# coding: utf-8

# In[ ]:



import pandas as pd
import pyodbc
import os
import re
from sqlalchemy import create_engine


################################################################
print()
print("#################################### EL-PROCESS STARTED ####################################")
print()
print()
################################################################


### Create engines

database_name = 'adventureworksdw2019'
mssqlserver_servername = 'xxx' # Name of the SQL Server here. If you're working on your local machine, it's the name of your machine.

mssqlserver_uri = f"mssql+pyodbc://{os.environ.get('mssqlserver_user')}:{os.environ.get('mssqlserver_pass')}@{mssqlserver_servername}/{database_name}?driver=SQL+Server"
mssqlserver_engine = create_engine(mssqlserver_uri)

postgres_uri = f"postgres+psycopg2://{os.environ.get('postgres_user')}:{os.environ.get('postgres_pass')}@localhost:5432/{database_name}"
postgres_engine = create_engine(postgres_uri)


################################################################
print(f"Engines created for database {database_name}")
print()
print()
################################################################


### Query all tables, including views

mssqlserver_table_query = """

    SELECT
          t.name AS table_name
        , s.name AS schema_name
    FROM sys.tables t
    INNER JOIN sys.schemas s
    ON t.schema_id = s.schema_id

    UNION

    SELECT
          v.name AS table_name
        , s.name AS schema_name
    FROM sys.views v
    INNER JOIN sys.schemas s
    ON v.schema_id = s.schema_id

    ORDER BY schema_name, table_name;

"""

mssqlserver_connection = mssqlserver_engine.connect()

mssqlserver_tables = mssqlserver_connection.execute(mssqlserver_table_query)
mssqlserver_tables = mssqlserver_tables.fetchall()
mssqlserver_tables = dict(mssqlserver_tables)

mssqlserver_schemas = set(mssqlserver_tables.values())

mssqlserver_connection.close()

################################################################
print(f"Tables collected. Found {len(mssqlserver_tables)} tables in {len(mssqlserver_schemas)} schemas.")
print()
print()
################################################################


### Schema creation

postgres_connection = postgres_engine.connect()

for schema in mssqlserver_schemas:
    schema_create = f"""

        DROP SCHEMA IF EXISTS "{schema.lower()}" CASCADE;
        CREATE SCHEMA"{schema.lower()}";

    """

    postgres_connection.execute(schema_create) 
    print(f" - Schema {schema.lower()} created")

postgres_connection.close()


################################################################
print()
print(f"Schemas created.")
print()
print()
################################################################


### Table dump

for table_name, schema_name in mssqlserver_tables.items():
    
    table_no = list(mssqlserver_tables.keys()).index(f"{table_name}") + 1
    ################################################################
    print()
    print(f"##### Dumping table No. {table_no} from {len(mssqlserver_tables)}: {schema_name}.{table_name}...")
    ################################################################
    
    mssqlserver_connection = mssqlserver_engine.connect()
    postgres_connection = postgres_engine.connect()
    
    table_split = [t for t in re.split("([A-Z][^A-Z]*)", table_name) if t]
    table_split = '_'.join(table_split)
    table_split = table_split.lower()
    
    ################################################################
    print(f"    . Converted {table_name} to --> {table_split}")
    ################################################################
    
    full_table = f"""

        SELECT
        *
        FROM {schema_name}.{table_name};

    """
    
    df = pd.read_sql(full_table, mssqlserver_connection)
    df.columns = map(str.lower, df.columns)
    df.to_sql(schema=schema_name.lower(), name=table_split, con=postgres_connection, chunksize=5000, index=False, index_label=False, if_exists='replace')
    
    ################################################################
    print(f"   .. Wrote {schema_name}.{table_split} to database")
    ################################################################
    
    
    postgres_connection.close()
    mssqlserver_connection.close()


mssqlserver_engine.dispose()
postgres_engine.dispose()


print()
print()
print("Engines disposed")
print()
print()
print("#################################### PROCESS FINISHED ####################################")
print()

