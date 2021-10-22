# SCRIPT TO EXTRACT SCHEMA

# # required to connect to odbc
import os
import pyodbc
import pandas as pd
from datetime import datetime

# DECLARE VARIABLES
v = 0
path =r'folder name'
schema = "schema name"

#  function to change workign directory
def change_folder():
    print("original file path", os.getcwd())
    try:
        os.chdir(path)
        print("Current working directory: {0}".format(os.getcwd()))
    except FileNotFoundError:
        print("Directory: {0} does not exist".format(path))
    except NotADirectoryError:
        print("{0} is not a directory".format(path))
    except PermissionError:
        print("You do not have permissions to change to {0}".format(path))

# function to QUERY FROM SQL SERVER
def query_sql():
    key = '''DSN=;UID=;PWD='''
    conn = pyodbc.connect(key)
    cursor = conn.cursor()

    sql = """
            SELECT
                [TABLE_CATALOG] as 'schema', [TABLE_NAME] as 'table', [COLUMN_NAME] as 'column', [DATA_TYPE] as 'dtype'
            FROM
                {}.INFORMATION_SCHEMA.COLUMNS c
            ORDER BY [TABLE_NAME],[COLUMN_NAME] asc;
            """.format(schema)
    print("querying db1..")
    # read into dataframe
    query = pd.read_sql_query(sql, conn)
    df = pd.DataFrame(query)
    print(df)
    #  # access a particular element in the dataframe
    # print("col name ", df.iat[v,2])
    return(df)

# function to QUERY FROM POSTGRES MYTUKAR DATAMART
def query_postgres():
    # connect to server, query and close the connection
    key = '''DSN=;UID=;PWD='''
    conn = pyodbc.connect(key)
    cursor = conn.cursor()
    sql = """
        select
           table_schema as "schema", table_name as "table", column_name as "column", data_type as "dtype"
        from
            information_schema."columns" c
        where
            table_schema = '{}'
        order by table_name, column_name asc;
    """.format(schema)
    print("querying postgres...")
    SQL_Query = pd.read_sql_query(sql, conn)
    cursor.close()
    conn.close()
    print("connection closed.")
    # save result to dataframe
    df = pd.DataFrame(SQL_Query)
    print(df)
    #  # access a particular element in the dataframe
    # print("col name ",df.iat[v,2])
    return df

change_folder()
df1 = query_sql()
df2 = query_postgres()
df1.sort_index(inplace=True)
df2.sort_index(inplace=True)

# # # check whole table
# # check identical table
# print(df1.equals(df2))
# print(df1)

# # use pd compare
# df4 = df1.compare(df2)
# df4.to_csv('comparison.csv')

# pkey merge (JOIN TABLES)
#
# use merge method 1
m = df1.merge(df2, left_on = ["schema", "table", "column"], right_on=["schema", "table", "column"],how='outer', suffixes=['','_'], validate='1:1', indicator=True)

# REASSIGNING VALUES TO IMPROVE READBILITY
d={"left_only":"sql only","right_only":"postgres only","both":"available both"}
m['_merge'] = m['_merge'].map(d)

# GROUP BY OPERATIONS
# group any entries with more than 1 row for any pkey
agg_m = m.groupby(['schema', 'table','column'], as_index=False).agg(lambda x: '>>> '.join(map(str, set(x))))
# ADD NEW COLUMN TO CHECK FOR IDENTICAL VALUE
agg_m.loc[agg_m['dtype'] == agg_m['dtype_'], 'identical?']= 'True'
agg_m.loc[agg_m['dtype'] != agg_m['dtype_'], 'identical?']= 'False'
# ADDED TIMESTAMP COLUMN FOR EXTRACTION
agg_m.loc[:,'extraction_timestamp']= datetime.today().strftime('%Y-%m-%d %H:%M:%S')

# m = df1.merge(df2, on='id', how='outer', suffixes=['','_'], indicator=True)
# m = m.loc[m['_merge'] !='both']

# renaming some column
agg_m.rename(columns = {'dtype':'sql', 'dtype_':'postgres', '_merge':'pkey found in'}, inplace = True)
print(agg_m)

################################################ OUTPUT files #########################################

# saving option a - overwrite
agg_m.to_csv(f'schema-comparison-{schema}.csv', sep=',', index=0)

# # saving option B - appending
# filename = f'schema-comparison-{schema}.csv'
# with open(filename, 'a', newline='' ,encoding="utf-8") as f:
#     agg_m.to_csv(f, header=(f.tell()==0), sep=',', index=0)

print(f'Saved as {schema}.csv!')
