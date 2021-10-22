## PURPOSE
# CREATED TO FACILITATE THE MOVE OF DB FROM SQL SERVER TO POSTGRES
## TO DO
# TO EXTRACT
# AND COMPARE TABLES FROM TWO DIFFERENT SET OF DB

# # whats missing
# proper error handling
# cleanup on code

# # required to connect to odbc
import os
import pyodbc
import pandas as pd
from datetime import datetime

## VARIABLES
# date of extraction where applicable
start_date = """'2021-10-01'"""
end_date = """'2021-10-15'"""
# folder path for working directory
path =r''

# list of tables to loop through
tables = ["t_user", "t_sales"]

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
def query_sql(self):
    key = '''DSN=;UID=;PWD='''
    conn = pyodbc.connect(key)
    cursor = conn.cursor()

    sql = """
            SELECT
                id as pkey, *
            FROM
                [DB].[SCHEMA].{}
            where
                created_at as DATE >= {} and
                created_at as DATE < {}
            order by
                id DESC
            """.format(table,start_date, end_date)
    print("querying sql database..")
    # read into dataframe
    query = pd.read_sql_query(sql, conn)
    df = pd.DataFrame(query)
    print(df)
    return(df)

# function to QUERY FROM POSTGRES
def query_postgres(self):
    # connect to server, query and close the connection
    key = '''DSN=;UID=;PWD='''
    conn = pyodbc.connect(key)
    cursor = conn.cursor()
    sql = """
            select
                id as pkey, *
            from
                [DB].[SCHEMA].{}
            where
                created_at  >= {} and
                created_at  < {}
            order by
                id DESC
    """.format(table,start_date, end_date)
    print("querying postgres..")
    SQL_Query = pd.read_sql_query(sql, conn)
    cursor.close()
    conn.close()
    print("connection closed.")

    # save result to dataframe
    df = pd.DataFrame(SQL_Query)
    print(df)
    return df

# RUNNING THE CODE
change_folder()
# LOOP THROUGH A LIST OF TABLES
for table in tables:
    df1 = query_sql(table)
    df2 = query_postgres(table)
    df1.sort_index(inplace=True)
    df2.sort_index(inplace=True)

    # # # TEMPORY EXPORT OUTPUT
    # df1.to_csv('df1.csv')
    # df2.to_csv('df2.csv')

    # UNPIVOT TABLE (FROM WIDE TO LONG)
    df1= df1.melt(id_vars=['pkey'])
    df2= df2.melt(id_vars=['pkey'])

    # RENAMING COLUMN TO IMPROVE READBILITY
    df1.rename({'variable':'column'}, axis=1, inplace=True)
    df2.rename({'variable':'column'}, axis=1, inplace=True)

    # # # TEMPORY EXPORT OUTPUT
    # df1.to_csv('df1.csv')
    # df2.to_csv('df2.csv')

    # # # MERGE METHOD 1 (merge)
    m = df1.merge(df2, on = ['pkey', 'column'], how='outer', suffixes=['','_'], validate='1:1', indicator=True)
    # #clean up value column
    # convert column to string
    m['value'] = m['value'].map(str)
    m['value_'] = m['value_'].map(str)
    # replace line break with spaces to workaround PQ breaking when reading in the data
    m['value'] = m['value'].map(lambda x : x.replace('\r\n', ' '))
    m['value_'] = m['value_'].map(lambda x : x.replace('\r\n', ' '))
    # print(m['value'])

    # REASSIGNING VALUES TO IMPROVE READBILITY
    d={"left_only":"sql only","right_only":"postgres only","both":"available both"}
    m['_merge'] = m['_merge'].map(d)

    # GROUP BY OPERATIONS
    agg_m = m.groupby(['pkey', 'column'], as_index=False).agg(lambda x: '>>> '.join(map(str, set(x)))).reset_index()
    # supposed to clear up all the na values
    agg_m = agg_m.fillna(0)
    # ADD NEW COLUMN TO CHECK FOR IDENTICAL VALUE
    agg_m.loc[agg_m['value'] == agg_m['value_'], 'identical?']= 'True'
    agg_m.loc[agg_m['value'] != agg_m['value_'], 'identical?']= 'False'

    # ADDED TIMESTAMP COLUMN FOR EXTRACTION FOR AUDIT TRAIL
    agg_m.loc[:,'extraction_timestamp']= datetime.today().strftime('%Y-%m-%d %H:%M:%S')

    ####################################### FINAL OUTPUT files #########################################
    # renaming some column
    agg_m.rename({'value':'sql', 'value_':'postgres', '_merge':'pkey found in'}, axis=1, inplace=True)
    # print(agg_m['sql'].head(10))

    # saving option a - overwrite
    agg_m.to_csv(f'table-comparison-{table}.csv', sep=',', index=0)

    # saving option B - appending
    # filename = f'table-comparison-{table}.csv'
    # with open(filename, 'a', newline='' ,encoding="utf-8") as f:
    #     agg_m.to_csv(f, header=(f.tell()==0), sep=',', index=0)

    print(f'Saved as {table}.csv!')

    # # SAVING AS XLSX (NOT RECOMMENDED)
    # # must create the file first in excel before saving as xlsx, python cant create
    #     filename = f'table-comparison-{table}.xlsx'
    #     with pd.ExcelWriter(filename, engine="openpyxl", mode="a") as f:
    #         agg_m.to_excel(f, sheet_name=table)
