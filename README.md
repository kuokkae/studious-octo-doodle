# studious-octo-doodle
data migration db check

there is a need to migrate our db from sql server to postgres

we need to be able to check if the data moved over from one system to another is performed properly

this solution combines the use of python and microsoft excel

Python is used to extract and perform some transformation, microsoft excel is used to 

intention of this tool is to perform a snapshot check on two area:
1. schema, table and column check to see if they're identical along with the data type
2. comparison between the values in each table to see if they identical.
