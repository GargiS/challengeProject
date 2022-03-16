Requirement:

1. Staging files onto the platform
2. Validating the file integrity of all feeds
3. Validating and quality checking the data – flagging any rows that do not conform
4. Required data transformations and curating
5. Type 2 history and Change Data Capture (CDC)
6. Storage as PARQUET


How to deploy :
1. Run deploy.sh
2. zip getting created in target/ folder

How to runScript:
1. runScript

Algorithm:
1. Read input Files and store
2. File Validatior holds static methods to validate below
   a. record count does not match - record code 1
   b. file name does not match - return code 2
   c. primary key test fail - return code 3
   d. Column mismatch ( Invalid file – missing columns/addition columns) - return code 4
3. Record Validation is done by adding Dirty Flag :
   a. Invalid Fields for below reasons :
   b. DataType check
   c. mandatory check
3. For Type2 history changes, there are two approaches
   a.  Preferred appraoch , is to save as deltaTable(saves in parquet format) and for scd use delta merge option
    ( with the join expression having colums involved in CDC logic and partition by columns)

Enhancement