# adyen-postgres-partitioning
These functions are designed to create and maintain partitions in PostgreSQL with a minimal impact on the applicaton. The priority is to not impact the application. When multiple options are available the weakest lock possible is being used, when a heavy lock is required we use a timeout to prevent long lasting locks. 

Every function in this project starts with a detailed comment on what function does and how to use it. 

# Features
The functions in this project can

- Partition an existing table. The origional table will not partitioned itself, but becomes the first partition
- Add indexes to a partitioned table and all children
- Add foreign keys to a partitioned table and all children
- Add date constraints to a table partitioned on an integer column
- Count the number of available, unused partitions
- Get the details for the last partition
- Add new partitions to a partitioned table. The new partitions will have the same properties as the latest available partition
- Detach partitions from a partitioned table
- Drop detached partitions

Besides all the functions the project also contains the script `partition_maintenance.sql`. This scripts requires two tables being created
- dba.partition_configuration
- dba.detached_partitions

The scripts performs the following tasks based on the configuration in the table `dba.partition_configuration`. 
- Add new partitions
- Add date constraints
- Detach partitions
- Drop detached partitions after a cool-down period

See the documentation within sql/tables/tables.sql for the configuration details. 

# Installation
All function will be installed in the DBA schema. If you don't have this schema yet, create it by running sql/schema/schema.sql.

You can add the individual functions directly on the database from `psql` with the `\i` command. Use `psql` to login on your database and run
```sql
\i <full path to the function>.<filename>.sql
```
N.B. Some functions use other functions and the script `partition_maintenance.sql` requires a set of tables to be created. 

## Install all functions
To create all the functions and the tables required to configure maintenance apply the following scripts in order from the root directory of the project
sql/schema/schema.sql
sql/tables/tables.sql
sql/functions/create_all_functions.sql

## Test functions
To test all functions run the following scripts from the project root folder
test/tables.sql
test/run_functions.sql
test/configuration.sql
test/cleanup.sql
