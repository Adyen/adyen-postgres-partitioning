# adyen-postgres-partitioning
These functions are designed to create and maintain partitions in PostgreSQL with a minimal impact on the applicaton. The priority is to not impact the application. When possible, the weakest lock possible is being used, when a heavy lock is required we use a timeout to prevent long lasting locks. 

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

# Installation
You can add the individual functions directly on the database from `psql` with the `\i` command. Use `psql` to login on your database and run
```sql
\i <full path to the function>.<filename>.sql
```
N.B. Some functions use other functions. 
