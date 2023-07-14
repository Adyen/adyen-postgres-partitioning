/*
This table contains the configuration for partitioned tables. 
It is possible to configure the following options
 - Enable auto maintenance for this table
 - The number of free, available partitions
 - Add constraints to partitions
 - Detach partitions
 - Drop detached partitions

Automatic maintenance
---------------------
If you want the maintenance script to do maintenance on the partitioned tables, you have to 
specify the schema, table name and set the 'auto-maintenance' flag explicitly to true.

insert into dba.partition_configuration values ('public', 'test_partition', json_build_object(
        'auto-maintenance', true 
    )
);

Without any further configuration the maintenance script will now create new partitions for your 
partitioned table until there are at least 
 - three free, available partitions for a table partitioned on any integer type
 - three partitions with a starting date after today for a table partitioned on a date/timestamp type

Number of free, available partitions
------------------------------------
If you want more than three unused, available partitions for a partitioned table you can configure
this number with the 'nr' option. This might be useful when using daily partitions and you want to 
have at least a week of partitions available to be on the safe side at all times.

insert into dba.partition_configuration values ('public', 'test_partition', json_build_object(
        'auto-maintenance', true,
        'nr', 6
    )
);

In this example the script will create partitions for the partitioned table test_partition until there
are six unused, available partitions. 

Add constraints
---------------
The script can also add date constraints on a table partitioned on an integer column. This is useful
when the table is partitioned on a integer type, but also accessed based on a date type. 

A check constraint with a minimal date is added as soon as a partition contains at least one row. 
A check constraint with a maximum date is added as soon as a row matching the upper boundary of 
the partition has been added. 

N.B. This method only works with increasing, non changing values. 

To configure this constraints use a json object with a marker and column name on which the constraint
is required. 

insert into dba.partition_configuration values ('public', 'test_partition', json_build_object(
        'auto-maintenance', true,
        'date_constraint', json_build_object('marker', 'trip_date', 'constraint_column', 'trip_date')
    )
);

In this example the following constraints will be added to the children of table 'test_partition'
 - <child_name>_trip_date_min
 - <child_name>_trip_date_max

The min constraint will be based on min(<constraint_column>), the max constraint on the 
max(<constraint_column). 

Detach and drop partitions 
--------------------------
The script can also detach and drop detached partitions. Detaching can be done automatically on 
tables holding PII data for example. After detaching the child is not longer accessible from the parent
table. 

To specify the time for detaching or dropping any valid Postgres interval works. A table will
be detached when the upper partition boundary is smaller the current date minus the detach period. 

For dropping detached partitions the scrips checks the detached_date in the table dba.detached_partitions. 
The script will not drop detached partitions which detach date is smaller than 4 days ago as a 
safety measure.

Example for detaching and dropping a partitions. 
insert into dba.partition_configuration values ('public', 'test_partition_datetime', json_build_object(
        'auto-maintenance', true,
        'detach', '5 years',
        'drop_detached', '4 days'
    )
);

*/
CREATE TABLE dba.partition_configuration(
    schema_name text not null,
    table_name text not null,
    configuration json not null
);

ALTER TABLE dba.partition_configuration ADD CONSTRAINT partition_configuration_pk PRIMARY KEY (table_name, schema_name);

COMMENT ON COLUMN dba.partition_configuration.schema_name IS
'Schema name';
COMMENT ON COLUMN dba.partition_configuration.table_name IS
'Table name of the parent table';
COMMENT ON COLUMN dba.partition_configuration.configuration IS
'Configuration for partitioned table';

/*
This table contains a list of detached partitions. All information for reattaching the child table is available. The 
function checks this table to select tables eligible for dropping.

Detached tables still exists in the database, but are no longer accessible from the parent table.
*/
CREATE TABLE dba.detached_partitions(
    schema text,
    parent_relname text,
    partition_relname text,
    range text[],
    detached_date DATE
);

ALTER TABLE dba.detached_partitions ADD CONSTRAINT detached_partitions_pk PRIMARY KEY (schema, parent_relname, partition_relname);

COMMENT ON COLUMN dba.detached_partitions.schema IS
'Schema name';
COMMENT ON COLUMN dba.detached_partitions.parent_relname IS
'Table name of the parent table from which the partition is detached';
COMMENT ON COLUMN dba.detached_partitions.partition_relname IS
'Table name of the detached partition';
COMMENT ON COLUMN dba.detached_partitions.range IS
'The partition boundaries';
COMMENT ON COLUMN dba.detached_partitions.detached_date IS
'The date at which the partitions has been detached';
