-- Schema and supporting tables required by the partitioning framework.
-- Run this once before loading the functions.

CREATE SCHEMA IF NOT EXISTS dba;

CREATE TABLE IF NOT EXISTS dba.partition_configuration (
    schema_name   text NOT NULL,
    table_name    text NOT NULL,
    configuration json NOT NULL DEFAULT '{}',
    CONSTRAINT partition_configuration_pk PRIMARY KEY (schema_name, table_name)
);

CREATE TABLE IF NOT EXISTS dba.detached_partitions (
    schema            text,
    parent_relname    text,
    partition_relname text,
    range             text[],
    detached_date     date,
    CONSTRAINT detached_partitions_pk PRIMARY KEY (schema, parent_relname, partition_relname)
);
