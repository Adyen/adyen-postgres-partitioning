-- Convert tables into partioned tables
SELECT dba.partition_table('public','test_partition','id','1','7000000','1000000','native');
SELECT dba.partition_table('public','test_partition_date','trip_date','1970-01-01','2023-01-01','1 month','native', TRUE);
SELECT dba.partition_table('public','test_partition_datetime','trip_date','1970-01-01','2023-01-01','1 week','native');
SELECT dba.partition_table('public','test_partition_inh','id','1','999999','1000','inheritance');

-- Create additional partitions for partitioned tables
SELECT dba.partition_add_up_to_nr_of_free_partitions('public','test_partition', 3);
SELECT dba.partition_add_up_to_nr_of_free_partitions('public','test_partition_date', 3);
SELECT dba.partition_add_up_to_nr_of_free_partitions('public','test_partition_datetime', 3);

-- Create different type of indexes to partitioned table
SELECT dba.partition_add_concurrent_index_on_partitioned_table('public','test_partition', ARRAY['vendor_id']) \gexec
SELECT dba.partition_add_concurrent_index_on_partitioned_table('public','test_partition_date', ARRAY['lower(extra)', 'rate_code_id desc nulls first'], 'btree') \gexec
SELECT dba.partition_add_concurrent_index_on_partitioned_table('public','test_partition_datetime', ARRAY['id'], 'btree', true) \gexec

-- Add foreign key to a partitioned table
SELECT dba.partition_add_foreign_key_on_partitioned_table('public','test_partition_datetime', 'test_partition_test_date_time_test_partition_fk', 'test_partition', ARRAY['id'], ARRAY['id']) \gexec

-- Create date constraints on a partition
-- First insert some values
INSERT INTO test_partition(id, trip_date) VALUES (8000000, '2000-01-01');
INSERT INTO test_partition(id, trip_date) VALUES (8999999, '2000-12-31');
-- Add the constraints
SELECT dba.partition_add_constraints('public', 'test_partition', 'trip_date', 'trip_date');

-- Detach a partition
SELECT dba.partition_detach_partition('public','test_partition_datetime', 'test_partition_datetime_20230102_20230109');

-- Drop detached table
SELECT dba.partition_drop_detached_partition('public','test_partition_datetime', 'test_partition_datetime_20230102_20230109');

