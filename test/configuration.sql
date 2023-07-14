insert into dba.partition_configuration values ('public', 'test_partition', json_build_object(
        'auto-maintenance', true, 
	'date_constraint', json_build_object('marker', 'trip_date', 'constraint_column', 'trip_date'),
        'nr', 4
    )
);

insert into dba.partition_configuration values ('public', 'test_partition_datetime', json_build_object(
        'auto-maintenance', true,
        'nr', 6,
        'detach', '13 days',
        'drop_detached', '4 days'
    )
);

INSERT INTO test_partition(id, trip_date) VALUES (7000000, '2001-01-01');
INSERT INTO test_partition(id, trip_date) VALUES (7999999, '2001-12-31');

-- Run partition_maintenance script
\i sql/functions/partition_maintenance.sql

-- Update the dba.detach_partitions table to simulate detached partitions over a period of time
UPDATE dba.detached_partitions SET detached_date = range[1]::DATE + INTERVAL '2 weeks';

-- Run partition_maintenance script again to drop the detached partitions
\i sql/functions/partition_maintenance.sql
