-- Utility functions (no dependencies)
\i sql/functions/fmt_readable_number.sql
\i sql/functions/uuid_v7_to_timestamptz.sql
\i sql/functions/uuid_timestamptz_to_v7.sql
\i sql/functions/get_optimized_column_order.sql

-- Table analysis and copy utilities
\i sql/functions/find_matching_index_by_definition.sql
\i sql/functions/generate_create_table_optimized_columns.sql
\i sql/functions/create_optimized_table_copy.sql

-- Lock-safe execution
\i sql/functions/lock_safe_execute.sql
\i sql/functions/calculate_query_date_interval.sql

-- Core partition helpers (used by many other functions)
\i sql/functions/partition_table_is_partitioned.sql
\i sql/functions/partition_get_partition_column_info.sql
\i sql/functions/partition_get_last_partition_details.sql
\i sql/functions/partition_get_grid_width.sql
\i sql/functions/partition_compute_bridge_upper.sql
\i sql/functions/partition_get_active_upper_bound.sql
\i sql/functions/partition_get_current_partition_boundaries.sql
\i sql/functions/partition_calculate_free_partitions.sql
\i sql/functions/partition_partitioned_on_primary_key.sql

-- Partition creation
\i sql/functions/partition_add_concurrent_index_on_partitioned_table.sql
\i sql/functions/partition_add_constraints.sql
\i sql/functions/partition_add_foreign_key_on_partitioned_table.sql
\i sql/functions/partition_add_up_to_nr_of_free_partitions.sql
\i sql/functions/partition_copy_fk_to_new_table.sql
\i sql/functions/partition_copy_indexes_to_new_table.sql
\i sql/functions/partition_create_table_inherits_from_template.sql
\i sql/functions/partition_alter_partitioned_table_options.sql

-- Partition conversion and table setup
\i sql/functions/partition_declarative.sql
\i sql/functions/partition_inheritance.sql
\i sql/functions/partition_table.sql
\i sql/functions/partition_convert_inheritance_to_native.sql
\i sql/functions/partition_table_native_wrapper.sql
\i sql/functions/partition_table_native_aligned_wrapper.sql

-- Partition detach and drop
\i sql/functions/partition_detach_partition.sql
\i sql/functions/partition_detach_partitions.sql
\i sql/functions/partition_detach_partitions_without_uuidv7.sql
\i sql/functions/partition_query_based_detach_partitions.sql
\i sql/functions/partition_query_based_maintenance_detach_partitions.sql
\i sql/functions/partition_drop_default_partition.sql
\i sql/functions/partition_drop_detached_partition.sql

-- Partition maintenance
\i sql/functions/partition_extend_all_partitioned_tables.sql
\i sql/functions/partition_change_range_on_partitioned_table.sql
\i sql/functions/partition_realign_boundaries.sql
\i sql/functions/partition_realign_with_leader.sql
\i sql/functions/partition_table_revert.sql
\i sql/functions/partition_table_revert_cleanup.sql

-- Partition inspection and reporting
\i sql/functions/partition_check_triggers_on_all_partitions.sql
\i sql/functions/partition_fix_triggers_on_all_partitions.sql
\i sql/functions/partition_report_table_free_extends_below_threshold.sql
\i sql/functions/partition_report_table_free_extends_below_date_threshold.sql
