-- Legacy JDBC dimension refresh retained as a documented migration marker.
-- Since Flink 2.2 / Flink CDC 3.6, mysql-to-paimon.yaml owns the initial
-- snapshot and continuous binlog synchronization for all four DIM tables.
-- DolphinScheduler no longer executes this file.
SET 'execution.runtime-mode' = 'batch';
