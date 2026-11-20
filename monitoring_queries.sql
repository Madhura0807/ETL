-- Handy queries over the pipeline metadata tables (MySQL / SQLite compatible).

-- Last 20 runs
SELECT pipeline_name, execution_id, status, start_time, duration_seconds,
       extracted_count, valid_count, invalid_count, loaded_count, failed_batch_count, schema_drift_status
FROM etl_pipeline_runs ORDER BY start_time DESC LIMIT 20;

-- Which quality checks fail most often
SELECT check_name, COUNT(*) AS runs_failed, SUM(failed) AS total_failed_records
FROM etl_data_quality_results WHERE status <> 'PASSED'
GROUP BY check_name ORDER BY total_failed_records DESC;

-- Why were records quarantined in a run?
SELECT stage, reason, COUNT(*) AS records
FROM etl_quarantined_records WHERE execution_id = 'PUT-EXECUTION-ID-HERE'
GROUP BY stage, reason ORDER BY records DESC;

-- Failed or rolled-back batches
SELECT execution_id, batch_number, row_count, status, attempts, error_message
FROM etl_batch_log WHERE status IN ('FAILED', 'ROLLED_BACK') ORDER BY created_at DESC;

-- Schema drift history
SELECT pipeline_name, created_at, previous_version, status, decision
FROM etl_schema_drift_history ORDER BY created_at DESC;
