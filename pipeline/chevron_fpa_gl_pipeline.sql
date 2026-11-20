-- chevron_fpa_gl_pipeline.sql
-- Bronze -> Gold dims -> Silver -> Gold fact
-- Parameters: ${raw_path} (set in pipeline configuration)

-- ============================================================
-- Bronze: 5 streaming tables from CSV volume
-- ============================================================

CREATE OR REFRESH STREAMING TABLE bronze_gl_actuals
COMMENT 'Raw GL actuals from CSV'
AS SELECT _metadata.file_path AS source_file,
          current_timestamp() AS ingest_ts, *
FROM STREAM read_files('${raw_path}/gl_actuals/',
     format => 'csv', header => true, inferColumnTypes => false);

CREATE OR REFRESH STREAMING TABLE bronze_dim_account
AS SELECT _metadata.file_path AS source_file,
          current_timestamp() AS ingest_ts, *
FROM STREAM read_files('${raw_path}/dim_account/',
     format => 'csv', header => true, inferColumnTypes => false);

CREATE OR REFRESH STREAMING TABLE bronze_dim_cost_center
AS SELECT _metadata.file_path AS source_file,
          current_timestamp() AS ingest_ts, *
FROM STREAM read_files('${raw_path}/dim_cost_center/',
     format => 'csv', header => true, inferColumnTypes => false);

CREATE OR REFRESH STREAMING TABLE bronze_dim_time
AS SELECT _metadata.file_path AS source_file,
          current_timestamp() AS ingest_ts, *
FROM STREAM read_files('${raw_path}/dim_time/',
     format => 'csv', header => true, inferColumnTypes => false);

CREATE OR REFRESH STREAMING TABLE bronze_dim_scenario
AS SELECT _metadata.file_path AS source_file,
          current_timestamp() AS ingest_ts, *
FROM STREAM read_files('${raw_path}/dim_scenario/',
     format => 'csv', header => true, inferColumnTypes => false);

-- ============================================================
-- Gold dims (declare BEFORE silver; QUALIFY dedup, latest wins)
-- ============================================================

CREATE OR REFRESH MATERIALIZED VIEW dim_account (
  CONSTRAINT valid_account EXPECT (account_id RLIKE '^\\d{4}$') ON VIOLATION FAIL UPDATE)
AS SELECT account_id, account_name, statement, account_type
FROM bronze_dim_account
QUALIFY row_number() OVER (PARTITION BY account_id ORDER BY ingest_ts DESC) = 1;

CREATE OR REFRESH MATERIALIZED VIEW dim_cost_center (
  CONSTRAINT valid_cc EXPECT (cost_center_id RLIKE '^CC\\d{3}$') ON VIOLATION FAIL UPDATE)
AS SELECT cost_center_id, cost_center_name
FROM bronze_dim_cost_center
QUALIFY row_number() OVER (PARTITION BY cost_center_id ORDER BY ingest_ts DESC) = 1;

CREATE OR REFRESH MATERIALIZED VIEW dim_time (
  CONSTRAINT valid_period_fmt EXPECT (period RLIKE '^\\d{4}-\\d{2}$') ON VIOLATION FAIL UPDATE)
AS SELECT period, CAST(year AS INT) AS year, CAST(month AS INT) AS month,
          CAST(is_actuals AS BOOLEAN) AS is_actuals
FROM bronze_dim_time
QUALIFY row_number() OVER (PARTITION BY period ORDER BY ingest_ts DESC) = 1;

CREATE OR REFRESH MATERIALIZED VIEW dim_scenario (
  CONSTRAINT valid_scenario EXPECT (scenario_id IN ('ACT','BUD','FC1','FCST','WIF')) ON VIOLATION FAIL UPDATE)
AS SELECT scenario_id, scenario_name
FROM bronze_dim_scenario
QUALIFY row_number() OVER (PARTITION BY scenario_id ORDER BY ingest_ts DESC) = 1;

-- ============================================================
-- Silver: validated actuals (DROP on 3+1 rules, WARN on cc_format)
-- ============================================================

CREATE OR REFRESH STREAMING TABLE silver_actuals (
  CONSTRAINT valid_period EXPECT (period RLIKE '^\\d{4}-(0[1-9]|1[0-2])$') ON VIOLATION DROP ROW,
  CONSTRAINT amount_not_null EXPECT (amount IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT known_account EXPECT (dim_account_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT actuals_only EXPECT (is_actuals = true) ON VIOLATION DROP ROW,
  CONSTRAINT cost_center_format EXPECT (cost_center_id RLIKE '^CC\\d{3}$'))
AS SELECT
    s.period, s.account_id, s.cost_center_id, s.scenario_id,
    CAST(NULLIF(s.amount, '') AS DECIMAL(18,2)) AS amount,
    CAST(s.is_actuals AS BOOLEAN) AS is_actuals,
    a.account_id AS dim_account_id,
    s.ingest_ts
FROM STREAM bronze_gl_actuals s
LEFT JOIN dim_account a ON s.account_id = a.account_id;

-- ============================================================
-- Gold fact: deduplicated by ingest time (latest wins)
-- ============================================================

CREATE OR REFRESH MATERIALIZED VIEW fact_actuals
AS SELECT period, account_id, cost_center_id, scenario_id,
          amount, is_actuals
FROM silver_actuals
QUALIFY row_number() OVER (
  PARTITION BY period, account_id, cost_center_id, scenario_id
  ORDER BY ingest_ts DESC
) = 1;
