-- =============================================================================
-- Unity Catalog / Delta Lake DDL for the MDM lakehouse.
-- Catalog per environment (mdm_dev, mdm_prod); schema per medallion layer.
-- =============================================================================
CREATE CATALOG IF NOT EXISTS mdm;
CREATE SCHEMA IF NOT EXISTS mdm.bronze COMMENT 'Raw, append-only, schema-on-read';
CREATE SCHEMA IF NOT EXISTS mdm.silver COMMENT 'Canonical, standardized, validated (current state per source record)';
CREATE SCHEMA IF NOT EXISTS mdm.gold   COMMENT 'Golden records, cross-reference, match & survivorship lineage';
CREATE SCHEMA IF NOT EXISTS mdm.ops    COMMENT 'Watermarks, run log, stage counts, alerts';
CREATE SCHEMA IF NOT EXISTS mdm.ref    COMMENT 'Reference data (countries, states, nicknames, vocabularies)';

-- ---------------------------------------------------------------- BRONZE (one per source)
CREATE TABLE IF NOT EXISTS mdm.bronze.crm (
  _source_system STRING NOT NULL, _source_record_id STRING NOT NULL, _ingest_run_id STRING NOT NULL,
  _ingest_ts TIMESTAMP, _source_file STRING, _record_hash STRING, _payload STRING, _rescued_data STRING
) CLUSTER BY (_ingest_run_id)
  TBLPROPERTIES (delta.enableChangeDataFeed = true, delta.logRetentionDuration = 'interval 365 days');
-- (identical tables for erp_billing, ecommerce, partner_api, party_events)

-- ---------------------------------------------------------------- SILVER
CREATE TABLE IF NOT EXISTS mdm.silver.party (
  record_key STRING NOT NULL COMMENT 'source_system:source_record_id',
  source_system STRING NOT NULL, source_record_id STRING NOT NULL, source_trust DOUBLE,
  first_name STRING, last_name STRING, full_name STRING, name_suffix STRING,
  email STRING  MASK mdm.ops.mask_email,             -- PII column masks (Unity Catalog)
  phone STRING  MASK mdm.ops.mask_phone,
  date_of_birth DATE MASK mdm.ops.mask_dob,
  address_line1 STRING, city STRING, state_code STRING, postal_code STRING, country_code STRING,
  status STRING, source_updated_at TIMESTAMP,
  _record_hash STRING, _ingest_run_id STRING, _source_file STRING,
  _transformations STRING COMMENT 'JSON: field-level lineage of every standardization step',
  _std_errors STRING, _raw STRING,
  _dq_status STRING COMMENT 'VALID | WARNING | QUARANTINED', _dq_score DOUBLE, _dq_failures STRING, _dq_run_id STRING,
  CONSTRAINT pk_party PRIMARY KEY (record_key) RELY,
  CONSTRAINT chk_dq CHECK (_dq_status IN ('VALID','WARNING','QUARANTINED'))
) CLUSTER BY (record_key) TBLPROPERTIES (delta.enableChangeDataFeed = true);

CREATE TABLE IF NOT EXISTS mdm.silver.dq_results (
  run_id STRING, record_key STRING, source_system STRING, rule_id STRING, rule STRING,
  severity STRING, dimension STRING, column STRING, value STRING
) CLUSTER BY (run_id, rule_id);

-- ---------------------------------------------------------------- GOLD
CREATE TABLE IF NOT EXISTS mdm.gold.golden_record (
  golden_id STRING NOT NULL, first_name STRING, last_name STRING, full_name STRING,
  email STRING MASK mdm.ops.mask_email, phone STRING MASK mdm.ops.mask_phone, date_of_birth DATE MASK mdm.ops.mask_dob,
  address_line1 STRING, city STRING, state_code STRING, postal_code STRING, country_code STRING, status STRING,
  source_record_count INT, source_systems STRING, member_record_keys STRING,
  attribute_confidence DOUBLE, match_confidence DOUBLE, record_hash STRING,
  version INT, created_run_id STRING, updated_run_id STRING, status_mdm STRING,
  CONSTRAINT pk_golden PRIMARY KEY (golden_id) RELY
) CLUSTER BY (golden_id) TBLPROPERTIES (delta.enableChangeDataFeed = true);   -- CDF = SCD2 history + downstream sync

CREATE TABLE IF NOT EXISTS mdm.gold.xref (
  record_key STRING NOT NULL, golden_id STRING NOT NULL, source_system STRING, source_record_id STRING, run_id STRING,
  CONSTRAINT pk_xref PRIMARY KEY (record_key) RELY,
  CONSTRAINT fk_xref_golden FOREIGN KEY (golden_id) REFERENCES mdm.gold.golden_record(golden_id)
) CLUSTER BY (golden_id);

CREATE TABLE IF NOT EXISTS mdm.gold.golden_events (
  run_id STRING, event STRING, golden_id STRING, related_golden_id STRING, record_keys STRING, reason STRING);
CREATE TABLE IF NOT EXISTS mdm.gold.golden_redirects (old_golden_id STRING, new_golden_id STRING, run_id STRING);
CREATE TABLE IF NOT EXISTS mdm.gold.survivorship_log (
  run_id STRING, golden_id STRING, attribute STRING, value STRING, winner_record_key STRING,
  winner_source STRING, deciding_rule STRING, candidates STRING) CLUSTER BY (golden_id);

-- ---------------------------------------------------------------- OPS
CREATE TABLE IF NOT EXISTS mdm.ops.watermarks (source STRING, value STRING);
CREATE TABLE IF NOT EXISTS mdm.ops.stage_counts (run_id STRING, source STRING, stage STRING, cnt BIGINT);

-- ---------------------------------------------------------------- PRIVACY
-- PII is masked for everyone except the mdm_pii_readers group (data stewards, operational systems).
CREATE FUNCTION IF NOT EXISTS mdm.ops.mask_email(v STRING) RETURN
  CASE WHEN is_account_group_member('mdm_pii_readers') THEN v ELSE regexp_replace(v, '^[^@]+', '***') END;
CREATE FUNCTION IF NOT EXISTS mdm.ops.mask_phone(v STRING) RETURN
  CASE WHEN is_account_group_member('mdm_pii_readers') THEN v ELSE concat('***', right(v, 4)) END;
CREATE FUNCTION IF NOT EXISTS mdm.ops.mask_dob(v DATE) RETURN
  CASE WHEN is_account_group_member('mdm_pii_readers') THEN v ELSE trunc(v, 'YEAR') END;

GRANT USE CATALOG ON CATALOG mdm TO `mdm_consumers`;
GRANT SELECT ON SCHEMA mdm.gold TO `mdm_consumers`;
GRANT SELECT, MODIFY ON SCHEMA mdm.silver TO `mdm_engineers`;

-- ---------------------------------------------------------------- handy views
CREATE OR REPLACE VIEW mdm.gold.v_reconciliation AS
SELECT run_id, source, stage, cnt FROM mdm.ops.stage_counts;

CREATE OR REPLACE VIEW mdm.gold.v_golden_as_of AS        -- point-in-time golden record via time travel
SELECT * FROM mdm.gold.golden_record TIMESTAMP AS OF current_timestamp() - INTERVAL 7 DAYS;
