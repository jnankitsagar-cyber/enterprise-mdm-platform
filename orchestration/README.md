# Orchestration

* **ADF (`adf/`)** owns *data movement into the lake* (SFTP, on-prem DBs via
  self-hosted IR, SaaS connectors) and the schedule. It then triggers one
  Databricks job. ADF is used because it has 90+ managed connectors and the
  self-hosted integration runtime for on-prem sources - things we would
  otherwise build and operate ourselves.
* **Databricks Workflows (`databricks/databricks.yml`)** owns *data processing*:
  a DAG of bronze -> silver -> identity resolution -> survivorship ->
  reconciliation, with per-task retries, a single concurrent run (golden-id
  assignment must not race), spot instances with on-demand fallback and alerts.
  Deployed as a Databricks Asset Bundle from CI (`databricks bundle deploy -t prod`).

Locally, `python -m mdm run` executes the same stages in-process.
