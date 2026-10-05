# Operations runbook

## Daily health check (2 minutes)
1. Open `reports/dashboard.html` (Azure Workbook in production). Run status should be **SUCCESS / BALANCED** with no alerts.
2. Watch the DQ pass rate trend. A sudden drop for one source usually means an upstream format change.
3. Check the review queue size. Growth means a new source pattern, or thresholds that need recalibrating.

## Alerts

| alert | severity | first action |
|---|---|---|
| `reconciliation_break` | critical | Open `reports/reconciliation_<run>.md` → *Exceptions* and follow the playbook below. Hold downstream publication if gold is affected (R4–R6). |
| `dead_letter_rate_high` | critical | `jq -r .reason lakehouse/_dead_letter/<run>.jsonl \| sort \| uniq -c`. One dominant reason usually means one upstream defect. |
| `dq_pass_rate_low` | warning | Open `reports/dq_scorecard_<run>.csv` sorted by pass_rate and compare with the previous run. |
| `review_queue_growing` | warning | `python -m mdm review`. If many pairs share one pattern, adjust config instead of deciding them one by one. |

## Investigating reconciliation exceptions

| check | meaning | where to look |
|---|---|---|
| **R1** source ≠ bronze + DLQ | records lost at the edge | 1) Does the `.ctl` control total match the file's line count? (truncated transfer: re-request the file). 2) DLQ `stage=ingest` reasons. 3) For DB/API: does the `COUNT(*)` predicate equal the extract predicate (watermark)? |
| **R2** bronze ≠ standardized + DLQ | lost in mapping | DLQ `stage=standardize`. Usually a mapping that references a renamed field. Check `schema_registry` in `_state/ingestion_state.json` for a new version. |
| **R3** standardized ≠ unchanged + DQ outcomes | counting bug or duplicate keys | `DQ050` hits for the source; unchanged re-sends (hash) count |
| **R4** eligible ≠ xref | valid record not mastered, or mastered twice | anti-join `silver.party` and `gold.xref` on `record_key`; matching logs for skipped blocks |
| **R5** golden ≠ xref IDs / orphans | inconsistent gold | `golden_events` for this run (MERGE/RETIRE); `golden_redirects` |
| **R6** golden delta unexplained | IDs created or lost outside the lifecycle | diff `gold/golden_record/_versions/<prev>` against current, then compare with `golden_events` |
| **Q1** quarantine rate high | upstream data defect | group `silver.dq_results` by `rule_id` for that source |

Try it: `python -m mdm demo --simulate-truncated-file` produces an R1 break on day 2 and fires the critical alert.

## Tracing a golden record
```bash
python -m mdm trace GR-XXXXXXXXXX            # or GET /golden-records/{id}/lineage
```
The trace shows raw values, file and position, every transformation, DQ failures, match evidence, survivorship winners and lifecycle events.

## Data-steward workflow
1. `python -m mdm review` lists the pairs with both records side by side.
2. Append decisions to `lakehouse/_state/steward_decisions.csv`:
   ```csv
   record_key_a,record_key_b,decision,steward,reason
   crm:C100013,ecommerce:9002,MATCH,j.doe,confirmed by phone call
   ```
3. Re-run. Overrides beat the model, are never re-scored, and `NO_MATCH` acts as a cannot-link constraint in clustering.

## Replaying dead-lettered records
Fix the data upstream (or hand-correct it), drop a new file into the source's landing folder, and run. The file manifest is keyed on name *and checksum*, so a corrected file with the same name is picked up.

## Re-processing after a rule change
Bronze keeps raw payloads, so a fix to standardization or DQ can be applied to history. In production, run job 02 for the affected runs with a full refresh of silver, then 03 and 04. Golden IDs stay stable because assignment honours the existing xref.
