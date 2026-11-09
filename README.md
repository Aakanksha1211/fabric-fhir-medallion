# FHIR API Data Ingestion & Analytics — Medallion Lakehouse on Microsoft Fabric

Ingests public FHIR R4 data into a Medallion Lakehouse (Raw → Bronze → Silver → Gold) on
Microsoft Fabric. JSON and XML are handled from the same source through one parameterized
pipeline, with paginated incremental ingestion, metadata/audit logging, SCD Type 2 history,
pipeline orchestration, and a one-page Power BI report.

- **Source:** `https://hapi.fhir.org/baseR4`
- **Resources:** Patient → Encounter → Observation → Condition (dependency order)
- **Formats:** JSON and XML, selected at run time via `p_format`
- **Platform:** Fabric Lakehouse, Spark notebooks, Data Pipelines, Power BI

## Architecture

```
                 FHIR R4 API (hapi.fhir.org/baseR4)
                          │  paginated pull (Bundle 'next' links)
                          │  incremental via _lastUpdated high-water mark
                          │  format chosen by p_format (json | xml)
                          ▼
┌──────────── RAW ────────────┐   Files/raw_json/<resource>/ingest_date=YYYY-MM-DD/*.json
│  API responses stored as-is │   Files/raw_xml/<resource>/ingest_date=YYYY-MM-DD/*.xml
└──────────────┬──────────────┘   (date-bucketed, one file per page)
               ▼
┌──────────── BRONZE ─────────┐   bronze_json_<resource>  (spark.read.json)
│  decode format → Delta      │   bronze_xml_<resource>   (ElementTree parse)
│  + metadata, keep payload   │   id, version_id, last_updated, resource_payload,
└──────────────┬──────────────┘   extraction_timestamp, api_url_or_params, run_id, ingest_date
               ▼
┌──────────── SILVER ─────────┐   silver_<resource>  (both formats merged into one table)
│  flatten + clean + dedup    │   source_format column tracks provenance (json / xml)
│  SCD Type 2 history         │   SCD2: effective_from / effective_to / is_current / record_hash
└──────────────┬──────────────┘
               ▼
┌──────────── GOLD ───────────┐   gold_dim_patient (dimension)
│  star schema + views        │   gold_fact_encounter / _observation / _condition (facts)
│  optimised for reporting    │   gold_vw_patient_360, gold_vw_observation_enriched (views)
└──────────────┬──────────────┘
               ▼
              Power BI — rpt_fhir_analytics (1-page report)
```

- **Raw** — each API response stored as-is, one file per page, bucketed by resource and `ingest_date`. Any layer can be rebuilt from raw without re-calling the API.
- **Bronze** — files decoded to Delta: JSON via `spark.read.json`, XML via ElementTree with the FHIR namespace. Keeps `resource_payload` plus metadata (`id`, `version_id`, `last_updated`, `extraction_timestamp`, `api_url_or_params`, `run_id`, `ingest_date`).
- **Silver** — reads `bronze_json_*` and/or `bronze_xml_*`, flattens nested FHIR fields to typed columns, strips reference prefixes (`Patient/123` → `123`), dedups to the latest version, applies SCD Type 2. `source_format` records provenance.
- **Gold** — star schema (patient dimension + encounter/observation/condition facts) and views, built from `is_current = true` silver.

Control tables: `control_watermark` (high-water mark per resource + format), `control_audit`
(one row per API call: `extraction_timestamp`, `load_timestamp`, `api_url_or_params`,
`record_count`, `format`).

## Format handling

`p_format` selects the format at run time. Raw switches the `Accept` header and file
extension; bronze switches the parser. Silver and gold are format-agnostic — running as XML
is just `p_format = xml`.

XML is decoded to columns in **bronze** (deserialization), not in a Dataflow Gen2; silver
performs the transformation. A single `FLATTEN` spec drives both formats: JSON via
`get_json_object`, XML via a JSONPath→element-path conversion
(`$.clinicalStatus.coding[0].code` → `clinicalStatus/coding/code`).

## Load strategy

| Layer | Strategy | Mechanism |
|-------|----------|-----------|
| Raw | Incremental | `_lastUpdated >= watermark`; append a new `ingest_date` partition each run |
| Bronze | Full rebuild | idempotent overwrite from all raw partitions |
| Silver | Incremental upsert | SCD2 `MERGE` — closes changed rows, inserts new versions, keeps history |
| Gold | Full rebuild | rebuilt from `is_current = true` silver |

## Orchestration

- **`pl_fhir_medallion`** (child) — `01 raw → 02 bronze → 03 silver → 04 gold`, wired with "on success" dependencies, each notebook fed `p_ingest_date` and `p_format`.
- **`pl_fhir_master`** (parent) — `ForEach` over a date array, invoking the child once per date (Fabric cannot nest `ForEach`). Run once per format.

Resources load Patient → Encounter → Observation → Condition so foreign keys resolve.

## Data model

```
                 ┌───────────────────────┐
                 │     gold_dim_patient  │
                 │  patient_id (PK)      │
                 │  name_full, gender,   │
                 │  birth_date, age,     │
                 │  marital_status       │
                 └───────────┬───────────┘
            patient_id (FK)  │
        ┌────────────────────┼────────────────────┐
        ▼                    ▼                     ▼
┌─────────────────┐ ┌─────────────────────┐ ┌─────────────────┐
│gold_fact_       │ │gold_fact_           │ │gold_fact_       │
│   encounter     │ │   observation       │ │   condition     │
│ encounter_id PK │ │ observation_id PK   │ │ condition_id PK │
│ patient_id   FK │ │ patient_id      FK  │ │ patient_id   FK │
│ status, class,  │ │ encounter_id    FK ─┼─┼ encounter_id FK │
│ type, period    │ │ code_text, value_*  │ │ code_text,      │
│ encounter_date  │ │ effective_datetime  │ │ onset/recorded  │
└─────────────────┘ └─────────────────────┘ └─────────────────┘
```

- `gold_fact_*.patient_id` → `gold_dim_patient.patient_id`
- `gold_fact_observation.encounter_id` / `gold_fact_condition.encounter_id` → `gold_fact_encounter.encounter_id`
- Silver SCD2 columns: `business_id`, `record_hash` (SHA-256), `effective_from` / `effective_to`, `is_current`.

Note: on the shared public test server, child resources do not always reference the paged
patients, so some joins are sparse — the keys and model are correct, density depends on the
source data.

## Running it

1. Create a Fabric workspace and Lakehouse; pin the Lakehouse as default. Import the five notebooks.
2. Build the child pipeline (`pl_fhir_medallion`) as the four-notebook chain, and the parent (`pl_fhir_master`) as a date `ForEach` invoking the child with `p_ingest_date = @item()`.
3. Run the parent with `p_format = json`, then `p_format = xml`. Silver merges both; gold rebuilds from the merged silver.
4. Build the Power BI report on the gold tables.

## Report

`rpt_fhir_analytics`, on the gold model: count cards (patients, conditions, observations),
patients by gender, top conditions, encounters by class, and a patient detail table.

<img width="1163" height="646" alt="FHIR analytics report — cards, gender donut, conditions and encounters" src="https://github.com/user-attachments/assets/1388aad9-76c9-4628-9c13-3a04d401005c" />

<img width="910" height="644" alt="FHIR analytics report — patient detail and encounters by class" src="https://github.com/user-attachments/assets/59754a2d-c514-431f-850d-43a5b9d3e767" />
