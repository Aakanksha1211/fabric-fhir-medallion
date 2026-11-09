# FHIR API Data Ingestion & Analytics — Medallion Lakehouse on Microsoft Fabric

This project pulls public **FHIR R4** healthcare data from a live API and lands it in a
**Medallion Lakehouse** (Raw → Bronze → Silver → Gold) on Microsoft Fabric. The
interesting part is that the *same* source serves both JSON and XML, and instead of
building two pipelines I handle both through one parameterized flow. Along the way it does
paginated incremental ingestion, keeps metadata and audit logs, tracks history with SCD
Type 2, orchestrates everything in dependency order, and finishes with a one-page Power BI
report on the gold layer.

A few quick facts to set the scene:

- **Source:** `https://hapi.fhir.org/baseR4` — the public HAPI FHIR R4 test server (no auth).
- **Resources:** Patient, Encounter, Observation, Condition — loaded in that order, because the later ones reference the earlier ones.
- **Formats:** JSON and XML, chosen at run time with a single `p_format` parameter.
- **Built on:** Fabric Lakehouse, Spark notebooks, Data Pipelines, and Power BI.

## How the data flows

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

**Raw** is the landing zone. Every API response is written to OneLake exactly as it came
back — one file per page, bucketed by resource and ingest date. Nothing is reshaped here,
which means any layer downstream can be rebuilt from raw without touching the API again.

**Bronze** turns those files into Delta tables. For JSON that's `spark.read.json`; for XML
it's an ElementTree parse with the FHIR namespace. Each row keeps the original record in a
`resource_payload` column plus ingestion metadata — the id, version, last-updated
timestamp, and the API call details joined in from the audit log.

**Silver** is where the real transformation happens. It reads whatever bronze tables exist
(JSON, XML, or both), flattens the nested FHIR fields into clean typed columns, strips
reference prefixes like `Patient/123` down to the bare id, de-duplicates to the latest
version of each record, and maintains full history with SCD Type 2. A `source_format`
column records whether each row originally came from the JSON or the XML path.

**Gold** is a small star schema built for reporting — a patient dimension, fact tables for
encounters, observations and conditions, and a couple of views that pre-join the common
questions. It reads only the current version (`is_current = true`) from silver.

Two control tables keep the flow honest: `control_watermark` holds a high-water mark per
resource and format so each run only pulls what's new, and `control_audit` records one row
per API call (when it was called, when it was saved, the URL, the record count, and the
format).

## One source, two formats, one pipeline

The same endpoint will hand back JSON or XML depending on the `Accept` header, so it felt
wrong to maintain two parallel pipelines. Instead format is just a parameter. Raw and
bronze flip their behaviour on `p_format` — the Accept header and file extension in raw,
the parser in bronze — and silver and gold don't care about format at all. Running the
whole thing as XML is nothing more than passing `p_format = xml`.

The one design question worth explaining is *where* the XML gets turned into columns. The
brief asks for a Dataflow Gen2 that does transformation but not the raw-to-table
conversion, and FHIR XML (with its namespaces and nested coding arrays) is genuinely
painful to parse in Power Query anyway. So I draw the line clearly: **bronze decodes the
format, silver transforms it.** Decoding a serialized payload into columns is
deserialization, not business logic, so it belongs upstream. And because FHIR's JSON and
XML are structurally the same shape, I reuse the one `FLATTEN` spec for both — JSON reads
it through `get_json_object`, and for XML I convert each JSONPath to the equivalent element
path (`$.clinicalStatus.coding[0].code` becomes `clinicalStatus/coding/code`). One spec,
both formats, no hardcoding.

## How much gets reloaded each run

The load strategy is deliberately different per layer rather than one blanket approach.
Ingestion is incremental — each run reads the watermark, asks the API for records with
`_lastUpdated` at or after it, and appends a new dated partition, so the source is never
fully re-read. Silver is an incremental upsert: the SCD Type 2 `MERGE` closes rows that
changed and opens new versions, keeping the whole history. Bronze and gold, by contrast,
are idempotent full rebuilds — at this data volume, rebuilding them cleanly from the layer
below is simpler and less error-prone than trying to merge, and the compute saved wouldn't
be worth the extra complexity. In short, the pipeline is incremental exactly where it
matters (the API boundary and the history table) and simple everywhere else.

## Orchestration

Two pipelines handle the scheduling. The child, `pl_fhir_medallion`, runs the four
notebooks in order — raw, bronze, silver, gold — wired with "on success" dependencies so a
layer only runs if the previous one worked, and each notebook receives the `p_ingest_date`
and `p_format` parameters. The parent, `pl_fhir_master`, wraps a `ForEach` over a list of
dates and invokes the child once per date; because Fabric doesn't allow nested `ForEach`,
the date loop and the resource chain have to live in separate pipelines. You run the parent
once for JSON and once for XML. Within each run, the resources go Patient → Encounter →
Observation → Condition so the foreign keys always resolve.

## The data model

The gold layer is a straightforward star:

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

Every fact points back to `gold_dim_patient` on `patient_id`, and observations and
conditions also carry an `encounter_id` back to the encounter fact. The silver tables
underneath carry the SCD2 machinery — `business_id`, a SHA-256 `record_hash` for change
detection, the `effective_from` / `effective_to` validity window, and the `is_current`
flag — so point-in-time history is always available even though gold only shows the present.

One honest caveat: on a shared public test server the child resources don't always
reference the same patients that happened to be paged, so some joins come back sparse. The
keys and the model are correct; the referential density just depends on what the source
happens to hold.

## Running it yourself

1. Create a Fabric workspace and Lakehouse, and **pin the Lakehouse as the default** —
   bare table names in `saveAsTable` won't resolve otherwise. Import the five notebooks.
2. Build the child pipeline (`pl_fhir_medallion`) as the four-notebook chain, and the
   parent (`pl_fhir_master`) as a date `ForEach` that invokes the child with
   `p_ingest_date = @item()`.
3. Run the parent with `p_format = json`, then again with `p_format = xml`. Silver merges
   the two and gold rebuilds from the merged result.
4. Build the Power BI report on the gold tables.

To show two or three days of incremental loading, the parent's date array just lists a few
dates and runs the child once per date. Because the watermark moves forward after every
run, each dated partition is a genuinely new slice rather than a re-pull of the same data.

A couple of Fabric-specific things worth knowing if you're reproducing it: each notebook's
first cell is a parameters cell (`ingest_date`, `data_format`), and `%run 00_config` has to
sit alone in its own cell. One small but real gotcha in the XML bronze path is that I
declare an explicit `StringType` schema — without it, a column that happens to be all-null
for a resource infers as `NullType` and the Delta write fails.

## What the assignment asked for

- [x] Incremental ingestion with pagination, demonstrated across 2–3 days
- [x] Raw responses stored as-is, then converted to Delta tables
- [x] Metadata columns (`extraction_timestamp`, `api_url_or_params`)
- [x] Versioning — when called / when saved — plus SCD Type 2 history
- [x] Full medallion: Raw → Bronze → Silver → Gold
- [x] Silver cleaning and dedup; gold warehouse views
- [x] Orchestration in dependency order (parent + child pipelines)
- [x] Modular, config-driven code with no hardcoding
- [x] Documentation of the pipeline and table relationships
- [x] XML variant through the same pipeline (`p_format = xml`)
- [x] One-page Power BI report on the gold layer

## The report

The report, `rpt_fhir_analytics`, sits on top of the gold model. It opens with count cards
for patients, conditions and observations, then breaks patients down by gender, shows the
most common conditions and encounters by class, and lists patient detail — so all four FHIR
resources are represented at a glance.

<img width="1163" height="646" alt="FHIR analytics report — cards, gender donut, conditions and encounters" src="https://github.com/user-attachments/assets/1388aad9-76c9-4628-9c13-3a04d401005c" />

<img width="910" height="644" alt="FHIR analytics report — patient detail and encounters by class" src="https://github.com/user-attachments/assets/59754a2d-c514-431f-850d-43a5b9d3e767" />
