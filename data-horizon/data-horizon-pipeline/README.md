# Data Horizon Pipeline

An end-to-end industrial data pipeline built on AWS. Every 6 hours, the pipeline ingests tag data from a source API, transforms and validates the data with PySpark, and loads the results into Redshift for analytics — fully orchestrated by Step Functions, fully reproducible with Terraform.

---

## Architecture

```
EventBridge (every 6 hours)
        │
        ▼
┌─────────────────────────────────────────────────────┐
│              Parent State Machine                   │
│                                                     │
│  Child1: Config & Map                               │
│  ├── Lambda: load tag list from S3                  │
│  ├── Lambda: write run metadata → DynamoDB          │
│  └── Lambda: generate Map State input → S3          │
│        │                                            │
│  Child2: Data Extraction                            │
│  └── Map State (up to 10 concurrent-configurable)                │
│      └── Lambda × N: fetch tag data from source API │
│          ├── raw JSON → S3 (raw bucket)             │
│          └── status → DynamoDB                      │
│        │                                            │
│  Child3: Transformation & Validation                │
│  ├── Glue Job 1 (transform):                        │
│  │   raw JSON → Parquet (cleaned bucket)            │
│  └── Glue Job 2 (validation):                       │
│      PASS → Parquet (validated bucket)              │
│      FAIL → quarantine bucket                       │
│        │                                            │
│  Child4: Redshift Load                              │
│  └── Step Functions SDK: COPY validated Parquet     │
│      into 13 Redshift staging tables                │
└─────────────────────────────────────────────────────┘
        │
        ▼ (on any failure)
      SNS → email alert
      SQS DLQ → failed tag batches
```

### Key Design Decisions

**Step Functions as the failure authority.** Every Lambda error and Glue failure propagates up through the child state machine's Fail state to the parent's `States.ALL` catch. The parent `ExecutionsFailed` metric is the canonical alarm — there are no redundant Lambda `Errors` or Glue failure alarms.

**Fault-isolated Glue jobs.** A single corrupt tag file does not abort a Glue run. The transform job catches per-tag read errors, marks that tag `FAILED` in DynamoDB, and continues processing all remaining tags. The validation job does the same — a tag is marked `VALIDATE=FAILED` only when every record it contributed across all 13 tables was quarantined.

**Single-table DynamoDB design.** One `PipelineAudit` table tracks all run metadata and tag records using prefix-based partition keys (`RUN#`, `TAG#`) with a GSI (`PIPELINE#`) for pipeline-wide queries.

**Child2 tolerates partial failure.** The map state allows 50% item failure without failing the child state machine. Failed tag batches route to an SQS DLQ — the DLQ alarm is the signal for partial extraction failures, since the child can succeed while individual tags failed.

---

## Tech Stack

| Layer | Technology |
|---|---|
| Language | Python 3.12+ |
| Orchestration | AWS Step Functions (ASL) — 1 parent + 4 child state machines |
| Serverless compute | AWS Lambda (config loader, map state processor) |
| Batch compute | AWS Glue PySpark (transform, validation) |
| Data warehouse | Amazon Redshift (dc2.large, VPC-isolated) |
| Storage | Amazon S3 — 7 dedicated buckets |
| State tracking | Amazon DynamoDB (single-table, on-demand capacity) |
| Scheduling | Amazon EventBridge (every 6 hours) |
| Messaging | Amazon SQS (2 DLQs), Amazon SNS (failure alerts) |
| Schema registry | AWS Glue Data Catalog (13 domain tables) |
| Config / secrets | AWS SSM Parameter Store |
| Monitoring | Amazon CloudWatch — alarms, structured JSON logs |
| Infrastructure | Terraform — modular components, 3 environments |
| Code quality | ruff (lint + format), mypy (type checking), pytest (unit + integration) |

---

## Repository Layout

```
data-horizon-pipeline/
├── glue_jobs/
│   ├── scripts/
│   │   ├── transform_job.py          # PySpark: raw JSON → cleaned Parquet (13 tables)
│   │   └── validation_job.py         # PySpark: validate cleaned data, route valid/quarantine
│   └── utils/
│       ├── schema_definitions.py     # 13 domain table schemas + column maps
│       ├── spark_helpers.py          # GlueContext init, S3 read/write, Parquet helpers
│       ├── validation_rules.py       # Per-table quality rules (null, enum, range, duplicate)
│       └── dynamodb_updater.py       # Bulk tag status writes during Glue runs
│
├── lambdas/
│   ├── orchestrator/                 # Child1 Lambda — config load, DynamoDB init, map gen
│   │   ├── handler.py
│   │   ├── config_loader.py
│   │   ├── dynamodb_writer.py
│   │   └── map_state_generator.py
│   └── map_state_processor/         # Child2 Lambda — one invocation per tag 
│       ├── handler.py
│       ├── api_client.py
│       ├── response_processor.py
│       ├── s3_writer.py
│       └── dynamodb_updater.py
│
├── statemachine/
│   ├── modular_orchestrator.asl.json # Parent — sequences Child1 → Child4
│   ├── config_loader.asl.json        # Child1
│   ├── data_extractor.asl.json       # Child2 — Map State fan-out
│   ├── transformation.asl.json       # Child3 — Glue transform + validation in sequence
│   └── redshift_load.asl.json        # Child4 — SDK integration
│
├── shared/
│   ├── aws_clients.py                # Cached boto3 factory + CloudWatch metric helper
│   ├── constants.py                  # DynamoDB key prefixes, status enums, SSM loader
│   ├── exceptions.py                 # PipelineError hierarchy (retryable vs permanent)
│   └── logger.py                     # Structured JSON logging, run_id ContextVar
│
├── terraform/
│   ├── main.tf                       # Module composition (all  modules wired here)
│   ├── variables.tf                  # input variables
│   ├── outputs.tf                    # ARNs, endpoints, bucket names
│   ├── environments/
│   │   ├── dev/                      # dev.tfvars + backend.hcl
│   │   ├── staging/                  # staging.tfvars + backend.hcl
│   │   └── prod/                     # prod.tfvars + backend.hcl
│   └── modules/
│       ├── vpc/                      # VPC + private subnets for Redshift
│       ├── s3/                       # 7 buckets (raw, cleaned, validated, quarantine,
│       │                             #   scripts, orchestration, config) + lifecycle rules
│       ├── dynamodb/                 # PipelineAudit table + GSI
│       ├── sqs/                      # extraction-failures + eventbridge-failures DLQs
│       ├── sns/                      # pipeline-failure-alerts topic
│       ├── iam/                      # least-privilege roles (Lambda ×2, Glue,
│       │                             #   Step Functions, EventBridge)
│       ├── lambda/                   # Config Loader + Map State Processor functions
│       ├── glue/                     # Transform + Validation Glue job definitions
│       ├── glue_catalog/             # Glue Data Catalog DB + domain tables
│       ├── step_function/            # Parent + 4 child state machines
│       ├── redshift/                 # dc2.large cluster, subnet group, IAM role
│       ├── eventbridge/              # 6-hour schedule rule + DLQ failure routing
│       ├── cloudwatch/               # Log groups (30-day retention) +  alarms
│       └── ssm_parameters/           # 10+ SecureString parameters under /data-horizon/{env}/
│
├── redshift/
│   ├── migrations/                   # 001_create_schema, 002_create_tables (13 tables)
│   └── copy_commands/                # COPY command templates for Parquet ingestion
│
├── config/
│   ├── source_config/                # API endpoint and auth config
│   └── schemas/                      # raw_schema.json, cleaned_schema.json
│
├── scripts/
│   ├── deploy.sh                     # Package Lambdas, upload Glue scripts, terraform apply
│   ├── teardown.sh                   # terraform destroy + cleanup
│   ├── run_pipeline_manual.sh        # Manually trigger a pipeline run
│   ├── upload_glue_scripts.sh        # Sync glue_jobs/ to S3 scripts bucket
│   ├── package_lambdas.sh            # Zip each Lambda with its dependencies
│   └── seed_config.sh                # Seed source config to S3
```

---

## S3 Bucket Layout

| Bucket | Purpose | Lifecycle |
|---|---|---|
| `raw` | One JSON file per tag per run (`raw/<run_id>/<tag_id>.json`) | Glacier Deep Archive after 30 days |
| `cleaned` | Parquet output from transform job, partitioned by table | Glacier Deep Archive after 30 days |
| `validated` | Parquet output from validation job — Redshift COPY source | Standard |
| `quarantine` | Records that failed validation rules | Standard |
| `scripts` | Glue job scripts and utils uploaded by CI/CD | Standard |
| `orchestration` | Map State input JSON files written by orchestrator Lambda | 30-day expiry |
| `config` | Source API config and JSON schemas | Standard |


---

## DynamoDB Schema

Single table: `PipelineAudit`

| Pattern | PK | SK | Attributes |
|---|---|---|---|
| Run metadata | `RUN#<run_id>` | `META` | status, start_time, tag_count, source |
| Tag record | `RUN#<run_id>` | `TAG#<tag_id>` | extract_status, transform_status, validate_status, record_count |
| Pipeline-level GSI | `PIPELINE#<source>` | `RUN#<run_id>` | Used for listing runs by pipeline |

---

## Data Domain

The pipeline processes **13 domain tables** per run:

`TAG`, `EQUIPMENT`, `LOCATION`, `CUSTOMER`, `MEASUREMENTS`, `EVENTS`, `CALIBRATION`, `MAINTENANCE`, `ALARMS`, `SETPOINTS`, `PROCESS_DATA`, `QUALITY_METRICS`, `CONFIGURATION`

Each table is independently transformed, validated, and loaded into a corresponding Redshift staging table via `TRUNCATE + COPY`.

---

## Monitoring

Monitoring is implemented as  CloudWatch metric alarms wired to a single SNS topic.

### Alarm Summary

| Category | Alarms | Signal |
|---|---|---|
| Step Functions | `ExecutionsTimedOut`, `ExecutionThrottled` on parent;| Canonical pipeline failure |
| Lambda | `Throttles` > 5 in 5 min; `Duration` > 80% of timeout | Pre-failure — SF never sees throttled requests |
| Glue | JVM heap > 80%; `numFailedTasks` > 0; `ValidationRejectionRate` > 15% | Internal Spark health — SF only sees pass/fail |
| SQS DLQ | `ApproximateNumberOfMessagesVisible` > 0; `ApproximateAgeOfOldestMessage` > 6h | Only signal for partial extraction failures |
| DynamoDB | `SystemErrors`, `UserErrors`, `ThrottledRequests` | DynamoDB errors do not always kill Lambda/Glue |

### SNS Severity Tiers

**P1 — immediate response:** `ExecutionsFailed` / `ExecutionsTimedOut` on any state machine; DLQ message visible; `PipelineRunDurationMinutes` > 300.

**P2 — investigate within the hour:** Glue JVM heap > 80%; `ValidationRejectionRate` > 15%; Lambda `Throttles` or `Duration`; DynamoDB `UserErrors`.

**P3 — dashboard trends:** S3 volume trends; DynamoDB latency; per-table durations; concurrent executions.

---

## Infrastructure

All infrastructure is managed with Terraform.  modules decompose every AWS resource into a single-purpose component.

### Environments

| Environment | Workers | Concurrency | Redshift |
|---|---|---|---|
| dev | 2 × G.1X | 1 | 1 × dc2.large |
| staging| 2 × G.1X | 1 | 1 × dc2.large |
| prod | 2 × G.1X | 1 | 1 × dc2.large |

### Key Variables

| Variable | Default | Description |
|---|---|---|
| `transform_workers` | 2 | Glue G.1X workers for transform job |
| `validation_workers` | 2 | Glue G.1X workers for validation job |
| `map_state_concurrency` | 10 | Max concurrent tag extractions |
| `config_loader_timeout` | 60s | Lambda timeout (alarms fire at 80%) |
| `map_processor_timeout` | 60s | Lambda timeout (alarms fire at 80%) |
| `log_retention_days` | 30 | CloudWatch log group retention |
| `redshift_node_type` | dc2.large | Redshift node type |

---

## Prerequisites

- AWS CLI configured with valid credentials
- Terraform >= 1.1.5
- Python 3.12+
- `uv` for running ruff, pytest, mypy
- bash available in PATH (Git Bash on Windows)

---

## Setup

### 1. Initialize Terraform

```bash
cd terraform
terraform init -backend-config=environments/dev/backend.hcl
```

### 2. Review and apply infrastructure

```bash
terraform plan -var-file=environments/dev/terraform.tfvars
terraform apply -var-file=environments/dev/terraform.tfvars
```

### 3. Deploy the full pipeline

```bash
bash scripts/deploy.sh dev build
bash scripts/deploy.sh dev upload
```

This script packages the Lambda functions, uploads Glue job scripts to S3, and applies Terraform in one step.

### 4. Apply Redshift migrations

```bash
bash scripts/run_redshift_migrations.sh dev
```

## Common Commands

```bash
# Run all unit tests
uv run pytest -x -q


# Lint and format
uv run ruff check . --fix
uv run ruff format .

# Type check
uv run mypy lambdas/ glue_jobs/ shared/

```

---

## Logging

All deployed components emit structured JSON logs to CloudWatch. Every log entry carries:

- `run_id` — injected via a `ContextVar` in `shared/logger.py`; flows from EventBridge trigger through all Lambdas and Glue job arguments
- `level`, `logger`, `timestamp` — standard fields
- Component-specific fields:  `tag_id`, `record_count`, `duration_ms`

The `run_id` is the primary correlation key for tracing a full pipeline execution across Lambda, Glue, Step Functions, and DynamoDB.

---

## Error Handling

| Failure type | Behavior |
|---|---|
| Lambda error (transient) | Step Functions retries with exponential backoff (configured per child ASL) |
| Lambda error (permanent) | Child SF moves to Fail state → parent catches → SNS alert |
| Tag batch extraction failure | Routes to `extraction-failures` SQS DLQ; child2 continues other tags |
| Single corrupt tag file (Glue) | Tag marked `FAILED` in DynamoDB; job continues all other tags |
| Record fails validation | Written to quarantine bucket; tag marked `VALIDATE=FAILED` only if all its records were quarantined |
| EventBridge trigger failure | Routes to `eventbridge-failures` SQS DLQ |

Custom exceptions in `shared/exceptions.py` (`RetryableError`, `PermanentError`) map directly to Step Functions retry vs catch behavior.

---

## IAM — Least-Privilege Roles

| Role | Bound to | Key permissions |
|---|---|---|
| `lambda-config-loader` | Orchestrator Lambda | S3:GetObject (config), DynamoDB:PutItem, SSM:GetParametersByPath |
| `lambda-map-processor` | Map State Processor Lambda | S3:PutObject (raw), DynamoDB:UpdateItem, SSM:GetParametersByPath, SecretsManager:GetSecretValue |
| `glue-job` | Transform + Validation Glue jobs | S3:GetObject/PutObject (raw→cleaned→validated→quarantine), DynamoDB:UpdateItem, CloudWatch:PutMetricData, Glue:GetTable/UpdatePartition |
| `step-functions` | Parent + child state machines | Lambda:InvokeFunction, Glue:StartJobRun/GetJobRun, Redshift-data:ExecuteStatement, SQS:SendMessage (DLQ), SNS:Publish |
| `eventbridge` | EventBridge scheduler | StepFunctions:StartExecution |

---
