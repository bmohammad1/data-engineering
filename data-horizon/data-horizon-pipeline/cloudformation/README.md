# Data Horizon Pipeline — CloudFormation IaC

CloudFormation alternative to the Terraform infrastructure under `../terraform/`. Creates **identical** AWS resources. Terraform remains the primary IaC; this CFN stack is an additive alternative.

## Layout

```
parent-foundation.yaml      # foundation stack: VPC, IAM, S3, DynamoDB, SQS/SNS, SSM
parent-app.yaml             # app stack: Redshift, Glue, Lambda, Step Functions, EventBridge, alarms
nested/                     # one nested template per terraform module
params/{dev,staging,prod}.json
scripts/
  full-deploy.sh            # single end-to-end deploy (use this)
  bootstrap.sh              # seeds SecureString SSM params (called by full-deploy.sh)
  deploy.sh                 # renders ASL + deploys stacks (called by full-deploy.sh)
  render-stepfunctions.py   # inlines ../statemachine/*.asl.json into stepfunctions.yaml
```

## Prerequisites

- AWS CLI v2 configured (`aws sts get-caller-identity` must succeed)
- Python 3.12+ with `pip`
- `zip` available on PATH (used to build Lambda zips and Glue utils.zip)

## Scope

`full-deploy.sh` deploys **only the main pipeline** (this folder). The mock source API is a separate stack — deploy it independently with `../../source-mock-api/cloudformation/scripts/deploy.sh dev`. The pipeline's `SourceApiBaseUrl` parameter in `params/<env>.json` must point at a running source API (the mock one or a real one) before the pipeline can run end-to-end.

## Deploy (one command)

```bash
./scripts/full-deploy.sh dev
```

That's it. The script handles everything for the main pipeline:

1. Checks prerequisites and AWS credentials.
2. Builds Lambda zips if missing or stale.
3. Builds Glue `utils.zip` if missing.
4. Seeds the 2 SecureString SSM params (`source-api-token`, `redshift-master-password`) if absent — prompts you silently for each value the first time, then skips on re-runs.
5. Renders the Step Functions template from the ASL JSON files.
6. Deploys the foundation stack (VPC, S3, IAM, DynamoDB, SQS/SNS, SSM, Glue Catalog).
7. Uploads Glue scripts and Lambda zips to the buckets the foundation created.
8. Deploys the app stack (Redshift, Glue jobs, Lambda, Step Functions, EventBridge, alarms).
9. Prints a ready-to-paste `aws stepfunctions start-execution` command for a test run.

### Re-runs

Safe and incremental:
- Lambda zips rebuilt only when source files are newer.
- SSM secrets skipped if already present.
- CloudFormation change sets — no-op when nothing changed, incremental update otherwise.


### Forcing a secret rotation

```bash
FORCE_RESEED=1 ./scripts/full-deploy.sh dev
```


## End-to-end run

The order below brings up the mock source API, deploys the pipeline, prepares Redshift, seeds the source config, and triggers the first pipeline run.

### 1. Deploy the mock source API

The pipeline needs a running source API before it can extract anything. Deploy it first and copy the printed `ApiUrl` and access token.

```bash
cd ../../source-mock-api/cloudformation
./scripts/deploy.sh dev
```

Then update `cloudformation/params/dev.json` in this folder so `SourceApiBaseUrl` matches the printed `ApiUrl`. The deploy also prints an access token (24-hour TTL) — supply it on the next step via the `SOURCE_API_TOKEN` env var.

### 2. Deploy the pipeline

```bash
cd ../../data-horizon-pipeline/cloudformation
SOURCE_API_TOKEN="<token-from-step-1>" ./scripts/full-deploy.sh dev
```

Provisions the foundation + app stacks. On first run it prompts (silently) for any missing secrets; subsequent runs reuse what's in SSM. See `params/dev.json` for tunables like `RedshiftMasterPassword`.

### 3. Run Redshift migrations

Creates the target schema and tables in the Redshift cluster:

```bash
cd ..
bash scripts/run_redshift_migrations.sh dev
```

Applies every `.sql` file under `redshift/migrations/` in order via the Redshift Data API.

### 4. Upload the source config to the orchestration bucket

The pipeline's Config Loader reads tag/source definitions from `s3://<config-bucket>/source_config/`. Upload `config/source_config/` to that key in the config bucket (the bucket name is in the foundation stack's `ConfigBucketName` output).

### 5. Start the parent Step Function

Grab the parent state-machine ARN from the app stack's `ParentStateMachineArn` output and start an execution with input `{"startFrom":"config_loader"}`. Watch progress in the AWS Console → Step Functions, or tail CloudWatch Logs for the orchestrator Lambda and the Glue jobs. End state: Parquet in the `validated` bucket and rows loaded into Redshift staging tables.

