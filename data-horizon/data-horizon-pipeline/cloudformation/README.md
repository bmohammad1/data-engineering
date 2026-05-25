# Data Horizon Pipeline — CloudFormation IaC

CloudFormation alternative to the Terraform infrastructure under `../terraform/`. Creates **identical** AWS resources. Terraform remains the primary IaC; this CFN stack is an additive alternative.

## Layout

```
parent-foundation.yaml      # foundation stack: VPC, IAM, S3, DynamoDB, SQS/SNS, SSM
parent-app.yaml             # app stack: Redshift, Glue, Lambda, Step Functions, EventBridge, alarms
nested/                     # one nested template per terraform module
params/{dev,staging,prod}.json
samconfig.toml              # SAM CLI per-env config
scripts/
  full-deploy.sh            # single end-to-end deploy (use this)
  bootstrap.sh              # seeds SecureString SSM params (called by full-deploy.sh)
  deploy.sh                 # renders ASL + deploys stacks (called by full-deploy.sh)
  render-stepfunctions.py   # inlines ../statemachine/*.asl.json into stepfunctions.yaml
```

## Prerequisites

- AWS CLI v2 configured (`aws sts get-caller-identity` must succeed)
- AWS SAM CLI installed (`sam --version`)
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

### Providing secrets non-interactively

```bash
SOURCE_API_TOKEN="..." REDSHIFT_MASTER_PASSWORD="..." ./scripts/full-deploy.sh dev
```

### Forcing a secret rotation

```bash
FORCE_RESEED=1 ./scripts/full-deploy.sh dev
```

## What it does under the hood

If you want to run the individual phases manually:

```bash
./scripts/bootstrap.sh dev --source-api-token "..." --redshift-master-password "..."
python scripts/render-stepfunctions.py
./scripts/deploy.sh dev
```

## CFN-vs-Terraform notes

1. **SecureString SSM params** — CFN cannot create them. The bootstrap step (inside `full-deploy.sh`) seeds them via `aws ssm put-parameter`.
2. **Account suffix** in S3 bucket names — computed in the deploy script and passed as a parameter (CFN has no string-slice function).
3. **Two-pass deploy** — foundation creates the scripts bucket; deploy script then uploads Glue scripts and Lambda zips; app stack references them.
4. **Step Function ASL** — `render-stepfunctions.py` inlines the ASL JSON into the YAML via `Fn::Sub` (mirrors terraform's `templatefile()`).

Everything else is byte-equivalent to terraform.
