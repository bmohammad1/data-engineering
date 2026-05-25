# Mock Source API — CloudFormation IaC

CloudFormation alternative to the Terraform infrastructure under `../terraform/`. Creates identical AWS resources. Terraform remains the primary IaC; this CFN stack is an additive alternative.

## Layout

```
parent.yaml                # orchestrator
nested/
  cognito.yaml             # User pool + domain + resource server + M2M app client
  lambda.yaml              # Lambda function + IAM role + log group
  apigateway.yaml          # Regional REST API + Cognito authorizer + CORS + JSON access logs
params/{dev,staging,prod}.json
scripts/
  deploy.sh                # one-script end-to-end deploy (use this)
```

## Prerequisites

- AWS CLI v2 configured (`aws sts get-caller-identity` must succeed)
- AWS SAM CLI installed (`sam --version`)
- Python 3.12+

## Deploy (one command)

```bash
./scripts/deploy.sh dev
```

The script handles everything:

1. Checks prerequisites and AWS credentials.
2. Builds the Lambda zip via `../build.sh` if missing or stale (installs Linux-compatible deps, copies app code).
3. Creates the artifact S3 bucket (`mock-source-api-artifacts-<account>-<region>`) if missing.
4. Uploads the Lambda zip with a content-hashed key (no re-upload when nothing changed).
5. Runs `sam deploy` against `parent.yaml`, creating:
   - Cognito user pool + domain + M2M app client (`client_credentials` flow, scope `mock-source-api/read`)
   - Lambda function (`mock-source-api-<env>`, python3.12) + IAM role + log group
   - Regional REST API with Cognito authorizer on `GET /tags` and `GET /tag/{tag_id}`, plus CORS `OPTIONS`, plus JSON access logs
6. Prints stack outputs and a ready-to-paste `curl` test command (with the Cognito client secret already fetched).

### Re-runs

Safe and incremental:
- Lambda zip rebuilt only if source files are newer.
- Artifact bucket creation is idempotent.
- `sam deploy` uses change sets — no-op when nothing changed.

## Outputs

After deploy the stack reports:
- `ApiUrl` — base URL of the REST API
- `CognitoTokenUrl` — OAuth2 token endpoint
- `CognitoClientId` / `CognitoCustomScope` — `client_credentials` inputs
- `CognitoUserPoolId`, `LambdaFunctionName`

The Cognito client secret is fetched automatically by the deploy script and printed inside the test command.

## Wiring into the main pipeline

Once deployed, copy `ApiUrl` into `data-horizon-pipeline/cloudformation/params/<env>.json` as `SourceApiBaseUrl`. Get a token (see the printed test command), then pass it to the pipeline deploy:

```bash
SOURCE_API_TOKEN="<bearer-token>" ../../data-horizon-pipeline/cloudformation/scripts/full-deploy.sh dev
```
