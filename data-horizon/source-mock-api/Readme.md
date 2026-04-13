# Source MOCK API

Stateless FastAPI application deployed on AWS Lambda that returns randomly generated, FK-consistent industrial data for a given tag ID. Static reference data (tags, equipment, locations, customers) is built deterministically at startup, and per-request data (measurements, alarms, maintenance, etc.) is generated randomly on each call.

## Tech Stack

- **Language:** Python 3.12
- **Framework:** FastAPI + Pydantic v2
- **Deployment:** AWS Lambda via Mangum
- **Infrastructure:** Terraform (API Gateway HTTP API, Lambda, Cognito)
- **Auth:** AWS Cognito OAuth2 `client_credentials` flow

## Project Structure

```
source-mock-api/
├── app/
│   ├── __init__.py
│   ├── main.py              # FastAPI routes
│   ├── models.py            # Pydantic response models
│   ├── data_generator.py    # Random data generators
│   └── static_data.py       # Static tags, equipment, locations, customers
├── terraform/
│   ├── providers.tf          # AWS provider + backend config
│   ├── variables.tf          # Input variables
│   ├── main.tf               # Lambda, API Gateway, Cognito resources
│   ├── outputs.tf            # API URL, Cognito client ID
│   └── terraform.tfvars.example
├── lambda_handler.py         # Mangum adapter for Lambda
├── build.sh                  # Builds Lambda zip package
└── requirements.txt
```

## Available Endpoints

| Method | Path             | Auth     | Description                        |
|--------|------------------|----------|------------------------------------|
| GET    | `/tags`          | Cognito  | List all 5,000 available tag IDs   |
| GET    | `/tag/{tag_id}`  | Cognito  | Full tag data + all related records|

Valid tag IDs: `TAG-00001` through `TAG-05000`

### Response Structure (`/tag/{tag_id}`)

A single tag response includes ~13,100 records across 13 related tables:

| Field                    | Type              | Count |
|--------------------------|-------------------|-------|
| `tag`                    | Tag               | 1     |
| `equipment`              | Equipment         | 1     |
| `location`               | Location          | 1     |
| `customer`               | Customer          | 1     |
| `measurements`           | Measurement[]     | 4,000 |
| `alarms`                 | Alarm[]           | 2,000 |
| `maintenance`            | Maintenance[]     | 1,500 |
| `events`                 | Event[]           | 2,000 |
| `contracts`              | CustomerContract[]| 800   |
| `billing`                | Billing[]         | 800   |
| `inventory`              | Inventory[]       | 600   |
| `regulatory_compliance`  | RegulatoryCompliance[] | 600 |
| `financial_forecasts`    | FinancialForecast[] | 700 |

Static data (tag, equipment, location, customer) is deterministic — same values every request. Generated data (measurements, alarms, etc.) is random per-request with FK consistency enforced via `tag_id` and `customer_id`.

## Local Development

```bash
pip install -r requirements.txt
pip install uvicorn
uvicorn app.main:app --reload --port 8000
```

Test: `GET http://localhost:8000/tag/TAG-00001`

## Deployment

### Prerequisites

- AWS CLI configured
- Terraform >= 1.5
- Python 3.12

### 1. Build Lambda Zip

```bash
bash build.sh
# Creates build/lambda.zip
```

### 2. Configure Variables

```bash
cd terraform
cp terraform.tfvars.example terraform.tfvars
# Edit terraform.tfvars with your values
```

### 3. Provision Infrastructure

```bash
cd terraform
terraform init
terraform plan
terraform apply
```

### Terraform Outputs

After `terraform apply`, the following outputs are available:

| Output                  | Description                                      |
|-------------------------|--------------------------------------------------|
| `api_url`               | API Gateway base URL                             |
| `lambda_function_name`  | Lambda function name                             |
| `cognito_user_pool_id`  | Cognito User Pool ID                             |
| `cognito_token_url`     | Cognito token endpoint for `client_credentials`  |
| `cognito_custom_scope`  | Custom scope to assign to your app client        |
| `cognito_client_id`     | Cognito app client ID for M2M authentication     |
| `cognito_client_secret` | Cognito app client secret (marked sensitive)     |

The `cognito_client_secret` is marked as `sensitive` in Terraform, so it displays as `<sensitive>` in the CLI to prevent accidental exposure in terminal logs or CI output. To retrieve the actual value:

```bash
terraform output -raw cognito_client_secret
```

### 4. Create Cognito App Client (Manual)

1. Get the User Pool ID and custom scope from Terraform:
   ```bash
   cd terraform
   terraform output cognito_user_pool_id
   terraform output cognito_custom_scope   # mock-source-api/read
   ```
2. Create an app client in the AWS Console or CLI with:
   - **OAuth flow**: `client_credentials`
   - **Custom scope**: `mock-source-api/read`
   - **Generate client secret**: Yes

### 5. Get Cognito M2M Token

```bash
TOKEN_URL=$(cd terraform && terraform output -raw cognito_token_url)

curl -X POST $TOKEN_URL \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "grant_type=client_credentials&client_id=<CLIENT_ID>&client_secret=<CLIENT_SECRET>&scope=mock-source-api/read"
```

### 6. Call the API

```bash
API_URL=$(cd terraform && terraform output -raw api_url)

curl -H "Authorization: Bearer <ACCESS_TOKEN>" \
  $API_URL/tag/TAG-00001
```

### Redeploy After Code Changes

```bash
bash build.sh
cd terraform && terraform apply
```

### Tear Down

```bash
cd terraform
terraform destroy
```

## Performance
- Memory: 256 MB
- Timeout: 30 s
- Data is generated in-memory
