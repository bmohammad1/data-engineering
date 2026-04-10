# Data Horizon

Monorepo for the Data Horizon platform — an end-to-end industrial/IoT data ingestion and analytics system.

## Projects

| Project | Description |
|---------|-------------|
| [source-mock-api](source-mock-api/) | Stateless mock API returning randomly generated, FK-consistent industrial/IoT data. Deployed as AWS Lambda behind API Gateway with Cognito auth. |
| [data-horizon-pipeline](data-horizon-pipeline/) | AWS data pipeline that ingests data from the source API via Step Functions, transforms with Glue (PySpark), and loads into Redshift for analytics. |

## Architecture

```
EventBridge (6h schedule)
    └── Step Functions
            ├── Lambda Orchestrator → loads config, generates Map State input
            ├── Map State (Lambda) → calls source-mock-api, writes raw JSON to S3
            ├── Glue Transform → raw JSON → cleaned JSON
            ├── Glue Validation → cleaned → Parquet (or bad bucket)
            └── Redshift COPY → Parquet → analytics tables
```

## Getting Started

Each project has its own setup instructions:

- **source-mock-api**: See [source-mock-api/Readme.md](source-mock-api/Readme.md)
- **data-horizon-pipeline**: See [data-horizon-pipeline/README.md](data-horizon-pipeline/README.md)

## Prerequisites

- Python 3.12+
- AWS CLI configured with valid credentials
- Terraform >= 1.1.5
- uv (for linting/testing tooling)
