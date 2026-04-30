#!/usr/bin/env bash
# Prepare the test environment for the Glue transform job.
#
# Run this after: cd terraform/test-glue && terraform apply -auto-approve
#
# What it does:
#   1. Reads bucket names from Terraform outputs
#   2. Builds utils.zip
#   3. Uploads transform_job.py and utils.zip to the scripts bucket
#   4. Seeds one raw tag JSON file to s3://<raw-bucket>/raw/test-run-001/
#
# After this script completes, trigger the job manually in the Glue console.
# Set job parameter --run_id = test-run-001
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
TF_DIR="${PROJECT_ROOT}/terraform/test-glue"

# ---------------------------------------------------------------------------
# Read Terraform outputs
# ---------------------------------------------------------------------------

echo "Reading Terraform outputs..."
cd "${TF_DIR}"

RAW_BUCKET=$(terraform output -raw raw_bucket_name)
SCRIPTS_BUCKET=$(terraform output -raw scripts_bucket_name)
GLUE_JOB=$(terraform output -raw glue_job_name)

cd "${PROJECT_ROOT}"

echo "  Raw bucket    : ${RAW_BUCKET}"
echo "  Scripts bucket: ${SCRIPTS_BUCKET}"
echo "  Glue job      : ${GLUE_JOB}"
echo ""

# ---------------------------------------------------------------------------
# Build utils.zip
# ---------------------------------------------------------------------------

echo "Building utils.zip..."
bash "${SCRIPT_DIR}/build_glue_zip.sh"
echo ""

# ---------------------------------------------------------------------------
# Upload Glue script and utils.zip
# ---------------------------------------------------------------------------

echo "Uploading transform_job.py..."
aws s3 cp \
  "${PROJECT_ROOT}/glue_jobs/scripts/transform_job.py" \
  "s3://${SCRIPTS_BUCKET}/scripts/transform_job.py"

echo "Uploading utils.zip..."
aws s3 cp \
  "${PROJECT_ROOT}/glue_jobs/utils.zip" \
  "s3://${SCRIPTS_BUCKET}/scripts/utils.zip"

echo ""

# ---------------------------------------------------------------------------
# Seed raw test data
# ---------------------------------------------------------------------------

echo "Seeding raw test data to s3://${RAW_BUCKET}/raw/test-run-001/TAG-001.json..."

aws s3 cp - "s3://${RAW_BUCKET}/raw/test-run-001/TAG-001.json" <<'EOF'
{
  "tag": {
    "TagID": "TAG-001", "TagName": "Pressure Sensor A",
    "UnitOfMeasure": "PSI", "EquipmentID": "EQ-001", "LocationID": "LOC-001"
  },
  "equipment": {
    "EquipmentID": "EQ-001", "EquipmentName": "Pump A",
    "EquipmentType": "Centrifugal Pump", "Manufacturer": "Grundfos", "InstallDate": "2020-01-15"
  },
  "location": {
    "LocationID": "LOC-001", "SiteName": "Plant 1",
    "Area": "Zone A", "GPSCoordinates": "40.7128,-74.0060"
  },
  "customer": {
    "CustomerID": "CUST-001", "CustomerName": "Acme Corp",
    "Industry": "Oil & Gas", "ContactInfo": "ops@acme.com", "Region": "US-East"
  },
  "measurements": [
    {"MeasurementID": "M-001", "TagID": "TAG-001", "Timestamp": "2024-01-15T10:00:00", "Value": 42.5, "QualityFlag": "good"},
    {"MeasurementID": "M-002", "TagID": "TAG-001", "Timestamp": "2024-01-15T10:05:00", "Value": 43.1, "QualityFlag": "good"},
    {"MeasurementID": "M-003", "TagID": "TAG-001", "Timestamp": "2024-01-16T08:00:00", "Value": 40.2, "QualityFlag": "uncertain"}
  ],
  "alarms": [
    {"AlarmID": "AL-001", "TagID": "TAG-001", "AlarmType": "HIGH_PRESSURE", "ThresholdValue": 50.0, "Timestamp": "2024-01-15T11:00:00", "Status": "active"}
  ],
  "events": [
    {"EventID": "EV-001", "TagID": "TAG-001", "EventType": "CALIBRATION", "Timestamp": "2024-01-10T09:00:00", "Notes": "Annual calibration"}
  ],
  "maintenance": [
    {"MaintenanceID": "MN-001", "TagID": "TAG-001", "WorkOrderID": "WO-101", "MaintenanceDate": "2024-01-10", "ActionTaken": "Seal replacement", "Technician": "J. Smith"}
  ],
  "contracts": [
    {"ContractID": "CT-001", "CustomerID": "CUST-001", "TagID": "TAG-001", "ContractStartDate": "2024-01-01", "ContractEndDate": "2024-12-31", "ContractVolume": 10000.0, "PricePerUnit": 1.25}
  ],
  "billing": [
    {"BillingID": "BL-001", "TagID": "TAG-001", "CustomerID": "CUST-001", "BillingPeriod": "2024-01", "ConsumptionVolume": 850.5, "TotalAmount": 1063.13, "PaymentStatus": "paid"}
  ],
  "inventory": [
    {"InventoryID": "IV-001", "TagID": "TAG-001", "MaterialType": "Seal Kit", "Quantity": 5, "StorageLocation": "Warehouse A", "LastUpdated": "2024-01-10T09:00:00"}
  ],
  "regulatory_compliance": [
    {"ComplianceID": "RC-001", "TagID": "TAG-001", "RegulationName": "ISO 9001", "InspectionDate": "2024-01-05", "Status": "compliant", "Inspector": "EPA Inspector"}
  ],
  "financial_forecasts": [
    {"ForecastID": "FF-001", "TagID": "TAG-001", "ForecastDate": "2024-02-01", "ExpectedConsumption": 900.0, "ExpectedRevenue": 1125.0, "RiskFactor": 0.05}
  ]
}
EOF

echo ""
echo "========================================"
echo " Setup complete."
echo ""
echo " Next: open the Glue console and run:"
echo "   Job name : ${GLUE_JOB}"
echo "   --run_id : test-run-001"
echo ""
echo " After the job succeeds, verify output:"
echo "   aws s3 ls s3://$(terraform -C ${TF_DIR} output -raw cleaned_bucket_name 2>/dev/null || echo '<cleaned-bucket>')/cleaned/test-run-001/"
echo "========================================"
