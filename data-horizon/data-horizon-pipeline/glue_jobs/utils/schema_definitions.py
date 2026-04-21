"""Explicit Spark StructType schemas for all 13 domain tables.

These schemas are the single source of truth for column names and types
across the transform, validation, and Glue Data Catalog layers.
"""

from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

TAG_SCHEMA = StructType([
    StructField("TagID",         StringType(), nullable=False),
    StructField("TagName",       StringType(), nullable=True),
    StructField("Description",   StringType(), nullable=True),
    StructField("UnitOfMeasure", StringType(), nullable=True),
    StructField("EquipmentID",   StringType(), nullable=True),
    StructField("LocationID",    StringType(), nullable=True),
])

EQUIPMENT_SCHEMA = StructType([
    StructField("EquipmentID",   StringType(), nullable=False),
    StructField("EquipmentName", StringType(), nullable=True),
    StructField("EquipmentType", StringType(), nullable=True),
    StructField("Manufacturer",  StringType(), nullable=True),
    StructField("InstallDate",   DateType(),   nullable=True),
])

LOCATION_SCHEMA = StructType([
    StructField("LocationID",     StringType(), nullable=False),
    StructField("SiteName",       StringType(), nullable=True),
    StructField("Area",           StringType(), nullable=True),
    StructField("GPSCoordinates", StringType(), nullable=True),
])

CUSTOMER_SCHEMA = StructType([
    StructField("CustomerID",   StringType(), nullable=False),
    StructField("CustomerName", StringType(), nullable=True),
    StructField("Industry",     StringType(), nullable=True),
    StructField("ContactInfo",  StringType(), nullable=True),
    StructField("Region",       StringType(), nullable=True),
])

MEASUREMENTS_SCHEMA = StructType([
    StructField("MeasurementID", StringType(),    nullable=False),
    StructField("TagID",         StringType(),    nullable=False),
    StructField("Timestamp",     TimestampType(), nullable=True),
    StructField("Value",         DoubleType(),    nullable=True),
    StructField("QualityFlag",   StringType(),    nullable=True),
])

ALARMS_SCHEMA = StructType([
    StructField("AlarmID",        StringType(),    nullable=False),
    StructField("TagID",          StringType(),    nullable=False),
    StructField("AlarmType",      StringType(),    nullable=True),
    StructField("ThresholdValue", DoubleType(),    nullable=True),
    StructField("Timestamp",      TimestampType(), nullable=True),
    StructField("Status",         StringType(),    nullable=True),
])

MAINTENANCE_SCHEMA = StructType([
    StructField("MaintenanceID",   StringType(), nullable=False),
    StructField("TagID",           StringType(), nullable=False),
    StructField("WorkOrderID",     StringType(), nullable=True),
    StructField("MaintenanceDate", DateType(),   nullable=True),
    StructField("ActionTaken",     StringType(), nullable=True),
    StructField("Technician",      StringType(), nullable=True),
])

EVENTS_SCHEMA = StructType([
    StructField("EventID",    StringType(),    nullable=False),
    StructField("TagID",      StringType(),    nullable=False),
    StructField("EventType",  StringType(),    nullable=True),
    StructField("Timestamp",  TimestampType(), nullable=True),
    StructField("Notes",      StringType(),    nullable=True),
])

CONTRACTS_SCHEMA = StructType([
    StructField("ContractID",        StringType(), nullable=False),
    StructField("CustomerID",        StringType(), nullable=False),
    StructField("TagID",             StringType(), nullable=False),
    StructField("ContractStartDate", DateType(),   nullable=True),
    StructField("ContractEndDate",   DateType(),   nullable=True),
    StructField("ContractVolume",    DoubleType(), nullable=True),
    StructField("PricePerUnit",      DoubleType(), nullable=True),
])

BILLING_SCHEMA = StructType([
    StructField("BillingID",          StringType(), nullable=False),
    StructField("TagID",              StringType(), nullable=False),
    StructField("CustomerID",         StringType(), nullable=False),
    StructField("BillingPeriod",      StringType(), nullable=True),
    StructField("ConsumptionVolume",  DoubleType(), nullable=True),
    StructField("TotalAmount",        DoubleType(), nullable=True),
    StructField("PaymentStatus",      StringType(), nullable=True),
])

INVENTORY_SCHEMA = StructType([
    StructField("InventoryID",      StringType(),    nullable=False),
    StructField("TagID",            StringType(),    nullable=False),
    StructField("MaterialType",     StringType(),    nullable=True),
    StructField("Quantity",         IntegerType(),   nullable=True),
    StructField("StorageLocation",  StringType(),    nullable=True),
    StructField("LastUpdated",      TimestampType(), nullable=True),
])

REGULATORY_COMPLIANCE_SCHEMA = StructType([
    StructField("ComplianceID",     StringType(), nullable=False),
    StructField("TagID",            StringType(), nullable=False),
    StructField("RegulationName",   StringType(), nullable=True),
    StructField("InspectionDate",   DateType(),   nullable=True),
    StructField("Status",           StringType(), nullable=True),
    StructField("Inspector",        StringType(), nullable=True),
])

FINANCIAL_FORECASTS_SCHEMA = StructType([
    StructField("ForecastID",           StringType(), nullable=False),
    StructField("TagID",                StringType(), nullable=False),
    StructField("ForecastDate",         DateType(),   nullable=True),
    StructField("ExpectedConsumption",  DoubleType(), nullable=True),
    StructField("ExpectedRevenue",      DoubleType(), nullable=True),
    StructField("RiskFactor",           DoubleType(), nullable=True),
])

# Maps the JSON key in the raw TagResponse to its Spark schema.
# The key must exactly match the field name in the API response.
TABLE_SCHEMAS: dict[str, StructType] = {
    "tag":                   TAG_SCHEMA,
    "equipment":             EQUIPMENT_SCHEMA,
    "location":              LOCATION_SCHEMA,
    "customer":              CUSTOMER_SCHEMA,
    "measurements":          MEASUREMENTS_SCHEMA,
    "alarms":                ALARMS_SCHEMA,
    "maintenance":           MAINTENANCE_SCHEMA,
    "events":                EVENTS_SCHEMA,
    "contracts":             CONTRACTS_SCHEMA,
    "billing":               BILLING_SCHEMA,
    "inventory":             INVENTORY_SCHEMA,
    "regulatory_compliance": REGULATORY_COMPLIANCE_SCHEMA,
    "financial_forecasts":   FINANCIAL_FORECASTS_SCHEMA,
}

# Tables whose top-level JSON key holds a list (versus a single object).
LIST_TABLES = {
    "measurements",
    "alarms",
    "maintenance",
    "events",
    "contracts",
    "billing",
    "inventory",
    "regulatory_compliance",
    "financial_forecasts",
}

# Primary key column for each table — used for deduplication.
PRIMARY_KEYS: dict[str, str] = {
    "tag":                   "TagID",
    "equipment":             "EquipmentID",
    "location":              "LocationID",
    "customer":              "CustomerID",
    "measurements":          "MeasurementID",
    "alarms":                "AlarmID",
    "maintenance":           "MaintenanceID",
    "events":                "EventID",
    "contracts":             "ContractID",
    "billing":               "BillingID",
    "inventory":             "InventoryID",
    "regulatory_compliance": "ComplianceID",
    "financial_forecasts":   "ForecastID",
}
