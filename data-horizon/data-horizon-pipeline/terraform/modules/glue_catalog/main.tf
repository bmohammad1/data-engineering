locals {
  db_name = replace("${var.name_prefix}_db", "-", "_")
}

resource "aws_glue_catalog_database" "data_horizon" {
  name        = local.db_name
  description = "Data Horizon pipeline catalog database"
}

# =============================================================================
# Raw data — generic envelope table (one JSON object per tag per run)
# =============================================================================

resource "aws_glue_catalog_table" "raw_data" {
  database_name = aws_glue_catalog_database.data_horizon.name
  name          = "raw_data"
  description   = "Raw TagResponse JSON files, one object per tag per run"
  table_type    = "EXTERNAL_TABLE"
  parameters    = { classification = "json" }

  storage_descriptor {
    location      = "s3://${var.raw_bucket_name}/raw/"
    input_format  = "org.apache.hadoop.mapred.TextInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
    ser_de_info {
      serialization_library = "org.openx.data.jsonserde.JsonSerDe"
    }
    columns { name = "run_id";   type = "string" }
    columns { name = "tag_id";   type = "string" }
    columns { name = "data";     type = "string" }
  }
}

# =============================================================================
# Cleaned tables — JSON, one folder per domain table per run
# =============================================================================

resource "aws_glue_catalog_table" "cleaned_measurements" {
  database_name = aws_glue_catalog_database.data_horizon.name
  name          = "cleaned_measurements"
  description   = "Cleaned measurement records (JSON)"
  table_type    = "EXTERNAL_TABLE"
  parameters    = { classification = "json" }

  storage_descriptor {
    location      = "s3://${var.cleaned_bucket_name}/cleaned/"
    input_format  = "org.apache.hadoop.mapred.TextInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
    ser_de_info { serialization_library = "org.openx.data.jsonserde.JsonSerDe" }
    columns { name = "MeasurementID"; type = "string" }
    columns { name = "TagID";         type = "string" }
    columns { name = "Timestamp";     type = "timestamp" }
    columns { name = "Value";         type = "double" }
    columns { name = "QualityFlag";   type = "string" }
    columns { name = "_run_id";       type = "string" }
    columns { name = "_ingested_at";  type = "timestamp" }
  }
}

resource "aws_glue_catalog_table" "cleaned_alarms" {
  database_name = aws_glue_catalog_database.data_horizon.name
  name          = "cleaned_alarms"
  description   = "Cleaned alarm records (JSON)"
  table_type    = "EXTERNAL_TABLE"
  parameters    = { classification = "json" }

  storage_descriptor {
    location      = "s3://${var.cleaned_bucket_name}/cleaned/"
    input_format  = "org.apache.hadoop.mapred.TextInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
    ser_de_info { serialization_library = "org.openx.data.jsonserde.JsonSerDe" }
    columns { name = "AlarmID";        type = "string" }
    columns { name = "TagID";          type = "string" }
    columns { name = "AlarmType";      type = "string" }
    columns { name = "ThresholdValue"; type = "double" }
    columns { name = "Timestamp";      type = "timestamp" }
    columns { name = "Status";         type = "string" }
    columns { name = "_run_id";        type = "string" }
    columns { name = "_ingested_at";   type = "timestamp" }
  }
}

resource "aws_glue_catalog_table" "cleaned_maintenance" {
  database_name = aws_glue_catalog_database.data_horizon.name
  name          = "cleaned_maintenance"
  description   = "Cleaned maintenance records (JSON)"
  table_type    = "EXTERNAL_TABLE"
  parameters    = { classification = "json" }

  storage_descriptor {
    location      = "s3://${var.cleaned_bucket_name}/cleaned/"
    input_format  = "org.apache.hadoop.mapred.TextInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
    ser_de_info { serialization_library = "org.openx.data.jsonserde.JsonSerDe" }
    columns { name = "MaintenanceID";   type = "string" }
    columns { name = "TagID";           type = "string" }
    columns { name = "WorkOrderID";     type = "string" }
    columns { name = "MaintenanceDate"; type = "date" }
    columns { name = "ActionTaken";     type = "string" }
    columns { name = "Technician";      type = "string" }
    columns { name = "_run_id";         type = "string" }
    columns { name = "_ingested_at";    type = "timestamp" }
  }
}

resource "aws_glue_catalog_table" "cleaned_events" {
  database_name = aws_glue_catalog_database.data_horizon.name
  name          = "cleaned_events"
  description   = "Cleaned event records (JSON)"
  table_type    = "EXTERNAL_TABLE"
  parameters    = { classification = "json" }

  storage_descriptor {
    location      = "s3://${var.cleaned_bucket_name}/cleaned/"
    input_format  = "org.apache.hadoop.mapred.TextInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
    ser_de_info { serialization_library = "org.openx.data.jsonserde.JsonSerDe" }
    columns { name = "EventID";      type = "string" }
    columns { name = "TagID";        type = "string" }
    columns { name = "EventType";    type = "string" }
    columns { name = "Timestamp";    type = "timestamp" }
    columns { name = "Notes";        type = "string" }
    columns { name = "_run_id";      type = "string" }
    columns { name = "_ingested_at"; type = "timestamp" }
  }
}

resource "aws_glue_catalog_table" "cleaned_billing" {
  database_name = aws_glue_catalog_database.data_horizon.name
  name          = "cleaned_billing"
  description   = "Cleaned billing records (JSON)"
  table_type    = "EXTERNAL_TABLE"
  parameters    = { classification = "json" }

  storage_descriptor {
    location      = "s3://${var.cleaned_bucket_name}/cleaned/"
    input_format  = "org.apache.hadoop.mapred.TextInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
    ser_de_info { serialization_library = "org.openx.data.jsonserde.JsonSerDe" }
    columns { name = "BillingID";         type = "string" }
    columns { name = "TagID";             type = "string" }
    columns { name = "CustomerID";        type = "string" }
    columns { name = "BillingPeriod";     type = "string" }
    columns { name = "ConsumptionVolume"; type = "double" }
    columns { name = "TotalAmount";       type = "double" }
    columns { name = "PaymentStatus";     type = "string" }
    columns { name = "_run_id";           type = "string" }
    columns { name = "_ingested_at";      type = "timestamp" }
  }
}

resource "aws_glue_catalog_table" "cleaned_contracts" {
  database_name = aws_glue_catalog_database.data_horizon.name
  name          = "cleaned_contracts"
  description   = "Cleaned contract records (JSON)"
  table_type    = "EXTERNAL_TABLE"
  parameters    = { classification = "json" }

  storage_descriptor {
    location      = "s3://${var.cleaned_bucket_name}/cleaned/"
    input_format  = "org.apache.hadoop.mapred.TextInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
    ser_de_info { serialization_library = "org.openx.data.jsonserde.JsonSerDe" }
    columns { name = "ContractID";        type = "string" }
    columns { name = "CustomerID";        type = "string" }
    columns { name = "TagID";             type = "string" }
    columns { name = "ContractStartDate"; type = "date" }
    columns { name = "ContractEndDate";   type = "date" }
    columns { name = "ContractVolume";    type = "double" }
    columns { name = "PricePerUnit";      type = "double" }
    columns { name = "_run_id";           type = "string" }
    columns { name = "_ingested_at";      type = "timestamp" }
  }
}

resource "aws_glue_catalog_table" "cleaned_inventory" {
  database_name = aws_glue_catalog_database.data_horizon.name
  name          = "cleaned_inventory"
  description   = "Cleaned inventory records (JSON)"
  table_type    = "EXTERNAL_TABLE"
  parameters    = { classification = "json" }

  storage_descriptor {
    location      = "s3://${var.cleaned_bucket_name}/cleaned/"
    input_format  = "org.apache.hadoop.mapred.TextInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
    ser_de_info { serialization_library = "org.openx.data.jsonserde.JsonSerDe" }
    columns { name = "InventoryID";     type = "string" }
    columns { name = "TagID";           type = "string" }
    columns { name = "MaterialType";    type = "string" }
    columns { name = "Quantity";        type = "int" }
    columns { name = "StorageLocation"; type = "string" }
    columns { name = "LastUpdated";     type = "timestamp" }
    columns { name = "_run_id";         type = "string" }
    columns { name = "_ingested_at";    type = "timestamp" }
  }
}

resource "aws_glue_catalog_table" "cleaned_regulatory_compliance" {
  database_name = aws_glue_catalog_database.data_horizon.name
  name          = "cleaned_regulatory_compliance"
  description   = "Cleaned regulatory compliance records (JSON)"
  table_type    = "EXTERNAL_TABLE"
  parameters    = { classification = "json" }

  storage_descriptor {
    location      = "s3://${var.cleaned_bucket_name}/cleaned/"
    input_format  = "org.apache.hadoop.mapred.TextInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
    ser_de_info { serialization_library = "org.openx.data.jsonserde.JsonSerDe" }
    columns { name = "ComplianceID";   type = "string" }
    columns { name = "TagID";          type = "string" }
    columns { name = "RegulationName"; type = "string" }
    columns { name = "InspectionDate"; type = "date" }
    columns { name = "Status";         type = "string" }
    columns { name = "Inspector";      type = "string" }
    columns { name = "_run_id";        type = "string" }
    columns { name = "_ingested_at";   type = "timestamp" }
  }
}

resource "aws_glue_catalog_table" "cleaned_financial_forecasts" {
  database_name = aws_glue_catalog_database.data_horizon.name
  name          = "cleaned_financial_forecasts"
  description   = "Cleaned financial forecast records (JSON)"
  table_type    = "EXTERNAL_TABLE"
  parameters    = { classification = "json" }

  storage_descriptor {
    location      = "s3://${var.cleaned_bucket_name}/cleaned/"
    input_format  = "org.apache.hadoop.mapred.TextInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
    ser_de_info { serialization_library = "org.openx.data.jsonserde.JsonSerDe" }
    columns { name = "ForecastID";          type = "string" }
    columns { name = "TagID";               type = "string" }
    columns { name = "ForecastDate";        type = "date" }
    columns { name = "ExpectedConsumption"; type = "double" }
    columns { name = "ExpectedRevenue";     type = "double" }
    columns { name = "RiskFactor";          type = "double" }
    columns { name = "_run_id";             type = "string" }
    columns { name = "_ingested_at";        type = "timestamp" }
  }
}

# =============================================================================
# Validated tables — Parquet (snappy), updated by the Glue job via catalog sink
# =============================================================================

resource "aws_glue_catalog_table" "validated_measurements" {
  database_name = aws_glue_catalog_database.data_horizon.name
  name          = "validated_measurements"
  description   = "Validated measurement records (Parquet)"
  table_type    = "EXTERNAL_TABLE"
  parameters = {
    classification      = "parquet"
    "parquet.compression" = "SNAPPY"
  }

  storage_descriptor {
    location      = "s3://${var.validated_bucket_name}/validated/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
      parameters            = { "serialization.format" = "1" }
    }
    columns { name = "MeasurementID"; type = "string" }
    columns { name = "TagID";         type = "string" }
    columns { name = "Timestamp";     type = "timestamp" }
    columns { name = "Value";         type = "double" }
    columns { name = "QualityFlag";   type = "string" }
    columns { name = "_run_id";       type = "string" }
    columns { name = "_ingested_at";  type = "timestamp" }
  }
}

resource "aws_glue_catalog_table" "validated_alarms" {
  database_name = aws_glue_catalog_database.data_horizon.name
  name          = "validated_alarms"
  description   = "Validated alarm records (Parquet)"
  table_type    = "EXTERNAL_TABLE"
  parameters = {
    classification      = "parquet"
    "parquet.compression" = "SNAPPY"
  }

  storage_descriptor {
    location      = "s3://${var.validated_bucket_name}/validated/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
      parameters            = { "serialization.format" = "1" }
    }
    columns { name = "AlarmID";        type = "string" }
    columns { name = "TagID";          type = "string" }
    columns { name = "AlarmType";      type = "string" }
    columns { name = "ThresholdValue"; type = "double" }
    columns { name = "Timestamp";      type = "timestamp" }
    columns { name = "Status";         type = "string" }
    columns { name = "_run_id";        type = "string" }
    columns { name = "_ingested_at";   type = "timestamp" }
  }
}

resource "aws_glue_catalog_table" "validated_maintenance" {
  database_name = aws_glue_catalog_database.data_horizon.name
  name          = "validated_maintenance"
  description   = "Validated maintenance records (Parquet)"
  table_type    = "EXTERNAL_TABLE"
  parameters = {
    classification      = "parquet"
    "parquet.compression" = "SNAPPY"
  }

  storage_descriptor {
    location      = "s3://${var.validated_bucket_name}/validated/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
      parameters            = { "serialization.format" = "1" }
    }
    columns { name = "MaintenanceID";   type = "string" }
    columns { name = "TagID";           type = "string" }
    columns { name = "WorkOrderID";     type = "string" }
    columns { name = "MaintenanceDate"; type = "date" }
    columns { name = "ActionTaken";     type = "string" }
    columns { name = "Technician";      type = "string" }
    columns { name = "_run_id";         type = "string" }
    columns { name = "_ingested_at";    type = "timestamp" }
  }
}

resource "aws_glue_catalog_table" "validated_events" {
  database_name = aws_glue_catalog_database.data_horizon.name
  name          = "validated_events"
  description   = "Validated event records (Parquet)"
  table_type    = "EXTERNAL_TABLE"
  parameters = {
    classification      = "parquet"
    "parquet.compression" = "SNAPPY"
  }

  storage_descriptor {
    location      = "s3://${var.validated_bucket_name}/validated/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
      parameters            = { "serialization.format" = "1" }
    }
    columns { name = "EventID";      type = "string" }
    columns { name = "TagID";        type = "string" }
    columns { name = "EventType";    type = "string" }
    columns { name = "Timestamp";    type = "timestamp" }
    columns { name = "Notes";        type = "string" }
    columns { name = "_run_id";      type = "string" }
    columns { name = "_ingested_at"; type = "timestamp" }
  }
}

resource "aws_glue_catalog_table" "validated_billing" {
  database_name = aws_glue_catalog_database.data_horizon.name
  name          = "validated_billing"
  description   = "Validated billing records (Parquet)"
  table_type    = "EXTERNAL_TABLE"
  parameters = {
    classification      = "parquet"
    "parquet.compression" = "SNAPPY"
  }

  storage_descriptor {
    location      = "s3://${var.validated_bucket_name}/validated/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
      parameters            = { "serialization.format" = "1" }
    }
    columns { name = "BillingID";         type = "string" }
    columns { name = "TagID";             type = "string" }
    columns { name = "CustomerID";        type = "string" }
    columns { name = "BillingPeriod";     type = "string" }
    columns { name = "ConsumptionVolume"; type = "double" }
    columns { name = "TotalAmount";       type = "double" }
    columns { name = "PaymentStatus";     type = "string" }
    columns { name = "_run_id";           type = "string" }
    columns { name = "_ingested_at";      type = "timestamp" }
  }
}

resource "aws_glue_catalog_table" "validated_contracts" {
  database_name = aws_glue_catalog_database.data_horizon.name
  name          = "validated_contracts"
  description   = "Validated contract records (Parquet)"
  table_type    = "EXTERNAL_TABLE"
  parameters = {
    classification      = "parquet"
    "parquet.compression" = "SNAPPY"
  }

  storage_descriptor {
    location      = "s3://${var.validated_bucket_name}/validated/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
      parameters            = { "serialization.format" = "1" }
    }
    columns { name = "ContractID";        type = "string" }
    columns { name = "CustomerID";        type = "string" }
    columns { name = "TagID";             type = "string" }
    columns { name = "ContractStartDate"; type = "date" }
    columns { name = "ContractEndDate";   type = "date" }
    columns { name = "ContractVolume";    type = "double" }
    columns { name = "PricePerUnit";      type = "double" }
    columns { name = "_run_id";           type = "string" }
    columns { name = "_ingested_at";      type = "timestamp" }
  }
}

resource "aws_glue_catalog_table" "validated_inventory" {
  database_name = aws_glue_catalog_database.data_horizon.name
  name          = "validated_inventory"
  description   = "Validated inventory records (Parquet)"
  table_type    = "EXTERNAL_TABLE"
  parameters = {
    classification      = "parquet"
    "parquet.compression" = "SNAPPY"
  }

  storage_descriptor {
    location      = "s3://${var.validated_bucket_name}/validated/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
      parameters            = { "serialization.format" = "1" }
    }
    columns { name = "InventoryID";     type = "string" }
    columns { name = "TagID";           type = "string" }
    columns { name = "MaterialType";    type = "string" }
    columns { name = "Quantity";        type = "int" }
    columns { name = "StorageLocation"; type = "string" }
    columns { name = "LastUpdated";     type = "timestamp" }
    columns { name = "_run_id";         type = "string" }
    columns { name = "_ingested_at";    type = "timestamp" }
  }
}

resource "aws_glue_catalog_table" "validated_regulatory_compliance" {
  database_name = aws_glue_catalog_database.data_horizon.name
  name          = "validated_regulatory_compliance"
  description   = "Validated regulatory compliance records (Parquet)"
  table_type    = "EXTERNAL_TABLE"
  parameters = {
    classification      = "parquet"
    "parquet.compression" = "SNAPPY"
  }

  storage_descriptor {
    location      = "s3://${var.validated_bucket_name}/validated/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
      parameters            = { "serialization.format" = "1" }
    }
    columns { name = "ComplianceID";   type = "string" }
    columns { name = "TagID";          type = "string" }
    columns { name = "RegulationName"; type = "string" }
    columns { name = "InspectionDate"; type = "date" }
    columns { name = "Status";         type = "string" }
    columns { name = "Inspector";      type = "string" }
    columns { name = "_run_id";        type = "string" }
    columns { name = "_ingested_at";   type = "timestamp" }
  }
}

resource "aws_glue_catalog_table" "validated_financial_forecasts" {
  database_name = aws_glue_catalog_database.data_horizon.name
  name          = "validated_financial_forecasts"
  description   = "Validated financial forecast records (Parquet)"
  table_type    = "EXTERNAL_TABLE"
  parameters = {
    classification      = "parquet"
    "parquet.compression" = "SNAPPY"
  }

  storage_descriptor {
    location      = "s3://${var.validated_bucket_name}/validated/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
      parameters            = { "serialization.format" = "1" }
    }
    columns { name = "ForecastID";          type = "string" }
    columns { name = "TagID";               type = "string" }
    columns { name = "ForecastDate";        type = "date" }
    columns { name = "ExpectedConsumption"; type = "double" }
    columns { name = "ExpectedRevenue";     type = "double" }
    columns { name = "RiskFactor";          type = "double" }
    columns { name = "_run_id";             type = "string" }
    columns { name = "_ingested_at";        type = "timestamp" }
  }
}

# =============================================================================
# Quarantine table — JSON, invalid records with _validation_errors column
# =============================================================================

resource "aws_glue_catalog_table" "quarantine_data" {
  database_name = aws_glue_catalog_database.data_horizon.name
  name          = "quarantine_data"
  description   = "Quarantined records that failed validation, with error details"
  table_type    = "EXTERNAL_TABLE"
  parameters    = { classification = "json" }

  storage_descriptor {
    location      = "s3://${var.bad_bucket_name}/quarantine/"
    input_format  = "org.apache.hadoop.mapred.TextInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
    ser_de_info { serialization_library = "org.openx.data.jsonserde.JsonSerDe" }
    columns { name = "_source_table";      type = "string" }
    columns { name = "_validation_errors"; type = "string" }
    columns { name = "_run_id";            type = "string" }
    columns { name = "_ingested_at";       type = "timestamp" }
  }
}
