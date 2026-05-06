"""Unit tests for apply_validation() — pure PySpark, no AWS dependencies."""

import json

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from glue_jobs.utils.validation_rules import apply_validation


# ---------------------------------------------------------------------------
# Schema definitions for each tested table
# ---------------------------------------------------------------------------

_MEASUREMENTS_SCHEMA = StructType([
    StructField("MeasurementID", StringType(),   True),
    StructField("TagID",         StringType(),   True),
    StructField("Timestamp",     TimestampType(), True),
    StructField("Value",         DoubleType(),   True),
    StructField("QualityFlag",   StringType(),   True),
])

_MAINTENANCE_SCHEMA = StructType([
    StructField("MaintenanceID",  StringType(), True),
    StructField("TagID",          StringType(), True),
    StructField("WorkOrderID",    StringType(), True),
    StructField("MaintenanceDate", DateType(),  True),
    StructField("ActionTaken",    StringType(), True),
    StructField("Technician",     StringType(), True),
])

_ALARMS_SCHEMA = StructType([
    StructField("AlarmID",        StringType(),   True),
    StructField("TagID",          StringType(),   True),
    StructField("AlarmType",      StringType(),   True),
    StructField("ThresholdValue", DoubleType(),   True),
    StructField("Timestamp",      TimestampType(), True),
    StructField("Status",         StringType(),   True),
])

_CONTRACTS_SCHEMA = StructType([
    StructField("ContractID",        StringType(), True),
    StructField("CustomerID",        StringType(), True),
    StructField("TagID",             StringType(), True),
    StructField("ContractStartDate", DateType(),   True),
    StructField("ContractEndDate",   DateType(),   True),
    StructField("ContractVolume",    DoubleType(), True),
    StructField("PricePerUnit",      DoubleType(), True),
])

_FINANCIAL_FORECASTS_SCHEMA = StructType([
    StructField("ForecastID",           StringType(), True),
    StructField("TagID",                StringType(), True),
    StructField("ForecastDate",         DateType(),   True),
    StructField("ExpectedConsumption",  DoubleType(), True),
    StructField("ExpectedRevenue",      DoubleType(), True),
    StructField("RiskFactor",           DoubleType(), True),
])


# ---------------------------------------------------------------------------
# DataFrame builder — avoids createDataFrame / cloudpickle entirely
# ---------------------------------------------------------------------------

def _make_df(spark, records: list[dict], schema: StructType):
    """Build a DataFrame from plain dicts using spark.sql + from_json.

    Dates and timestamps must be passed as ISO strings; from_json will cast
    them to the target type via the schema.
    """
    if not records:
        null_cols = ", ".join(
            f"CAST(NULL AS {_spark_type_sql(f.dataType)}) AS {f.name}"
            for f in schema.fields
        )
        return spark.sql(f"SELECT {null_cols} WHERE 1=0")

    parts = []
    for record in records:
        safe_json = json.dumps(record).replace("'", "''")
        df = (
            spark.sql(f"SELECT '{safe_json}' AS _raw")
            .select(F.from_json(F.col("_raw"), schema).alias("_d"))
            .select("_d.*")
        )
        parts.append(df)

    result = parts[0]
    for df in parts[1:]:
        result = result.unionByName(df)
    return result


def _spark_type_sql(data_type) -> str:
    """Map a PySpark DataType to the SQL type string used in CAST expressions."""
    mapping = {
        StringType():    "STRING",
        DoubleType():    "DOUBLE",
        TimestampType(): "TIMESTAMP",
        DateType():      "DATE",
    }
    return mapping.get(data_type, "STRING")


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestMeasurementsValidation:
    def test_valid_row_passes_all_rules(self, spark):
        df = _make_df(spark, [{
            "MeasurementID": "M-001",
            "TagID": "TAG-001",
            "Timestamp": "2024-01-01T12:00:00",
            "Value": 42.5,
            "QualityFlag": "GOOD",
        }], _MEASUREMENTS_SCHEMA)
        valid_df, invalid_df = apply_validation(df, "measurements")
        assert valid_df.count() == 1
        assert invalid_df.count() == 0

    def test_null_value_quarantined(self, spark):
        df = _make_df(spark, [{
            "MeasurementID": "M-002",
            "TagID": "TAG-001",
            "Timestamp": "2024-01-01T12:00:00",
            "Value": None,
            "QualityFlag": "GOOD",
        }], _MEASUREMENTS_SCHEMA)
        valid_df, invalid_df = apply_validation(df, "measurements")
        assert valid_df.count() == 0
        assert invalid_df.count() == 1
        errors = invalid_df.collect()[0]["_validation_errors"]
        assert "value_not_null" in errors

    def test_invalid_quality_flag_quarantined(self, spark):
        df = _make_df(spark, [{
            "MeasurementID": "M-003",
            "TagID": "TAG-001",
            "Timestamp": "2024-01-01T12:00:00",
            "Value": 10.0,
            "QualityFlag": "CORRUPTED",
        }], _MEASUREMENTS_SCHEMA)
        valid_df, invalid_df = apply_validation(df, "measurements")
        assert valid_df.count() == 0
        assert invalid_df.count() == 1
        errors = invalid_df.collect()[0]["_validation_errors"]
        assert "quality_flag_valid" in errors

    def test_multiple_rule_failures_combined_in_errors_column(self, spark):
        df = _make_df(spark, [{
            "MeasurementID": "M-004",
            "TagID": "TAG-001",
            "Timestamp": None,
            "Value": None,
            "QualityFlag": "CORRUPTED",
        }], _MEASUREMENTS_SCHEMA)
        valid_df, invalid_df = apply_validation(df, "measurements")
        assert valid_df.count() == 0
        errors = invalid_df.collect()[0]["_validation_errors"]
        assert "value_not_null" in errors
        assert "timestamp_not_null" in errors
        assert "quality_flag_valid" in errors
        assert "; " in errors


class TestMaintenanceValidation:
    def test_null_technician_quarantined(self, spark):
        df = _make_df(spark, [{
            "MaintenanceID":  "MX-001",
            "TagID":          "TAG-001",
            "WorkOrderID":    "WO-001",
            "MaintenanceDate": "2024-01-01",
            "ActionTaken":    "Replaced filter",
            "Technician":     None,
        }], _MAINTENANCE_SCHEMA)
        valid_df, invalid_df = apply_validation(df, "maintenance")
        assert valid_df.count() == 0
        errors = invalid_df.collect()[0]["_validation_errors"]
        assert "technician_required" in errors


class TestAlarmsValidation:
    def test_negative_threshold_quarantined(self, spark):
        df = _make_df(spark, [{
            "AlarmID":        "AL-001",
            "TagID":          "TAG-001",
            "AlarmType":      "HI",
            "ThresholdValue": -5.0,
            "Timestamp":      "2024-01-01T08:00:00",
            "Status":         "ACTIVE",
        }], _ALARMS_SCHEMA)
        valid_df, invalid_df = apply_validation(df, "alarms")
        assert valid_df.count() == 0
        errors = invalid_df.collect()[0]["_validation_errors"]
        assert "threshold_non_negative" in errors

    def test_invalid_alarm_status_quarantined(self, spark):
        df = _make_df(spark, [{
            "AlarmID":        "AL-002",
            "TagID":          "TAG-001",
            "AlarmType":      "HI",
            "ThresholdValue": 10.0,
            "Timestamp":      "2024-01-01T08:00:00",
            "Status":         "UNKNOWN_STATE",
        }], _ALARMS_SCHEMA)
        valid_df, invalid_df = apply_validation(df, "alarms")
        assert valid_df.count() == 0
        errors = invalid_df.collect()[0]["_validation_errors"]
        assert "status_valid" in errors


class TestContractsValidation:
    def test_end_date_before_start_date_quarantined(self, spark):
        df = _make_df(spark, [{
            "ContractID":        "C-001",
            "CustomerID":        "CUST-001",
            "TagID":             "TAG-001",
            "ContractStartDate": "2024-06-01",
            "ContractEndDate":   "2024-01-01",
            "ContractVolume":    1000.0,
            "PricePerUnit":      5.0,
        }], _CONTRACTS_SCHEMA)
        valid_df, invalid_df = apply_validation(df, "contracts")
        assert valid_df.count() == 0
        errors = invalid_df.collect()[0]["_validation_errors"]
        assert "end_after_start" in errors


class TestFinancialForecastsValidation:
    def test_risk_factor_out_of_range_quarantined(self, spark):
        df = _make_df(spark, [{
            "ForecastID":          "FF-001",
            "TagID":               "TAG-001",
            "ForecastDate":        "2024-01-01",
            "ExpectedConsumption": 500.0,
            "ExpectedRevenue":     2500.0,
            "RiskFactor":          1.5,
        }], _FINANCIAL_FORECASTS_SCHEMA)
        valid_df, invalid_df = apply_validation(df, "financial_forecasts")
        assert valid_df.count() == 0
        errors = invalid_df.collect()[0]["_validation_errors"]
        assert "risk_factor_in_range" in errors

    def test_valid_forecast_passes(self, spark):
        df = _make_df(spark, [{
            "ForecastID":          "FF-002",
            "TagID":               "TAG-001",
            "ForecastDate":        "2024-01-01",
            "ExpectedConsumption": 500.0,
            "ExpectedRevenue":     2500.0,
            "RiskFactor":          0.3,
        }], _FINANCIAL_FORECASTS_SCHEMA)
        valid_df, invalid_df = apply_validation(df, "financial_forecasts")
        assert valid_df.count() == 1
        assert invalid_df.count() == 0


class TestMixedValidInvalid:
    def test_valid_and_invalid_rows_split_correctly(self, spark):
        df = _make_df(spark, [
            {
                "MeasurementID": "M-001",
                "TagID":         "TAG-001",
                "Timestamp":     "2024-01-01T12:00:00",
                "Value":         42.5,
                "QualityFlag":   "GOOD",
            },
            {
                "MeasurementID": "M-002",
                "TagID":         "TAG-001",
                "Timestamp":     "2024-01-01T12:00:00",
                "Value":         None,
                "QualityFlag":   "GOOD",
            },
        ], _MEASUREMENTS_SCHEMA)
        valid_df, invalid_df = apply_validation(df, "measurements")
        assert valid_df.count() == 1
        assert invalid_df.count() == 1
        assert valid_df.collect()[0]["MeasurementID"] == "M-001"
        assert invalid_df.collect()[0]["MeasurementID"] == "M-002"
