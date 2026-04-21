"""Unit tests for apply_validation() — pure PySpark, no AWS dependencies."""

import datetime

import pytest
from pyspark.sql import Row

from glue_jobs.utils.validation_rules import apply_validation


class TestMeasurementsValidation:
    def test_valid_row_passes_all_rules(self, spark):
        row = Row(
            MeasurementID="M-001",
            TagID="TAG-001",
            Timestamp=datetime.datetime(2024, 1, 1, 12, 0, 0),
            Value=42.5,
            QualityFlag="GOOD",
        )
        df = spark.createDataFrame([row])
        valid_df, invalid_df = apply_validation(df, "measurements")
        assert valid_df.count() == 1
        assert invalid_df.count() == 0

    def test_null_value_quarantined(self, spark):
        row = Row(
            MeasurementID="M-002",
            TagID="TAG-001",
            Timestamp=datetime.datetime(2024, 1, 1, 12, 0, 0),
            Value=None,
            QualityFlag="GOOD",
        )
        df = spark.createDataFrame([row])
        valid_df, invalid_df = apply_validation(df, "measurements")
        assert valid_df.count() == 0
        assert invalid_df.count() == 1
        errors = invalid_df.collect()[0]["_validation_errors"]
        assert "value_not_null" in errors

    def test_invalid_quality_flag_quarantined(self, spark):
        row = Row(
            MeasurementID="M-003",
            TagID="TAG-001",
            Timestamp=datetime.datetime(2024, 1, 1, 12, 0, 0),
            Value=10.0,
            QualityFlag="CORRUPTED",
        )
        df = spark.createDataFrame([row])
        valid_df, invalid_df = apply_validation(df, "measurements")
        assert valid_df.count() == 0
        assert invalid_df.count() == 1
        errors = invalid_df.collect()[0]["_validation_errors"]
        assert "quality_flag_valid" in errors

    def test_multiple_rule_failures_combined_in_errors_column(self, spark):
        row = Row(
            MeasurementID="M-004",
            TagID="TAG-001",
            Timestamp=None,
            Value=None,
            QualityFlag="CORRUPTED",
        )
        df = spark.createDataFrame([row])
        valid_df, invalid_df = apply_validation(df, "measurements")
        assert valid_df.count() == 0
        errors = invalid_df.collect()[0]["_validation_errors"]
        assert "value_not_null" in errors
        assert "timestamp_not_null" in errors
        assert "quality_flag_valid" in errors
        assert "; " in errors


class TestMaintenanceValidation:
    def test_null_technician_quarantined(self, spark):
        row = Row(
            MaintenanceID="MX-001",
            TagID="TAG-001",
            WorkOrderID="WO-001",
            MaintenanceDate=datetime.date(2024, 1, 1),
            ActionTaken="Replaced filter",
            Technician=None,
        )
        df = spark.createDataFrame([row])
        valid_df, invalid_df = apply_validation(df, "maintenance")
        assert valid_df.count() == 0
        errors = invalid_df.collect()[0]["_validation_errors"]
        assert "technician_required" in errors


class TestAlarmsValidation:
    def test_negative_threshold_quarantined(self, spark):
        row = Row(
            AlarmID="AL-001",
            TagID="TAG-001",
            AlarmType="HI",
            ThresholdValue=-5.0,
            Timestamp=datetime.datetime(2024, 1, 1, 8, 0, 0),
            Status="ACTIVE",
        )
        df = spark.createDataFrame([row])
        valid_df, invalid_df = apply_validation(df, "alarms")
        assert valid_df.count() == 0
        errors = invalid_df.collect()[0]["_validation_errors"]
        assert "threshold_non_negative" in errors

    def test_invalid_alarm_status_quarantined(self, spark):
        row = Row(
            AlarmID="AL-002",
            TagID="TAG-001",
            AlarmType="HI",
            ThresholdValue=10.0,
            Timestamp=datetime.datetime(2024, 1, 1, 8, 0, 0),
            Status="UNKNOWN_STATE",
        )
        df = spark.createDataFrame([row])
        valid_df, invalid_df = apply_validation(df, "alarms")
        assert valid_df.count() == 0
        errors = invalid_df.collect()[0]["_validation_errors"]
        assert "status_valid" in errors


class TestContractsValidation:
    def test_end_date_before_start_date_quarantined(self, spark):
        row = Row(
            ContractID="C-001",
            CustomerID="CUST-001",
            TagID="TAG-001",
            ContractStartDate=datetime.date(2024, 6, 1),
            ContractEndDate=datetime.date(2024, 1, 1),
            ContractVolume=1000.0,
            PricePerUnit=5.0,
        )
        df = spark.createDataFrame([row])
        valid_df, invalid_df = apply_validation(df, "contracts")
        assert valid_df.count() == 0
        errors = invalid_df.collect()[0]["_validation_errors"]
        assert "end_after_start" in errors


class TestFinancialForecastsValidation:
    def test_risk_factor_out_of_range_quarantined(self, spark):
        row = Row(
            ForecastID="FF-001",
            TagID="TAG-001",
            ForecastDate=datetime.date(2024, 1, 1),
            ExpectedConsumption=500.0,
            ExpectedRevenue=2500.0,
            RiskFactor=1.5,
        )
        df = spark.createDataFrame([row])
        valid_df, invalid_df = apply_validation(df, "financial_forecasts")
        assert valid_df.count() == 0
        errors = invalid_df.collect()[0]["_validation_errors"]
        assert "risk_factor_in_range" in errors

    def test_valid_forecast_passes(self, spark):
        row = Row(
            ForecastID="FF-002",
            TagID="TAG-001",
            ForecastDate=datetime.date(2024, 1, 1),
            ExpectedConsumption=500.0,
            ExpectedRevenue=2500.0,
            RiskFactor=0.3,
        )
        df = spark.createDataFrame([row])
        valid_df, invalid_df = apply_validation(df, "financial_forecasts")
        assert valid_df.count() == 1
        assert invalid_df.count() == 0


class TestMixedValidInvalid:
    def test_valid_and_invalid_rows_split_correctly(self, spark):
        rows = [
            Row(
                MeasurementID="M-001",
                TagID="TAG-001",
                Timestamp=datetime.datetime(2024, 1, 1, 12, 0, 0),
                Value=42.5,
                QualityFlag="GOOD",
            ),
            Row(
                MeasurementID="M-002",
                TagID="TAG-001",
                Timestamp=datetime.datetime(2024, 1, 1, 12, 0, 0),
                Value=None,
                QualityFlag="GOOD",
            ),
        ]
        df = spark.createDataFrame(rows)
        valid_df, invalid_df = apply_validation(df, "measurements")
        assert valid_df.count() == 1
        assert invalid_df.count() == 1
        assert valid_df.collect()[0]["MeasurementID"] == "M-001"
        assert invalid_df.collect()[0]["MeasurementID"] == "M-002"
