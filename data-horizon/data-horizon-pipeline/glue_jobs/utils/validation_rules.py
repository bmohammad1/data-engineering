"""Per-table data quality validation rules.

Each rule is a (rule_name, Column_expression) pair where the expression
evaluates to True for a valid row. apply_validation() splits a DataFrame
into a valid set and an invalid set; the invalid set gets a
_validation_errors column listing every rule name that failed.
"""

import logging

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.column import Column

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Allowed enum values per column
# ---------------------------------------------------------------------------

_QUALITY_FLAGS = {"GOOD", "BAD", "UNCERTAIN"}
_ALARM_STATUSES = {"ACTIVE", "CLEARED", "ACKNOWLEDGED"}
_PAYMENT_STATUSES = {"PAID", "UNPAID", "OVERDUE"}
_COMPLIANCE_STATUSES = {"COMPLIANT", "NON_COMPLIANT", "PENDING"}


def _in_set(col_name: str, allowed: set[str]) -> Column:
    """Return a Column that is True when the value is in the allowed set."""
    return F.col(col_name).isin(list(allowed))


def _not_null_or_empty(col_name: str) -> Column:
    """Return a Column that is True when the value is non-null and non-empty."""
    return F.col(col_name).isNotNull() & (F.trim(F.col(col_name)) != "")


# ---------------------------------------------------------------------------
# Per-table rule definitions
# ---------------------------------------------------------------------------

# Each entry is a list of (rule_name, passing_condition) tuples.
# A row is VALID only if ALL conditions evaluate to True.

VALIDATION_RULES: dict[str, list[tuple[str, Column]]] = {
    "tag": [
        ("tag_id_required",   _not_null_or_empty("TagID")),
    ],
    "equipment": [
        ("equipment_id_required", _not_null_or_empty("EquipmentID")),
    ],
    "location": [
        ("location_id_required", _not_null_or_empty("LocationID")),
    ],
    "customer": [
        ("customer_id_required", _not_null_or_empty("CustomerID")),
    ],
    "measurements": [
        ("measurement_id_required", _not_null_or_empty("MeasurementID")),
        ("tag_id_required",         _not_null_or_empty("TagID")),
        ("value_not_null",          F.col("Value").isNotNull()),
        ("timestamp_not_null",      F.col("Timestamp").isNotNull()),
        ("quality_flag_valid",      _in_set("QualityFlag", _QUALITY_FLAGS)),
    ],
    "alarms": [
        ("alarm_id_required",      _not_null_or_empty("AlarmID")),
        ("tag_id_required",        _not_null_or_empty("TagID")),
        ("threshold_not_null",     F.col("ThresholdValue").isNotNull()),
        ("threshold_non_negative", F.col("ThresholdValue") >= 0),
        ("timestamp_not_null",     F.col("Timestamp").isNotNull()),
        ("status_valid",           _in_set("Status", _ALARM_STATUSES)),
    ],
    "maintenance": [
        ("maintenance_id_required", _not_null_or_empty("MaintenanceID")),
        ("tag_id_required",         _not_null_or_empty("TagID")),
        ("date_not_null",           F.col("MaintenanceDate").isNotNull()),
        ("technician_required",     _not_null_or_empty("Technician")),
    ],
    "events": [
        ("event_id_required",  _not_null_or_empty("EventID")),
        ("tag_id_required",    _not_null_or_empty("TagID")),
        ("timestamp_not_null", F.col("Timestamp").isNotNull()),
    ],
    "contracts": [
        ("contract_id_required",    _not_null_or_empty("ContractID")),
        ("customer_id_required",    _not_null_or_empty("CustomerID")),
        ("tag_id_required",         _not_null_or_empty("TagID")),
        ("volume_positive",         F.col("ContractVolume") > 0),
        ("price_positive",          F.col("PricePerUnit") > 0),
        ("end_after_start",
            F.col("ContractEndDate").isNotNull() &
            F.col("ContractStartDate").isNotNull() &
            (F.col("ContractEndDate") >= F.col("ContractStartDate"))
        ),
    ],
    "billing": [
        ("billing_id_required",    _not_null_or_empty("BillingID")),
        ("tag_id_required",        _not_null_or_empty("TagID")),
        ("customer_id_required",   _not_null_or_empty("CustomerID")),
        ("consumption_non_negative", F.col("ConsumptionVolume") >= 0),
        ("amount_non_negative",    F.col("TotalAmount") >= 0),
        ("payment_status_valid",   _in_set("PaymentStatus", _PAYMENT_STATUSES)),
    ],
    "inventory": [
        ("inventory_id_required", _not_null_or_empty("InventoryID")),
        ("tag_id_required",       _not_null_or_empty("TagID")),
        ("quantity_non_negative", F.col("Quantity") >= 0),
    ],
    "regulatory_compliance": [
        ("compliance_id_required", _not_null_or_empty("ComplianceID")),
        ("tag_id_required",        _not_null_or_empty("TagID")),
        ("inspection_date_not_null", F.col("InspectionDate").isNotNull()),
    ],
    "financial_forecasts": [
        ("forecast_id_required",       _not_null_or_empty("ForecastID")),
        ("tag_id_required",            _not_null_or_empty("TagID")),
        ("consumption_non_negative",   F.col("ExpectedConsumption") >= 0),
        ("revenue_non_negative",       F.col("ExpectedRevenue") >= 0),
        ("risk_factor_in_range",
            F.col("RiskFactor").isNotNull() &
            (F.col("RiskFactor") >= 0.0) &
            (F.col("RiskFactor") <= 1.0)
        ),
    ],
}


def apply_validation(df: DataFrame, table: str) -> tuple[DataFrame, DataFrame]:
    """Split df into (valid_df, invalid_df) based on per-table rules.

    invalid_df gets an extra _validation_errors column listing every
    rule name that failed, separated by '; '.
    """
    rules = VALIDATION_RULES.get(table, [])

    if not rules:
        logger.warning("No validation rules defined for table '%s' — treating all rows as valid", table)
        return df, df.filter(F.lit(False))

    # Build a boolean column per rule, then derive a list of failed rule names.
    failed_rule_parts = []
    for rule_name, condition in rules:
        # When condition is False the rule failed — include the rule name in the error list
        failed_rule_parts.append(
            F.when(~condition, F.lit(rule_name)).otherwise(F.lit(None))
        )

    # Concatenate non-null failed rule names into a single string
    errors_col = F.concat_ws(
        "; ",
        *[F.coalesce(part, F.lit("")) for part in failed_rule_parts]
    )
    # Trim trailing/leading separators that arise from empty strings
    errors_col = F.regexp_replace(errors_col, r"^(; )+|(; )+$|(?<=; )(; )+", "")

    df_with_errors = df.withColumn("_validation_errors", errors_col)

    # A row is valid when _validation_errors is empty
    valid_df = df_with_errors.filter(
        F.col("_validation_errors").isNull() | (F.trim(F.col("_validation_errors")) == "")
    ).drop("_validation_errors")

    invalid_df = df_with_errors.filter(
        F.col("_validation_errors").isNotNull() & (F.trim(F.col("_validation_errors")) != "")
    )

    return valid_df, invalid_df
