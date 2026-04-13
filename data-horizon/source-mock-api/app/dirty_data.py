"""Inject realistic data quality issues into generated records.

When the ``dirty`` query parameter is enabled, this module post-processes
clean Pydantic model instances into plain dicts and applies three
independent mutation passes — duplicate injection, extra-column injection,
and type corruption — to simulate real-world data quality problems that
downstream pipelines must handle.
"""

import copy
import logging
import random
import time
import uuid
from datetime import datetime

from pydantic import BaseModel

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Tunable rates — each applies independently, so a single record can be
# duplicated AND have extra columns AND be type-corrupted.
# ---------------------------------------------------------------------------
DUPLICATE_RATE = 0.05
EXTRA_COLUMN_RATE = 0.10
TYPE_CORRUPTION_RATE = 0.08
NEAR_DUPLICATE_RATIO = 0.30

# ---------------------------------------------------------------------------
# FK / ID fields that must never be mutated or nulled.
# ---------------------------------------------------------------------------
_PROTECTED_FIELDS = frozenset({
    "MeasurementID", "AlarmID", "MaintenanceID", "EventID", "ContractID",
    "BillingID", "InventoryID", "ComplianceID", "ForecastID",
    "TagID", "CustomerID", "EquipmentID", "LocationID",
})

# ---------------------------------------------------------------------------
# Per-table metadata — drives all three mutation passes so the logic is
# data-driven rather than hard-coded per table.
# ---------------------------------------------------------------------------
TABLE_METADATA: dict[str, dict] = {
    "measurements": {
        "float_fields": ["Value"],
        "int_fields": [],
        "date_fields": [],
        "datetime_fields": ["Timestamp"],
        "string_fields": ["QualityFlag"],
        "extra_columns": {
            "SensorFirmwareVersion": lambda: f"v{random.randint(1, 5)}.{random.randint(0, 9)}",
            "CalibrationOffset": lambda: round(random.uniform(-0.5, 0.5), 4),
        },
    },
    "alarms": {
        "float_fields": ["ThresholdValue"],
        "int_fields": [],
        "date_fields": [],
        "datetime_fields": ["Timestamp"],
        "string_fields": ["AlarmType", "Status"],
        "extra_columns": {
            "Priority": lambda: random.randint(1, 5),
            "EscalationLevel": lambda: random.choice(["L1", "L2", "L3"]),
        },
    },
    "maintenance": {
        "float_fields": [],
        "int_fields": [],
        "date_fields": ["MaintenanceDate"],
        "datetime_fields": [],
        "string_fields": ["WorkOrderID", "ActionTaken", "Technician"],
        "extra_columns": {
            "EstimatedCost": lambda: round(random.uniform(200, 15000), 2),
            "PartNumber": lambda: f"PN-{uuid.uuid4().hex[:8].upper()}",
        },
    },
    "events": {
        "float_fields": [],
        "int_fields": [],
        "date_fields": [],
        "datetime_fields": ["Timestamp"],
        "string_fields": ["EventType", "Notes"],
        "extra_columns": {
            "Severity": lambda: random.choice(["Low", "Medium", "High", "Critical"]),
            "CorrelationID": lambda: uuid.uuid4().hex[:12].upper(),
        },
    },
    "contracts": {
        "float_fields": ["ContractVolume", "PricePerUnit"],
        "int_fields": [],
        "date_fields": ["ContractStartDate", "ContractEndDate"],
        "datetime_fields": [],
        "string_fields": [],
        "extra_columns": {
            "RenewalStatus": lambda: random.choice(["Auto-Renew", "Pending", "Cancelled"]),
            "DiscountPercent": lambda: round(random.uniform(0, 25), 1),
        },
    },
    "billing": {
        "float_fields": ["ConsumptionVolume", "TotalAmount"],
        "int_fields": [],
        "date_fields": [],
        "datetime_fields": [],
        "string_fields": ["BillingPeriod", "PaymentStatus"],
        "extra_columns": {
            "Currency": lambda: random.choice(["USD", "EUR", "GBP", "CAD"]),
            "TaxAmount": lambda: round(random.uniform(50, 5000), 2),
        },
    },
    "inventory": {
        "float_fields": [],
        "int_fields": ["Quantity"],
        "date_fields": [],
        "datetime_fields": ["LastUpdated"],
        "string_fields": ["MaterialType", "StorageLocation"],
        "extra_columns": {
            "ReorderThreshold": lambda: random.randint(5, 50),
            "SupplierName": lambda: random.choice([
                "Acme Industrial", "GlobalParts Co", "PrecisionTech", "Apex Supply",
            ]),
        },
    },
    "compliance": {
        "float_fields": [],
        "int_fields": [],
        "date_fields": ["InspectionDate"],
        "datetime_fields": [],
        "string_fields": ["RegulationName", "Status", "Inspector"],
        "extra_columns": {
            "CertificateNumber": lambda: f"CERT-{random.randint(100000, 999999)}",
            "NextAuditDate": lambda: f"{random.randint(2026, 2028)}-{random.randint(1, 12):02d}-{random.randint(1, 28):02d}",
        },
    },
    "forecasts": {
        "float_fields": ["ExpectedConsumption", "ExpectedRevenue", "RiskFactor"],
        "int_fields": [],
        "date_fields": ["ForecastDate"],
        "datetime_fields": [],
        "string_fields": [],
        "extra_columns": {
            "ConfidenceInterval": lambda: round(random.uniform(0.70, 0.99), 2),
            "ModelVersion": lambda: f"v{random.randint(1, 4)}.{random.randint(0, 9)}.{random.randint(0, 9)}",
        },
    },
}

# ---------------------------------------------------------------------------
# Alternative date formats used for type-corruption mutations.
# ---------------------------------------------------------------------------
_ALT_DATE_FORMATS = ["%m/%d/%Y", "%d-%b-%Y", "%Y%m%d"]
_ALT_DATETIME_FORMATS = ["%m/%d/%Y %H:%M:%S", "%d-%b-%Y %H:%M", "%Y%m%d%H%M%S"]


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def apply_dirty_data(records: list[BaseModel], table_name: str) -> list[dict]:
    """Convert *records* to dicts and apply dirty-data mutations.

    Returns a list of plain dicts (not Pydantic models) so that type
    mismatches, extra columns, and nulls can be represented without
    triggering validation errors.
    """
    if table_name not in TABLE_METADATA:
        return [r.model_dump(mode="json") for r in records]

    start = time.perf_counter()
    dicts = [r.model_dump(mode="json") for r in records]

    dicts = _inject_duplicates(dicts, table_name)
    dicts = _inject_extra_columns(dicts, table_name)
    dicts = _corrupt_types(dicts, table_name)

    elapsed_ms = round((time.perf_counter() - start) * 1_000, 2)
    logger.debug(
        "Applied dirty data",
        extra={
            "table": table_name,
            "output_count": len(dicts),
            "dirty_data_ms": elapsed_ms,
        },
    )
    return dicts


# ---------------------------------------------------------------------------
# Mutation passes
# ---------------------------------------------------------------------------


def _inject_duplicates(records: list[dict], table_name: str) -> list[dict]:
    """Append exact or near-duplicate copies for ~DUPLICATE_RATE of records."""
    count = max(1, int(len(records) * DUPLICATE_RATE))
    chosen = random.sample(range(len(records)), min(count, len(records)))
    meta = TABLE_METADATA[table_name]

    duplicates: list[dict] = []
    for idx in chosen:
        original = records[idx]
        dup = copy.deepcopy(original)

        if random.random() < NEAR_DUPLICATE_RATIO:
            _mutate_near_duplicate(dup, meta)

        duplicates.append(dup)

    records.extend(duplicates)
    return records


def _mutate_near_duplicate(record: dict, meta: dict) -> None:
    """Slightly alter one non-ID field to create a near-duplicate."""
    candidates: list[str] = []
    for group in ("float_fields", "int_fields", "string_fields"):
        candidates.extend(meta.get(group, []))

    if not candidates:
        return

    field = random.choice(candidates)
    value = record.get(field)
    if value is None:
        return

    if field in meta.get("float_fields", []) and isinstance(value, (int, float)):
        record[field] = round(value + random.uniform(-5, 5), 2)
    elif field in meta.get("int_fields", []) and isinstance(value, int):
        record[field] = value + random.randint(-3, 3)
    elif isinstance(value, str):
        record[field] = value + " (revised)"


def _inject_extra_columns(records: list[dict], table_name: str) -> list[dict]:
    """Add 1-2 unexpected columns to ~EXTRA_COLUMN_RATE of records."""
    extras = TABLE_METADATA[table_name]["extra_columns"]
    if not extras:
        return records

    extra_keys = list(extras.keys())
    count = max(1, int(len(records) * EXTRA_COLUMN_RATE))
    chosen = random.sample(range(len(records)), min(count, len(records)))

    for idx in chosen:
        num_cols = random.randint(1, min(2, len(extra_keys)))
        cols = random.sample(extra_keys, num_cols)
        for col in cols:
            records[idx][col] = extras[col]()

    return records


def _corrupt_types(records: list[dict], table_name: str) -> list[dict]:
    """Apply one type-corruption mutation to ~TYPE_CORRUPTION_RATE of records."""
    meta = TABLE_METADATA[table_name]
    count = max(1, int(len(records) * TYPE_CORRUPTION_RATE))
    chosen = random.sample(range(len(records)), min(count, len(records)))

    for idx in chosen:
        mutation = random.choice(["string_for_number", "wrong_date", "null"])
        _apply_mutation(records[idx], meta, mutation)

    return records


def _apply_mutation(record: dict, meta: dict, mutation: str) -> None:
    """Apply a single type-corruption mutation to *record*."""
    if mutation == "string_for_number":
        _corrupt_number_to_string(record, meta)
    elif mutation == "wrong_date":
        _corrupt_date_format(record, meta)
    elif mutation == "null":
        _inject_null(record, meta)


def _corrupt_number_to_string(record: dict, meta: dict) -> None:
    """Convert a numeric field's value to its string representation."""
    candidates = meta.get("float_fields", []) + meta.get("int_fields", [])
    if not candidates:
        return
    field = random.choice(candidates)
    if record.get(field) is not None:
        record[field] = str(record[field])


def _corrupt_date_format(record: dict, meta: dict) -> None:
    """Reformat a date or datetime field into a non-ISO format."""
    date_fields = meta.get("date_fields", [])
    datetime_fields = meta.get("datetime_fields", [])

    if date_fields and (not datetime_fields or random.random() < 0.5):
        field = random.choice(date_fields)
        value = record.get(field)
        if value and isinstance(value, str):
            try:
                dt = datetime.strptime(value, "%Y-%m-%d")
                record[field] = dt.strftime(random.choice(_ALT_DATE_FORMATS))
            except ValueError:
                pass
    elif datetime_fields:
        field = random.choice(datetime_fields)
        value = record.get(field)
        if value and isinstance(value, str):
            try:
                dt = datetime.fromisoformat(value)
                record[field] = dt.strftime(random.choice(_ALT_DATETIME_FORMATS))
            except ValueError:
                pass


def _inject_null(record: dict, meta: dict) -> None:
    """Set a random non-ID field to None."""
    all_fields = (
        meta.get("float_fields", [])
        + meta.get("int_fields", [])
        + meta.get("date_fields", [])
        + meta.get("datetime_fields", [])
        + meta.get("string_fields", [])
    )
    candidates = [f for f in all_fields if f not in _PROTECTED_FIELDS]
    if candidates:
        record[random.choice(candidates)] = None
