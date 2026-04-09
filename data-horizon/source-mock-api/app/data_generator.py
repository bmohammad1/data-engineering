"""Generate random data for each request while preserving FK constraints.

Each generator function accepts a ``tag_id`` (and optionally ``customer_id``)
so that every generated record correctly references its parent tag and
customer.  IDs use truncated UUID4 hex to guarantee uniqueness across
concurrent requests.
"""

import logging
import random
import time
import uuid
from datetime import date, datetime, timedelta

from app.models import (
    Alarm,
    Billing,
    CustomerContract,
    Event,
    FinancialForecast,
    Inventory,
    Maintenance,
    Measurement,
    RegulatoryCompliance,
)

logger = logging.getLogger(__name__)

# --- ID length chosen to be short enough for readability while still
#     having negligible collision probability at our request volumes. ---
_UID_LENGTH = 12


def _uid() -> str:
    """Return a short, uppercase hex string suitable for use as a record ID."""
    return uuid.uuid4().hex[:_UID_LENGTH].upper()


def _random_datetime(days_back: int = 90) -> datetime:
    """Return a random datetime within the last ``days_back`` days."""
    offset = random.randint(0, days_back * 24 * 3600)
    return datetime.utcnow() - timedelta(seconds=offset)


def _random_date(days_back: int = 365) -> date:
    """Return a random date within the last ``days_back`` days."""
    return date.today() - timedelta(days=random.randint(0, days_back))


# ---------------------------------------------------------------------------
# Per-table generators — each accepts tag_id and (where needed) customer_id
# to enforce FK relationships.
# ---------------------------------------------------------------------------


def generate_measurements(tag_id: str, count: int = 5) -> list[Measurement]:
    """Generate ``count`` time-series measurement readings for a tag."""
    start = time.perf_counter()
    results = [
        Measurement(
            MeasurementID=f"MEAS-{_uid()}",
            TagID=tag_id,
            Timestamp=_random_datetime(),
            Value=round(random.uniform(0, 500), 2),
            QualityFlag=random.choice(["Good", "Bad", "Uncertain", "Substituted"]),
        )
        for _ in range(count)
    ]
    logger.debug(
        "Generated measurements",
        extra={
            "tag_id": tag_id,
            "record_count": len(results),
            "data_generation_ms": round((time.perf_counter() - start) * 1_000, 2),
        },
    )
    return results


def generate_alarms(tag_id: str, count: int = 3) -> list[Alarm]:
    """Generate ``count`` threshold-based alarm events for a tag."""
    start = time.perf_counter()
    results = [
        Alarm(
            AlarmID=f"ALM-{_uid()}",
            TagID=tag_id,
            AlarmType=random.choice(
                ["High", "Low", "HighHigh", "LowLow", "RateOfChange"]
            ),
            ThresholdValue=round(random.uniform(50, 400), 2),
            Timestamp=_random_datetime(),
            Status=random.choice(["Active", "Acknowledged", "Cleared", "Shelved"]),
        )
        for _ in range(count)
    ]
    logger.debug(
        "Generated alarms",
        extra={
            "tag_id": tag_id,
            "record_count": len(results),
            "data_generation_ms": round((time.perf_counter() - start) * 1_000, 2),
        },
    )
    return results


def generate_maintenance(tag_id: str, count: int = 2) -> list[Maintenance]:
    """Generate ``count`` maintenance work-order records for a tag."""
    actions = [
        "Replaced sensor head",
        "Calibrated transmitter",
        "Cleaned strainer",
        "Tightened flange bolts",
        "Replaced gasket",
        "Lubricated bearings",
        "Updated firmware",
        "Replaced wiring harness",
    ]
    technicians = [
        "John Martinez",
        "Sarah Chen",
        "Raj Patel",
        "Emily Johnson",
        "Carlos Rivera",
        "Wei Zhang",
    ]
    start = time.perf_counter()
    results = [
        Maintenance(
            MaintenanceID=f"MNT-{_uid()}",
            TagID=tag_id,
            WorkOrderID=f"WO-{random.randint(10000, 99999)}",
            MaintenanceDate=_random_date(180),
            ActionTaken=random.choice(actions),
            Technician=random.choice(technicians),
        )
        for _ in range(count)
    ]
    logger.debug(
        "Generated maintenance",
        extra={
            "tag_id": tag_id,
            "record_count": len(results),
            "data_generation_ms": round((time.perf_counter() - start) * 1_000, 2),
        },
    )
    return results


def generate_events(tag_id: str, count: int = 3) -> list[Event]:
    """Generate ``count`` operational events for a tag."""
    event_types = [
        "ProcessTrip",
        "ManualOverride",
        "SetpointChange",
        "ModeChange",
        "SystemRestart",
        "CommunicationLoss",
    ]
    notes_pool = [
        "Operator initiated shutdown for inspection.",
        "Automatic trip due to high vibration.",
        "Setpoint changed per engineering request.",
        "Mode switched from auto to manual.",
        "System rebooted after firmware update.",
        "Communication timeout — restored after 12 s.",
    ]
    start = time.perf_counter()
    results = [
        Event(
            EventID=f"EVT-{_uid()}",
            TagID=tag_id,
            EventType=random.choice(event_types),
            Timestamp=_random_datetime(),
            Notes=random.choice(notes_pool),
        )
        for _ in range(count)
    ]
    logger.debug(
        "Generated events",
        extra={
            "tag_id": tag_id,
            "record_count": len(results),
            "data_generation_ms": round((time.perf_counter() - start) * 1_000, 2),
        },
    )
    return results


def generate_contracts(
    tag_id: str, customer_id: str, count: int = 500
) -> list[CustomerContract]:
    """Generate ``count`` customer contracts linked to a tag and customer."""
    t0 = time.perf_counter()
    results = [
        CustomerContract(
            ContractID=f"CTR-{_uid()}",
            CustomerID=customer_id,
            TagID=tag_id,
            # End date is 1–3 years after start to simulate realistic contract durations.
            ContractStartDate=(start := _random_date(730)),
            ContractEndDate=start + timedelta(days=random.randint(365, 1095)),
            ContractVolume=round(random.uniform(1000, 100000), 2),
            PricePerUnit=round(random.uniform(1.5, 25.0), 2),
        )
        for _ in range(count)
    ]
    logger.debug(
        "Generated contracts",
        extra={
            "tag_id": tag_id,
            "record_count": len(results),
            "data_generation_ms": round((time.perf_counter() - t0) * 1_000, 2),
        },
    )
    return results


def generate_billing(tag_id: str, customer_id: str, count: int = 500) -> list[Billing]:
    """Generate ``count`` monthly billing records for a tag and customer."""
    months = [
        "2025-01",
        "2025-02",
        "2025-03",
        "2025-04",
        "2025-05",
        "2025-06",
        "2025-07",
        "2025-08",
        "2025-09",
        "2025-10",
        "2025-11",
        "2025-12",
        "2026-01",
        "2026-02",
        "2026-03",
    ]
    start = time.perf_counter()
    results = [
        Billing(
            BillingID=f"BIL-{_uid()}",
            TagID=tag_id,
            CustomerID=customer_id,
            BillingPeriod=random.choice(months),
            # Total amount derived from volume × random rate to keep billing realistic.
            ConsumptionVolume=(vol := round(random.uniform(500, 50000), 2)),
            TotalAmount=round(vol * random.uniform(2.0, 20.0), 2),
            PaymentStatus=random.choice(["Paid", "Pending", "Overdue", "Disputed"]),
        )
        for _ in range(count)
    ]
    logger.debug(
        "Generated billing",
        extra={
            "tag_id": tag_id,
            "record_count": len(results),
            "data_generation_ms": round((time.perf_counter() - start) * 1_000, 2),
        },
    )
    return results


def generate_inventory(tag_id: str, count: int = 500) -> list[Inventory]:
    """Generate ``count`` spare-parts inventory records for a tag."""
    materials = [
        "Spare Sensor",
        "Gasket Kit",
        "Lubricant",
        "Filter Element",
        "Wiring Harness",
        "Calibration Gas",
        "O-Ring Set",
        "Circuit Board",
    ]
    storage_locations = [
        "Warehouse A-12",
        "Warehouse B-05",
        "Field Cabinet 3",
        "Maintenance Shop",
        "Satellite Store C",
    ]
    start = time.perf_counter()
    results = [
        Inventory(
            InventoryID=f"INV-{_uid()}",
            TagID=tag_id,
            MaterialType=random.choice(materials),
            Quantity=random.randint(1, 200),
            StorageLocation=random.choice(storage_locations),
            LastUpdated=_random_datetime(30),
        )
        for _ in range(count)
    ]
    logger.debug(
        "Generated inventory",
        extra={
            "tag_id": tag_id,
            "record_count": len(results),
            "data_generation_ms": round((time.perf_counter() - start) * 1_000, 2),
        },
    )
    return results


def generate_compliance(tag_id: str, count: int = 500) -> list[RegulatoryCompliance]:
    """Generate ``count`` regulatory inspection records for a tag."""
    regulations = [
        "EPA 40 CFR 60",
        "OSHA 29 CFR 1910",
        "API 570",
        "ASME B31.3",
        "NFPA 30",
        "DOT 49 CFR 195",
    ]
    inspectors = [
        "M. Thompson",
        "K. Nguyen",
        "D. Kowalski",
        "A. Singh",
        "R. Fernandez",
    ]
    start = time.perf_counter()
    results = [
        RegulatoryCompliance(
            ComplianceID=f"CMP-{_uid()}",
            TagID=tag_id,
            RegulationName=random.choice(regulations),
            InspectionDate=_random_date(365),
            Status=random.choice(
                ["Compliant", "Non-Compliant", "Pending Review", "Exempted"]
            ),
            Inspector=random.choice(inspectors),
        )
        for _ in range(count)
    ]
    logger.debug(
        "Generated compliance",
        extra={
            "tag_id": tag_id,
            "record_count": len(results),
            "data_generation_ms": round((time.perf_counter() - start) * 1_000, 2),
        },
    )
    return results


def generate_forecasts(tag_id: str, count: int = 500) -> list[FinancialForecast]:
    """Generate ``count`` forward-looking financial forecasts for a tag."""
    start = time.perf_counter()
    results = [
        FinancialForecast(
            ForecastID=f"FRC-{_uid()}",
            TagID=tag_id,
            # Forecasts look 1–12 months into the future.
            ForecastDate=date.today() + timedelta(days=random.randint(30, 365)),
            # Revenue derived from consumption × random price multiplier.
            ExpectedConsumption=(cons := round(random.uniform(1000, 80000), 2)),
            ExpectedRevenue=round(cons * random.uniform(3.0, 18.0), 2),
            RiskFactor=round(random.uniform(0.01, 1.0), 3),
        )
        for _ in range(count)
    ]
    logger.debug(
        "Generated forecasts",
        extra={
            "tag_id": tag_id,
            "record_count": len(results),
            "data_generation_ms": round((time.perf_counter() - start) * 1_000, 2),
        },
    )
    return results
