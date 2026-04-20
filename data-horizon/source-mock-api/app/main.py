"""FastAPI application entrypoint and route handlers."""

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, status
from fastapi.responses import JSONResponse

from app.data_generator import (
    generate_alarms,
    generate_billing,
    generate_compliance,
    generate_contracts,
    generate_events,
    generate_forecasts,
    generate_inventory,
    generate_maintenance,
    generate_measurements,
)
from app.dirty_data import apply_dirty_data
from app.exceptions import TagNotFoundException
from app.logging_config import configure_logging, request_log_ctx
from app.middleware import RequestLoggingMiddleware
from app.models import (
    Customer,
    Equipment,
    Location,
    Tag,
    TagListResponse,
)
from app.static_data import CUSTOMERS, EQUIPMENT, LOCATIONS, TAGS

# Configure logging before anything else emits log messages.
configure_logging()

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Per-request record counts — these control the volume of generated data
# returned in each /tag/{tag_id} response. Tuned to produce a realistic
# payload size (~13k records) without exceeding Lambda response limits.
# ---------------------------------------------------------------------------
MEASUREMENT_COUNT = 4000
ALARM_COUNT = 2000
MAINTENANCE_COUNT = 1500
EVENT_COUNT = 2000
CONTRACT_COUNT = 800
BILLING_COUNT = 800
INVENTORY_COUNT = 600
COMPLIANCE_COUNT = 600
FORECAST_COUNT = 700

APP_VERSION = "1.0.0"


@asynccontextmanager
async def lifespan(_application: FastAPI):
    """Log static data stats on startup and confirm graceful shutdown."""
    logger.info(
        "Application started",
        extra={
            "version": APP_VERSION,
            "log_level": logging.getLevelName(logging.getLogger().level),
            "tags_loaded": len(TAGS),
            "equipment_loaded": len(EQUIPMENT),
            "locations_loaded": len(LOCATIONS),
            "customers_loaded": len(CUSTOMERS),
        },
    )
    yield
    logger.info("Application shutting down")


app = FastAPI(
    title="Source MOCK API",
    description="Returns randomly generated, FK-consistent data for a given TagID.",
    version=APP_VERSION,
    lifespan=lifespan,
)

app.add_middleware(RequestLoggingMiddleware)


@app.get("/tags", response_model=TagListResponse, status_code=status.HTTP_200_OK)
def list_tags() -> TagListResponse:
    """Return all available tag IDs."""
    return TagListResponse(tags=list(TAGS.keys()))


@app.get("/tag/{tag_id}", status_code=status.HTTP_200_OK)
def get_tag(tag_id: str) -> JSONResponse:
    """Return tag metadata plus randomly generated data from every related table.

    All data includes realistic quality issues: duplicates, extra columns, and
    type corruption. FK constraints are enforced for equipment, location, and
    customer references.
    """
    tag_data = TAGS.get(tag_id)
    if not tag_data:
        raise TagNotFoundException(tag_id)

    equipment_id = tag_data["EquipmentID"]
    location_id = tag_data["LocationID"]
    customer_id = tag_data["CustomerID"]

    tag = Tag(
        TagID=tag_data["TagID"],
        TagName=tag_data["TagName"],
        Description=tag_data["Description"],
        UnitOfMeasure=tag_data["UnitOfMeasure"],
        EquipmentID=equipment_id,
        LocationID=location_id,
    )

    measurements = generate_measurements(tag_id, count=MEASUREMENT_COUNT)
    alarms = generate_alarms(tag_id, count=ALARM_COUNT)
    maintenance = generate_maintenance(tag_id, count=MAINTENANCE_COUNT)
    events = generate_events(tag_id, count=EVENT_COUNT)
    contracts = generate_contracts(tag_id, customer_id, count=CONTRACT_COUNT)
    billing = generate_billing(tag_id, customer_id, count=BILLING_COUNT)
    inventory = generate_inventory(tag_id, count=INVENTORY_COUNT)
    compliance = generate_compliance(tag_id, count=COMPLIANCE_COUNT)
    forecasts = generate_forecasts(tag_id, count=FORECAST_COUNT)

    record_count = (
        len(measurements)
        + len(alarms)
        + len(maintenance)
        + len(events)
        + len(contracts)
        + len(billing)
        + len(inventory)
        + len(compliance)
        + len(forecasts)
    )
    request_log_ctx.get({}).update(tag_id=tag_id, record_count=record_count)

    return JSONResponse(content={
        "tag": tag.model_dump(mode="json"),
        "equipment": Equipment(**EQUIPMENT[equipment_id]).model_dump(mode="json"),
        "location": Location(**LOCATIONS[location_id]).model_dump(mode="json"),
        "customer": Customer(**CUSTOMERS[customer_id]).model_dump(mode="json"),
        "measurements": apply_dirty_data(measurements, "measurements"),
        "alarms": apply_dirty_data(alarms, "alarms"),
        "maintenance": apply_dirty_data(maintenance, "maintenance"),
        "events": apply_dirty_data(events, "events"),
        "contracts": apply_dirty_data(contracts, "contracts"),
        "billing": apply_dirty_data(billing, "billing"),
        "inventory": apply_dirty_data(inventory, "inventory"),
        "regulatory_compliance": apply_dirty_data(compliance, "compliance"),
        "financial_forecasts": apply_dirty_data(forecasts, "forecasts"),
    })
