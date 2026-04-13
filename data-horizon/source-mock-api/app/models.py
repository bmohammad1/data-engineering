"""Pydantic models for API request/response schemas."""

from datetime import date, datetime

from pydantic import BaseModel


# --- Master / Static Tables ---


class Equipment(BaseModel):
    """Physical equipment associated with a tag (e.g. compressor, pump, valve)."""

    EquipmentID: str
    EquipmentName: str
    EquipmentType: str
    Manufacturer: str
    InstallDate: date


class Location(BaseModel):
    """Geographic site where equipment is installed."""

    LocationID: str
    SiteName: str
    Area: str
    GPSCoordinates: str


class Customer(BaseModel):
    """Customer who owns or operates equipment at a location."""

    CustomerID: str
    CustomerName: str
    Industry: str
    ContactInfo: str
    Region: str


class Tag(BaseModel):
    """Instrument tag — the central entity linking equipment, location, and data streams."""

    TagID: str
    TagName: str
    Description: str
    UnitOfMeasure: str
    EquipmentID: str
    LocationID: str


# --- Per-Request Random Tables ---


class Measurement(BaseModel):
    """Time-series measurement reading from a tag's sensor."""

    MeasurementID: str
    TagID: str
    Timestamp: datetime
    Value: float
    QualityFlag: str


class Alarm(BaseModel):
    """Threshold-based alarm event triggered by a tag."""

    AlarmID: str
    TagID: str
    AlarmType: str
    ThresholdValue: float
    Timestamp: datetime
    Status: str


class Maintenance(BaseModel):
    """Work order record for maintenance performed on a tag's equipment."""

    MaintenanceID: str
    TagID: str
    WorkOrderID: str
    MaintenanceDate: date
    ActionTaken: str
    Technician: str


class Event(BaseModel):
    """Operational event associated with a tag (trips, overrides, mode changes)."""

    EventID: str
    TagID: str
    EventType: str
    Timestamp: datetime
    Notes: str


class CustomerContract(BaseModel):
    """Service or supply contract between a customer and a tag's data stream."""

    ContractID: str
    CustomerID: str
    TagID: str
    ContractStartDate: date
    ContractEndDate: date
    ContractVolume: float
    PricePerUnit: float


class Billing(BaseModel):
    """Monthly billing record for consumption on a tag."""

    BillingID: str
    TagID: str
    CustomerID: str
    BillingPeriod: str
    ConsumptionVolume: float
    TotalAmount: float
    PaymentStatus: str


class Inventory(BaseModel):
    """Spare parts and materials inventory associated with a tag's equipment."""

    InventoryID: str
    TagID: str
    MaterialType: str
    Quantity: int
    StorageLocation: str
    LastUpdated: datetime


class RegulatoryCompliance(BaseModel):
    """Regulatory inspection and compliance record for a tag."""

    ComplianceID: str
    TagID: str
    RegulationName: str
    InspectionDate: date
    Status: str
    Inspector: str


class FinancialForecast(BaseModel):
    """Forward-looking consumption and revenue forecast for a tag."""

    ForecastID: str
    TagID: str
    ForecastDate: date
    ExpectedConsumption: float
    ExpectedRevenue: float
    RiskFactor: float


# --- Aggregated Responses ---


class TagListResponse(BaseModel):
    """Response model for the /tags endpoint."""

    tags: list[str]


class TagResponse(BaseModel):
    """Full tag payload — static metadata plus all related generated records."""

    tag: Tag
    equipment: Equipment
    location: Location
    measurements: list[Measurement]
    alarms: list[Alarm]
    maintenance: list[Maintenance]
    events: list[Event]
    customer: Customer
    contracts: list[CustomerContract]
    billing: list[Billing]
    inventory: list[Inventory]
    regulatory_compliance: list[RegulatoryCompliance]
    financial_forecasts: list[FinancialForecast]
