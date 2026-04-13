"""Static reference data for Tags, Equipment, Locations, and Customers.

All records are built deterministically at module load using modulo-based
assignment from fixed seed pools.  This guarantees the same data on every
cold start and across Lambda instances, so FK references from generated
records always resolve correctly.
"""

import logging
from datetime import date

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Seed pools — used to build Equipment, Location, Customer, and Tag records.
# Each pool is cycled via modulo to assign attributes deterministically.
# ---------------------------------------------------------------------------

_EQUIPMENT_TYPES = [
    ("Compressor", "Atlas Copco"),
    ("Transmitter", "Emerson"),
    ("Valve", "Honeywell"),
    ("Generator", "Siemens"),
    ("Exchanger", "Alfa Laval"),
    ("Pump", "Grundfos"),
    ("Motor", "ABB"),
    ("Separator", "Schlumberger"),
    ("Meter", "Endress+Hauser"),
    ("Actuator", "Rotork"),
]

_EQUIPMENT_PREFIXES = [
    "COMP",
    "FLOW",
    "VALVE",
    "TURB",
    "HX",
    "PUMP",
    "MOT",
    "SEP",
    "MTR",
    "ACT",
]

_SITES = [
    ("Houston Refinery", "29.7604,-95.3698"),
    ("Baytown Chemical Plant", "29.7355,-94.9774"),
    ("Texas City Terminal", "29.3838,-94.9027"),
    ("Corpus Christi Pipeline Hub", "27.8006,-97.3964"),
    ("Midland Pump Station", "31.9973,-102.0779"),
    ("Beaumont Tank Farm", "30.0802,-94.1266"),
    ("Galveston LNG Facility", "29.3013,-94.7977"),
    ("Permian Basin Gathering", "31.8457,-102.3676"),
    ("Eagle Ford Processing", "28.6880,-98.0883"),
    ("Port Arthur Refinery", "29.8850,-93.9399"),
    ("Lake Charles Terminal", "30.2266,-93.2174"),
    ("Baton Rouge Complex", "30.4515,-91.1871"),
    ("Pascagoula Refinery", "30.3658,-88.5561"),
    ("Mobile Gas Plant", "30.6954,-88.0399"),
    ("Pensacola Storage", "30.4213,-87.2169"),
    ("New Orleans Hub", "29.9511,-90.0715"),
    ("Shreveport Station", "32.5252,-93.7502"),
    ("Oklahoma City Junction", "35.4676,-97.5164"),
    ("Tulsa Metering", "36.1540,-95.9928"),
    ("Cushing Tank Farm", "35.9851,-96.7670"),
]

_AREAS = [
    "Processing Unit",
    "Storage Area",
    "Loading Dock",
    "Metering Station",
    "Wellhead Cluster",
    "Control Room",
    "Compressor Bay",
    "Tank Battery",
    "Flare Stack",
    "Cooling Tower",
    "Boiler House",
    "Electrical Substation",
    "Pig Launcher",
    "Separator Train",
    "Dehydration Unit",
]

_CUSTOMER_NAMES = [
    "Gulf Energy Partners",
    "Petrochem Industries",
    "Lone Star Utilities",
    "MidContinent Resources",
    "Atlantic Refining Co",
    "Southern Pipeline LLC",
    "Frontier Gas Corp",
    "Coastal Petroleum",
    "Prairie Wind Energy",
    "Summit Fuel Services",
    "Delta Downstream Inc",
    "Canyon Creek Oil",
    "Evergreen Power Systems",
    "Redstone Chemical Works",
    "Blue Ridge Natural Gas",
    "Pinnacle Energy Group",
    "Clearwater Refining",
    "Iron Horse Pipeline",
    "Silverado Resources",
    "Horizon Fuel Partners",
]

_INDUSTRIES = [
    "Oil & Gas",
    "Petrochemical",
    "Utilities",
    "Natural Gas",
    "Refining",
    "Pipeline",
    "Power Generation",
    "Chemical",
    "LNG",
    "Midstream",
]

_REGIONS = [
    "Gulf Coast",
    "Southeast",
    "Texas",
    "Midwest",
    "Northeast",
    "Southwest",
    "West Coast",
    "Mid-Atlantic",
    "Plains",
    "Mountain",
]

_MEASUREMENTS = [
    ("PRESSURE", "Discharge pressure", "PSI"),
    ("FLOW", "Volumetric flow rate", "BBL/D"),
    ("TEMP", "Temperature reading", "°F"),
    ("RPM", "Rotational speed", "RPM"),
    ("DELTA_T", "Temperature differential", "°F"),
    ("VIBRATION", "Vibration level", "mm/s"),
    ("DENSITY", "Fluid density", "kg/m³"),
    ("POSITION", "Actuator position", "%"),
    ("POWER", "Power output", "MW"),
    ("LEVEL", "Tank level", "ft"),
    ("CURRENT", "Motor current draw", "A"),
    ("VOLTAGE", "Supply voltage", "V"),
    ("TORQUE", "Shaft torque", "Nm"),
    ("PH", "pH level", "pH"),
    ("CONDUCTIVITY", "Conductivity", "µS/cm"),
]

# ---------------------------------------------------------------------------
# Build Equipment (50 records)
# ---------------------------------------------------------------------------

_NUM_EQUIPMENT = 50


def _build_equipment() -> dict[str, dict]:
    """Build 50 equipment records with deterministic type/manufacturer cycling."""
    data = {}
    base_date = date(2015, 1, 1)
    for i in range(1, _NUM_EQUIPMENT + 1):
        eid = f"EQ-{i:04d}"
        etype, manufacturer = _EQUIPMENT_TYPES[(i - 1) % len(_EQUIPMENT_TYPES)]
        # Spread install dates over ~7 years using a prime multiplier to avoid clustering.
        install_offset = (i * 37) % 2500
        data[eid] = {
            "EquipmentID": eid,
            "EquipmentName": f"{etype} Unit {i}",
            "EquipmentType": etype,
            "Manufacturer": manufacturer,
            "InstallDate": str(
                date.fromordinal(base_date.toordinal() + install_offset)
            ),
        }
    return data


EQUIPMENT = _build_equipment()
logger.debug("Built equipment records", extra={"record_count": len(EQUIPMENT)})

# ---------------------------------------------------------------------------
# Build Locations (20 records)
# ---------------------------------------------------------------------------


def _build_locations() -> dict[str, dict]:
    """Build 20 location records — one per site in the seed pool."""
    data = {}
    for i, (site, gps) in enumerate(_SITES, start=1):
        lid = f"LOC-{i:04d}"
        area = _AREAS[(i - 1) % len(_AREAS)]
        data[lid] = {
            "LocationID": lid,
            "SiteName": site,
            "Area": f"{area} {((i - 1) // len(_AREAS)) + 1}",
            "GPSCoordinates": gps,
        }
    return data


LOCATIONS = _build_locations()
logger.debug("Built location records", extra={"record_count": len(LOCATIONS)})

# ---------------------------------------------------------------------------
# Build Customers (20 records)
# ---------------------------------------------------------------------------


def _build_customers() -> dict[str, dict]:
    """Build 20 customer records — one per name in the seed pool."""
    data = {}
    for i, name in enumerate(_CUSTOMER_NAMES, start=1):
        cid = f"CUST-{i:04d}"
        data[cid] = {
            "CustomerID": cid,
            "CustomerName": name,
            "Industry": _INDUSTRIES[(i - 1) % len(_INDUSTRIES)],
            "ContactInfo": f"contact{i}@{name.lower().replace(' ', '').replace(',', '')}.com",
            "Region": _REGIONS[(i - 1) % len(_REGIONS)],
        }
    return data


CUSTOMERS = _build_customers()
logger.debug("Built customer records", extra={"record_count": len(CUSTOMERS)})

# ---------------------------------------------------------------------------
# Build Tags (5000 records)
# ---------------------------------------------------------------------------

_NUM_TAGS = 5000

# Pre-compute key lists so modulo assignment is index-stable.
_eq_ids = list(EQUIPMENT.keys())
_loc_ids = list(LOCATIONS.keys())
_cust_ids = list(CUSTOMERS.keys())


def _build_tags() -> dict[str, dict]:
    """Build 5,000 tag records with round-robin FK assignment.

    Each tag is linked to exactly one equipment, location, and customer via
    modulo cycling over the static lookup lists.  This ensures every FK in
    generated data will resolve to a valid parent record.
    """
    data = {}
    for i in range(1, _NUM_TAGS + 1):
        tid = f"TAG-{i:05d}"
        eq_id = _eq_ids[(i - 1) % len(_eq_ids)]
        loc_id = _loc_ids[(i - 1) % len(_loc_ids)]
        cust_id = _cust_ids[(i - 1) % len(_cust_ids)]

        eq_prefix = _EQUIPMENT_PREFIXES[(i - 1) % len(_EQUIPMENT_PREFIXES)]
        meas_suffix, description, unit = _MEASUREMENTS[(i - 1) % len(_MEASUREMENTS)]
        eq_num = ((i - 1) // len(_MEASUREMENTS)) + 1

        data[tid] = {
            "TagID": tid,
            "TagName": f"{eq_prefix}_{eq_num:03d}_{meas_suffix}",
            "Description": f"{EQUIPMENT[eq_id]['EquipmentType']} Unit {eq_num} {description.lower()}",
            "UnitOfMeasure": unit,
            "EquipmentID": eq_id,
            "LocationID": loc_id,
            "CustomerID": cust_id,
        }
    return data


TAGS = _build_tags()
logger.debug("Built tag records", extra={"record_count": len(TAGS)})
