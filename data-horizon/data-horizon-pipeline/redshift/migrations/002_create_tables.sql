-- =============================================================================
-- Dimension tables (DISTSTYLE ALL — full copy on every node for join locality)
-- =============================================================================

CREATE TABLE IF NOT EXISTS staging.equipment (
    equipment_id    VARCHAR(20)   NOT NULL,
    equipment_name  VARCHAR(100),
    equipment_type  VARCHAR(50),
    manufacturer    VARCHAR(100),
    install_date    DATE,
    PRIMARY KEY (equipment_id)
)
DISTSTYLE ALL
SORTKEY (equipment_id);

CREATE TABLE IF NOT EXISTS staging.location (
    location_id     VARCHAR(20)   NOT NULL,
    site_name       VARCHAR(100),
    area            VARCHAR(100),
    gps_coordinates VARCHAR(50),
    PRIMARY KEY (location_id)
)
DISTSTYLE ALL
SORTKEY (location_id);

CREATE TABLE IF NOT EXISTS staging.customer (
    customer_id     VARCHAR(20)   NOT NULL,
    customer_name   VARCHAR(100),
    industry        VARCHAR(50),
    contact_info    VARCHAR(200),
    region          VARCHAR(50),
    PRIMARY KEY (customer_id)
)
DISTSTYLE ALL
SORTKEY (customer_id);

CREATE TABLE IF NOT EXISTS staging."tag" (
    tag_id          VARCHAR(20)   NOT NULL,
    tag_name        VARCHAR(100),
    description     VARCHAR(200),
    unit_of_measure VARCHAR(20),
    equipment_id    VARCHAR(20),
    location_id     VARCHAR(20),
    PRIMARY KEY (tag_id)
)
DISTSTYLE ALL
SORTKEY (tag_id);

-- =============================================================================
-- Fact tables (DISTKEY tag_id — collocates tag data on same node)
-- =============================================================================

CREATE TABLE IF NOT EXISTS staging.measurement (
    measurement_id  VARCHAR(20)       NOT NULL,
    tag_id          VARCHAR(20)       NOT NULL,
    timestamp       TIMESTAMP,
    value           DOUBLE PRECISION,
    quality_flag    VARCHAR(20),
    PRIMARY KEY (measurement_id)
)
DISTKEY (tag_id)
COMPOUND SORTKEY (tag_id, timestamp);

CREATE TABLE IF NOT EXISTS staging.alarm (
    alarm_id        VARCHAR(20)       NOT NULL,
    tag_id          VARCHAR(20)       NOT NULL,
    alarm_type      VARCHAR(30),
    threshold_value DOUBLE PRECISION,
    timestamp       TIMESTAMP,
    status          VARCHAR(20),
    PRIMARY KEY (alarm_id)
)
DISTKEY (tag_id)
COMPOUND SORTKEY (tag_id, timestamp);

CREATE TABLE IF NOT EXISTS staging.maintenance (
    maintenance_id   VARCHAR(20)   NOT NULL,
    tag_id           VARCHAR(20)   NOT NULL,
    work_order_id    VARCHAR(20),
    maintenance_date DATE,
    action_taken     VARCHAR(200),
    technician       VARCHAR(100),
    PRIMARY KEY (maintenance_id)
)
DISTKEY (tag_id)
COMPOUND SORTKEY (tag_id, maintenance_date);

CREATE TABLE IF NOT EXISTS staging.event (
    event_id    VARCHAR(20)   NOT NULL,
    tag_id      VARCHAR(20)   NOT NULL,
    event_type  VARCHAR(30),
    timestamp   TIMESTAMP,
    notes       VARCHAR(500),
    PRIMARY KEY (event_id)
)
DISTKEY (tag_id)
COMPOUND SORTKEY (tag_id, timestamp);

CREATE TABLE IF NOT EXISTS staging.customer_contract (
    contract_id         VARCHAR(20)       NOT NULL,
    customer_id         VARCHAR(20)       NOT NULL,
    tag_id              VARCHAR(20)       NOT NULL,
    contract_start_date DATE,
    contract_end_date   DATE,
    contract_volume     DOUBLE PRECISION,
    price_per_unit      DOUBLE PRECISION,
    PRIMARY KEY (contract_id)
)
DISTKEY (tag_id)
COMPOUND SORTKEY (tag_id, contract_start_date);

CREATE TABLE IF NOT EXISTS staging.billing (
    billing_id          VARCHAR(20)       NOT NULL,
    tag_id              VARCHAR(20)       NOT NULL,
    customer_id         VARCHAR(20)       NOT NULL,
    billing_period      VARCHAR(10),
    consumption_volume  DOUBLE PRECISION,
    total_amount        DOUBLE PRECISION,
    payment_status      VARCHAR(20),
    PRIMARY KEY (billing_id)
)
DISTKEY (tag_id)
COMPOUND SORTKEY (tag_id, billing_period);

CREATE TABLE IF NOT EXISTS staging.inventory (
    inventory_id     VARCHAR(20)   NOT NULL,
    tag_id           VARCHAR(20)   NOT NULL,
    material_type    VARCHAR(50),
    quantity         INTEGER,
    storage_location VARCHAR(100),
    last_updated     TIMESTAMP,
    PRIMARY KEY (inventory_id)
)
DISTKEY (tag_id)
COMPOUND SORTKEY (tag_id, last_updated);

CREATE TABLE IF NOT EXISTS staging.regulatory_compliance (
    compliance_id   VARCHAR(20)   NOT NULL,
    tag_id          VARCHAR(20)   NOT NULL,
    regulation_name VARCHAR(50),
    inspection_date DATE,
    status          VARCHAR(30),
    inspector       VARCHAR(100),
    PRIMARY KEY (compliance_id)
)
DISTKEY (tag_id)
COMPOUND SORTKEY (tag_id, inspection_date);

CREATE TABLE IF NOT EXISTS staging.financial_forecast (
    forecast_id          VARCHAR(20)       NOT NULL,
    tag_id               VARCHAR(20)       NOT NULL,
    forecast_date        DATE,
    expected_consumption DOUBLE PRECISION,
    expected_revenue     DOUBLE PRECISION,
    risk_factor          DOUBLE PRECISION,
    PRIMARY KEY (forecast_id)
)
DISTKEY (tag_id)
COMPOUND SORTKEY (tag_id, forecast_date);
