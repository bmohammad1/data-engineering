-- =============================================================================
-- Dimension tables (DISTSTYLE ALL — full copy on every node for join locality)
-- =============================================================================

CREATE TABLE IF NOT EXISTS staging.equipment (
    equipment_id    VARCHAR(20)   NOT NULL,
    equipment_name  VARCHAR(100)  NOT NULL,
    equipment_type  VARCHAR(50)   NOT NULL,
    manufacturer    VARCHAR(100)  NOT NULL,
    install_date    DATE          NOT NULL,
    _loaded_at      TIMESTAMP     DEFAULT GETDATE(),
    PRIMARY KEY (equipment_id)
)
DISTSTYLE ALL
SORTKEY (equipment_id);

CREATE TABLE IF NOT EXISTS staging.location (
    location_id     VARCHAR(20)   NOT NULL,
    site_name       VARCHAR(100)  NOT NULL,
    area            VARCHAR(100)  NOT NULL,
    gps_coordinates VARCHAR(50)   NOT NULL,
    _loaded_at      TIMESTAMP     DEFAULT GETDATE(),
    PRIMARY KEY (location_id)
)
DISTSTYLE ALL
SORTKEY (location_id);

CREATE TABLE IF NOT EXISTS staging.customer (
    customer_id     VARCHAR(20)   NOT NULL,
    customer_name   VARCHAR(100)  NOT NULL,
    industry        VARCHAR(50)   NOT NULL,
    contact_info    VARCHAR(200)  NOT NULL,
    region          VARCHAR(50)   NOT NULL,
    _loaded_at      TIMESTAMP     DEFAULT GETDATE(),
    PRIMARY KEY (customer_id)
)
DISTSTYLE ALL
SORTKEY (customer_id);

CREATE TABLE IF NOT EXISTS staging."tag" (
    tag_id          VARCHAR(20)   NOT NULL,
    tag_name        VARCHAR(100)  NOT NULL,
    description     VARCHAR(200)  NOT NULL,
    unit_of_measure VARCHAR(20)   NOT NULL,
    equipment_id    VARCHAR(20)   NOT NULL,
    location_id     VARCHAR(20)   NOT NULL,
    _loaded_at      TIMESTAMP     DEFAULT GETDATE(),
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
    timestamp       TIMESTAMP         NOT NULL,
    value           DOUBLE PRECISION  NOT NULL,
    quality_flag    VARCHAR(20)       NOT NULL,
    _loaded_at      TIMESTAMP         DEFAULT GETDATE(),
    PRIMARY KEY (measurement_id)
)
DISTKEY (tag_id)
COMPOUND SORTKEY (tag_id, timestamp);

CREATE TABLE IF NOT EXISTS staging.alarm (
    alarm_id        VARCHAR(20)       NOT NULL,
    tag_id          VARCHAR(20)       NOT NULL,
    alarm_type      VARCHAR(30)       NOT NULL,
    threshold_value DOUBLE PRECISION  NOT NULL,
    timestamp       TIMESTAMP         NOT NULL,
    status          VARCHAR(20)       NOT NULL,
    _loaded_at      TIMESTAMP         DEFAULT GETDATE(),
    PRIMARY KEY (alarm_id)
)
DISTKEY (tag_id)
COMPOUND SORTKEY (tag_id, timestamp);

CREATE TABLE IF NOT EXISTS staging.maintenance (
    maintenance_id   VARCHAR(20)   NOT NULL,
    tag_id           VARCHAR(20)   NOT NULL,
    work_order_id    VARCHAR(20)   NOT NULL,
    maintenance_date DATE          NOT NULL,
    action_taken     VARCHAR(200)  NOT NULL,
    technician       VARCHAR(100)  NOT NULL,
    _loaded_at       TIMESTAMP     DEFAULT GETDATE(),
    PRIMARY KEY (maintenance_id)
)
DISTKEY (tag_id)
COMPOUND SORTKEY (tag_id, maintenance_date);

CREATE TABLE IF NOT EXISTS staging.event (
    event_id    VARCHAR(20)   NOT NULL,
    tag_id      VARCHAR(20)   NOT NULL,
    event_type  VARCHAR(30)   NOT NULL,
    timestamp   TIMESTAMP     NOT NULL,
    notes       VARCHAR(500)  NOT NULL,
    _loaded_at  TIMESTAMP     DEFAULT GETDATE(),
    PRIMARY KEY (event_id)
)
DISTKEY (tag_id)
COMPOUND SORTKEY (tag_id, timestamp);

CREATE TABLE IF NOT EXISTS staging.customer_contract (
    contract_id         VARCHAR(20)       NOT NULL,
    customer_id         VARCHAR(20)       NOT NULL,
    tag_id              VARCHAR(20)       NOT NULL,
    contract_start_date DATE              NOT NULL,
    contract_end_date   DATE              NOT NULL,
    contract_volume     DOUBLE PRECISION  NOT NULL,
    price_per_unit      DOUBLE PRECISION  NOT NULL,
    _loaded_at          TIMESTAMP         DEFAULT GETDATE(),
    PRIMARY KEY (contract_id)
)
DISTKEY (tag_id)
COMPOUND SORTKEY (tag_id, contract_start_date);

CREATE TABLE IF NOT EXISTS staging.billing (
    billing_id          VARCHAR(20)       NOT NULL,
    tag_id              VARCHAR(20)       NOT NULL,
    customer_id         VARCHAR(20)       NOT NULL,
    billing_period      VARCHAR(10)       NOT NULL,
    consumption_volume  DOUBLE PRECISION  NOT NULL,
    total_amount        DOUBLE PRECISION  NOT NULL,
    payment_status      VARCHAR(20)       NOT NULL,
    _loaded_at          TIMESTAMP         DEFAULT GETDATE(),
    PRIMARY KEY (billing_id)
)
DISTKEY (tag_id)
COMPOUND SORTKEY (tag_id, billing_period);

CREATE TABLE IF NOT EXISTS staging.inventory (
    inventory_id     VARCHAR(20)   NOT NULL,
    tag_id           VARCHAR(20)   NOT NULL,
    material_type    VARCHAR(50)   NOT NULL,
    quantity         INTEGER       NOT NULL,
    storage_location VARCHAR(100)  NOT NULL,
    last_updated     TIMESTAMP     NOT NULL,
    _loaded_at       TIMESTAMP     DEFAULT GETDATE(),
    PRIMARY KEY (inventory_id)
)
DISTKEY (tag_id)
COMPOUND SORTKEY (tag_id, last_updated);

CREATE TABLE IF NOT EXISTS staging.regulatory_compliance (
    compliance_id   VARCHAR(20)   NOT NULL,
    tag_id          VARCHAR(20)   NOT NULL,
    regulation_name VARCHAR(50)   NOT NULL,
    inspection_date DATE          NOT NULL,
    status          VARCHAR(30)   NOT NULL,
    inspector       VARCHAR(100)  NOT NULL,
    _loaded_at      TIMESTAMP     DEFAULT GETDATE(),
    PRIMARY KEY (compliance_id)
)
DISTKEY (tag_id)
COMPOUND SORTKEY (tag_id, inspection_date);

CREATE TABLE IF NOT EXISTS staging.financial_forecast (
    forecast_id          VARCHAR(20)       NOT NULL,
    tag_id               VARCHAR(20)       NOT NULL,
    forecast_date        DATE              NOT NULL,
    expected_consumption DOUBLE PRECISION  NOT NULL,
    expected_revenue     DOUBLE PRECISION  NOT NULL,
    risk_factor          DOUBLE PRECISION  NOT NULL,
    _loaded_at           TIMESTAMP         DEFAULT GETDATE(),
    PRIMARY KEY (forecast_id)
)
DISTKEY (tag_id)
COMPOUND SORTKEY (tag_id, forecast_date);
