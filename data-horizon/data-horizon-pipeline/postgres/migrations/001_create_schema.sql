-- Migration 001: Create pipeline_audit schema
-- Run this against the RDS PostgreSQL instance after terraform apply.

CREATE SCHEMA IF NOT EXISTS pipeline_audit;
