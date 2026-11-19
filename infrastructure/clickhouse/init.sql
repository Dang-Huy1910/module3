-- Migration: ClickHouse Analytical Tables for Realtime E-Commerce Platform
CREATE DATABASE IF NOT EXISTS analytics;

-- 1. Orders Analytical Table with ReplacingMergeTree for logical deduplication by event_id
CREATE TABLE IF NOT EXISTS analytics.orders (
    event_id String,
    event_time DateTime64(3, 'UTC'),
    order_id String,
    customer_id String,
    total_amount UInt64,
    currency LowCardinality(String),
    kafka_topic LowCardinality(String),
    kafka_partition UInt16,
    kafka_offset UInt64,
    loaded_at_utc DateTime64(3, 'UTC') DEFAULT now64(3, 'UTC')
) ENGINE = ReplacingMergeTree()
ORDER BY event_id;

-- 2. Product Views Analytical Table with ReplacingMergeTree
CREATE TABLE IF NOT EXISTS analytics.product_views (
    event_id String,
    event_time DateTime64(3, 'UTC'),
    customer_id String,
    product_id String,
    session_id String,
    device_type LowCardinality(String),
    kafka_topic LowCardinality(String),
    kafka_partition UInt16,
    kafka_offset UInt64,
    loaded_at_utc DateTime64(3, 'UTC') DEFAULT now64(3, 'UTC')
) ENGINE = ReplacingMergeTree()
ORDER BY event_id;
