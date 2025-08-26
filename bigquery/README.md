# BigQuery Schema for Enriched Events

This folder contains BigQuery schema definitions and setup scripts for the enriched engagement events from the CDC pipeline.

## Files

- **`enriched_events_schema.json`** - JSON schema definition for programmatic table creation
- **`create_table.sql`** - SQL script for manual table creation with partitioning and clustering
- **`README.md`** - This documentation

## 🎯 Schema Overview

The enriched events table contains:

### **Original Event Fields:**
- `id` - Event identifier (INTEGER, required)
- `content_id` - Content UUID (STRING, required) 
- `user_id` - User UUID (STRING, required)
- `event_type` - Event type (STRING, required)
- `event_ts` - Event timestamp (TIMESTAMP, required)
- `duration_ms` - Engagement duration in ms (INTEGER, nullable)
- `device` - Device type (STRING, required)
- `raw_payload` - Raw event metadata JSON (STRING, required)

### **Enriched Fields:**
- `content_type` - Content type from metadata (STRING, nullable)
- `length_seconds` - Content length from metadata (INTEGER, nullable)

### **Derived Fields:**
- `engagement_seconds` - Derived from duration_ms (FLOAT, nullable)
- `engagement_pct` - Engagement percentage (FLOAT, nullable)

## Setup Instructions

### **Option 1: Using bq CLI**
```bash
# Set your BigQuery project ID
export BIGQUERY_PROJECT="your-bigquery-project"

# Create dataset (if not exists)
bq mk --dataset --description="Engagement analytics from CDC pipeline" \
  ${BIGQUERY_PROJECT}:engagement_analytics

# Create table using schema file
bq mk --table \
  --description="Enriched engagement events with content metadata" \
  --time_partitioning_field=event_ts \
  --time_partitioning_type=DAY \
  --clustering_fields=content_type,event_type \
  ${BIGQUERY_PROJECT}:engagement_analytics.enriched_events \
  enriched_events_schema.json
```

### **Option 2: Using Parameterized SQL**
```bash
# Set your BigQuery project ID and execute the create table script
export BIGQUERY_PROJECT="your-bigquery-project"
sed "s/{BIGQUERY_PROJECT}/${BIGQUERY_PROJECT}/g" create_table.sql | bq query --use_legacy_sql=false
```

### **Option 3: Using BigQuery Console**
1. Open [BigQuery Console](https://console.cloud.google.com/bigquery)
2. Create dataset: `engagement_analytics`
3. Copy/paste the SQL from `create_table.sql`
4. Run the query

## Table Features

### **Partitioning:**
- **Partitioned by** `DATE(event_ts)` for efficient time-based queries
- **Improves performance** for date range filters
- **Reduces costs** by scanning only relevant partitions

### **Clustering:**
- **Clustered by** `content_type, event_type`
- **Optimizes queries** that filter by content type or event type
- **Improves performance** for analytical queries