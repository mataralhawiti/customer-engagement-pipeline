# BigQuery Schema for Enriched Events

This folder contains BigQuery schema definitions and setup scripts for the enriched engagement events from the CDC pipeline.

## 📁 Files

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

## 🚀 Setup Instructions

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

# Or create a temporary file
sed "s/{BIGQUERY_PROJECT}/your-bigquery-project/g" create_table.sql > temp_create_table.sql
bq query --use_legacy_sql=false < temp_create_table.sql
rm temp_create_table.sql
```

### **Option 3: Using BigQuery Console**
1. Open [BigQuery Console](https://console.cloud.google.com/bigquery)
2. Create dataset: `engagement_analytics`
3. Copy/paste the SQL from `create_table.sql`
4. Run the query

## 📊 Table Features

### **Partitioning:**
- **Partitioned by** `DATE(event_ts)` for efficient time-based queries
- **Improves performance** for date range filters
- **Reduces costs** by scanning only relevant partitions

### **Clustering:**
- **Clustered by** `content_type, event_type`
- **Optimizes queries** that filter by content type or event type
- **Improves performance** for analytical queries

### **Labels:**
- `environment=production` - Environment identifier
- `source=cdc-pipeline` - Data source identifier

## 📈 Example Queries

### **Engagement by Content Type:**
```sql
SELECT 
  content_type,
  COUNT(*) as total_events,
  AVG(engagement_pct) as avg_engagement_pct,
  AVG(engagement_seconds) as avg_engagement_seconds
FROM `{BIGQUERY_PROJECT}.engagement_analytics.enriched_events`
WHERE DATE(event_ts) = CURRENT_DATE()
  AND engagement_pct IS NOT NULL
GROUP BY content_type
ORDER BY avg_engagement_pct DESC;
```

### **User Engagement Patterns:**
```sql
SELECT 
  user_id,
  COUNT(*) as total_events,
  COUNT(DISTINCT content_id) as unique_content,
  AVG(engagement_pct) as avg_engagement_pct
FROM `{BIGQUERY_PROJECT}.engagement_analytics.enriched_events`
WHERE DATE(event_ts) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
  AND event_type = 'play'
GROUP BY user_id
HAVING total_events >= 5
ORDER BY avg_engagement_pct DESC
LIMIT 100;
```

### **Content Performance:**
```sql
SELECT 
  content_id,
  content_type,
  length_seconds,
  COUNT(*) as play_events,
  AVG(engagement_pct) as avg_completion_pct,
  COUNTIF(engagement_pct >= 90) as high_engagement_users
FROM `{BIGQUERY_PROJECT}.engagement_analytics.enriched_events`
WHERE event_type = 'play'
  AND DATE(event_ts) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
  AND engagement_pct IS NOT NULL
GROUP BY content_id, content_type, length_seconds
HAVING play_events >= 10
ORDER BY avg_completion_pct DESC;
```

## 🔄 Data Pipeline Integration

To stream data from your CDC pipeline to BigQuery:

1. **BigQuery Sink** - Add BigQuery output to your Beam pipeline
2. **Dataflow** - Use Google Cloud Dataflow for managed streaming
3. **Pub/Sub to BigQuery** - Direct streaming from Pub/Sub topics
4. **Batch Loading** - Periodic batch loads from Redis/files

## 📋 Maintenance

- **Partition Management** - BigQuery auto-manages daily partitions
- **Cost Optimization** - Set partition expiration if needed
- **Schema Evolution** - Use `ALTER TABLE` for schema changes
- **Monitoring** - Set up BigQuery monitoring and alerting
