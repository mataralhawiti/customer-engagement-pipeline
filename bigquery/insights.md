# Basic Queries
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

# Insights

### **Engagement Correlation by Content Type and Device:**
Calculate the correlation between engagement percentage and content length, grouped by content type, to understand if longer content has lower engagement.
```sql
SELECT
  content_type,
  device,
  AVG(engagement_pct) AS avg_engagement_pct
FROM
  `engagement_analytics.enriched_events`
WHERE
  event_ts BETWEEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
  AND CURRENT_TIMESTAMP()
GROUP BY
  content_type,
  device;
```

### **90th Percentile Engagement by Content Type:**
Find the top 5 content types with the highest average engagement percentage for each device type.
```sql
SELECT
  content_type,
  APPROX_QUANTILES(engagement_seconds, 100)[
OFFSET
  (90)] AS percentile_90_engagement_seconds
FROM
  `engagement_analytics.enriched_events`
WHERE
  event_ts BETWEEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
  AND CURRENT_TIMESTAMP()
GROUP BY
  content_type;
```

### **Top Device by Engagement Duration:**
Identify the device type with the highest average engagement duration for 'play' events.
```sql
SELECT
  device,
  AVG(engagement_seconds) AS avg_engagement_seconds
FROM
  `engagement_analytics.enriched_events`
WHERE
  event_type = 'play'
  AND event_ts BETWEEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
  AND CURRENT_TIMESTAMP()
GROUP BY
  device
ORDER BY
  avg_engagement_seconds DESC
LIMIT
  1;
```

### **Engagement Variance by Device:**
Calculate the standard deviation of engagement percentage for each device type, filtering out events with engagement durations less than 10 seconds.
```sql
SELECT
  device,
  STDDEV_SAMP(engagement_pct) AS stddev_engagement_pct
FROM
  `engagement_analytics.enriched_events`
WHERE
  engagement_seconds > 10
  AND event_ts BETWEEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
  AND CURRENT_TIMESTAMP()
GROUP BY
  device;
```

### **Median Engagement for High-Engagement Content:**
Determine the median engagement duration (in seconds) for each content type, considering only events where the engagement percentage is greater than 50%.
```sql
SELECT
  content_type,
  APPROX_QUANTILES(engagement_seconds, 2)[
OFFSET
  (1)] AS median_engagement_seconds
FROM
  `engagement_analytics.enriched_events`
WHERE
  engagement_pct > 50
  AND event_ts BETWEEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
  AND CURRENT_TIMESTAMP()
GROUP BY
  content_type;
```

### **Top Content for iOS Users:**
Identify the top 3 content IDs with the highest average engagement duration, specifically for users on iOS devices.
```sql
SELECT
  content_id,
  AVG(engagement_seconds) AS avg_engagement_seconds
FROM
  `engagement_analytics.enriched_events`
WHERE
  device = 'ios'
  AND event_ts BETWEEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
  AND CURRENT_TIMESTAMP()
GROUP BY
  content_id
ORDER BY
  avg_engagement_seconds DESC
LIMIT
  3;
```

### **Long Content Engagement Analysis:**
Calculate the average engagement percentage for each content ID, only considering events where the content length is greater than 60 seconds.
```sql
SELECT
  content_id,
  AVG(engagement_pct) AS avg_engagement_pct
FROM
  `engagement_analytics.enriched_events`
WHERE
  length_seconds > 60
  AND event_ts BETWEEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
  AND CURRENT_TIMESTAMP()
GROUP BY
  content_id;
```

### **Device Engagement Statistics:**
Determine the average engagement duration for each device type, and the standard deviation of engagement duration to understand engagement diversity.
```sql
SELECT
  device,
  AVG(engagement_seconds) AS avg_engagement,
  STDDEV_SAMP(engagement_seconds) AS stddev_engagement
FROM
  `engagement_analytics.enriched_events`
WHERE
  event_ts BETWEEN TIMESTAMP('2023-01-01 00:00:00 UTC')
  AND TIMESTAMP('2023-01-31 23:59:59 UTC')
GROUP BY
  device;
```

### **User's Most Engaging Content:**
Determine the content with the maximum engagement percentage for each user, to identify their most engaging content.
```sql
SELECT
  user_id,
  ANY_VALUE(content_id
  HAVING
    MAX engagement_pct) AS most_engaging_content
FROM
  `engagement_analytics.enriched_events`
WHERE
  event_ts BETWEEN TIMESTAMP('2023-01-01 00:00:00 UTC')
  AND TIMESTAMP('2023-01-31 23:59:59 UTC')
GROUP BY
  user_id;
```

### **Content with Inconsistent Engagement:**
Identify the content IDs with the highest variance in engagement percentage, indicating inconsistent user engagement.
```sql
SELECT
  content_id,
  VAR_SAMP(engagement_pct) AS engagement_variance
FROM
  `engagement_analytics.enriched_events`
WHERE
  event_ts BETWEEN TIMESTAMP('2023-01-01 00:00:00 UTC')
  AND TIMESTAMP('2023-01-31 23:59:59 UTC')
GROUP BY
  content_id
ORDER BY
  engagement_variance DESC
LIMIT
  10;
```
