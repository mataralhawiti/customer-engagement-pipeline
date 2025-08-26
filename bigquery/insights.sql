--   Calculate the correlation between engagement percentage and content length, grouped by content type, to understand if longer content has lower engagement.
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


-- Find the top 5 content types with the highest average engagement percentage for each device type.
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

-- Identify the device type with the highest average engagement duration for 'play' events.
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

--   Calculate the standard deviation of engagement percentage for each device type, filtering out events with engagement durations less than 10 seconds.
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

-- Determine the median engagement duration (in seconds) for each content type, considering only events where the engagement percentage is greater than 50%.
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

-- Identify the top 3 content IDs with the highest average engagement duration, specifically for users on iOS devices.
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

-- Calculate the average engagement percentage for each content ID, only considering events where the content length is greater than 60 seconds
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


-- Determine the average engagement duration for each device type, and the standard deviation of engagement duration to understand engagement diversity.
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
  device

-- Determine the content with the maximum engagement percentage for each user, to identify their most engaging content.
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

--  Identify the content IDs with the highest variance in engagement percentage, indicating inconsistent user engagement.
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