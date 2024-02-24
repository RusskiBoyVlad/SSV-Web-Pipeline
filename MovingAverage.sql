-- Step 1: Flatten the data and extract `ga_session_id` and `event_date`
WITH flattened_sessions AS (
    SELECT
      event_date,
      MAX(IF(ep.key = 'ga_session_id', CAST(ep.value.int_value AS STRING), NULL)) AS ga_session_id
    FROM
      `springfieldvolunteers.analytics_398716475.events_*`,
      UNNEST(event_params) AS ep
    WHERE _TABLE_SUFFIX BETWEEN '20240215' AND '20240223' 
    GROUP BY
      event_date, event_timestamp  -- Assuming event_timestamp is unique per session event
),

-- Step 2: Calculate session count per day
daily_sessions AS (
    SELECT
      event_date,
      COUNT(DISTINCT ga_session_id) AS session_count
    FROM flattened_sessions
    GROUP BY event_date
)

-- Step 3: Apply rolling window function
SELECT 
    event_date,
    session_count,
    AVG(session_count) OVER (ORDER BY event_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS rolling_avg_sessions
FROM daily_sessions
ORDER BY event_date;

