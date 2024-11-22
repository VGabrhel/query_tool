SELECT
    hits.*
FROM
    `bigquery-public-data.google_analytics_sample.ga_sessions_20170801*`,
    UNNEST(hits) AS hits
LIMIT 100;