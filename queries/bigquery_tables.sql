CREATE OR REPLACE TABLE `ecom-442020.ga_sessions.events_unnest` AS 
  SELECT
  -- Human-readable timestamp
  TIMESTAMP_SECONDS(visitStartTime) AS event_timestamp_utc,

  -- Event information
  hits.eventInfo.eventCategory AS event_category,  -- Event category (e.g., 'Enhanced Ecommerce')
  hits.eventInfo.eventAction AS event_action,  -- Event action (e.g., 'Quickview Click', etc.)
  hits.eventInfo.eventLabel AS event_label,  -- Event label (e.g., 'Google Kick Ball' - can be null if the event is adding to the cart)

  -- Item-level information
  product.productSKU AS item_sku,
  product.v2ProductName AS item_name,
  product.v2ProductCategory AS item_category,
  -- First level of the category (before the first '/')
  SAFE_CAST(SPLIT(product.v2ProductCategory, '/')[SAFE_OFFSET(0)] AS STRING) AS item_category_l1,
  -- Second level of the category (between the first and second '/')
  SAFE_CAST(SPLIT(product.v2ProductCategory, '/')[SAFE_OFFSET(1)] AS STRING) AS item_category_l2,
  -- Third level of the category (between the second and third '/')
  SAFE_CAST(SPLIT(product.v2ProductCategory, '/')[SAFE_OFFSET(2)] AS STRING) AS item_category_l3,
  product.productPrice AS item_price,
  product.productQuantity AS item_quantity,
  
  -- Visit-level information
  fullVisitorId AS visitor_id,
  visitId AS visit_id,
  trafficSource.source AS traffic_source,
  trafficSource.medium AS traffic_medium,
  
  -- Transaction-level information
  hits.transaction.transactionId AS transaction_id,
  /*
  Calculate the revenue based on the business criteria - event
  */
  CASE
    WHEN hits.eventInfo.eventAction = 'Add to Cart' THEN product.productPrice
    ELSE hits.transaction.transactionRevenue 
  END AS transaction_revenue_currency,
  
  -- User-level information
  geoNetwork.country AS user_country,
  device.deviceCategory AS device_category
FROM
  `bigquery-public-data.google_analytics_sample.ga_sessions_*`
CROSS JOIN
  UNNEST(hits) AS hits
LEFT JOIN
  UNNEST(hits.product) AS product
WHERE
  hits.type IN ('EVENT')  -- Focus on event-based hits (e.g., views, clicks, add-to-cart)