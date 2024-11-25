SELECT 
  event_timestamp_utc,
  event_action,
  event_label,
  item_sku,
  visitor_id AS user_id,
  visit_id AS order_id,
  traffic_source,
  traffic_medium,
  user_country,
  device_category
FROM 
  `ecom-442020.ga_sessions.events_unnest`
;