-- Create the "orders" table summarizing orders based on transactions where the action is "Add to Cart"
CREATE OR REPLACE TABLE KEBOOLA_9900.WORKSPACE_94831764."orders" AS
  SELECT
    -- Extract the order date from the event timestamp
    TO_DATE(TO_TIMESTAMP("event_timestamp_utc", 'YYYY-MM-DD HH24:MI:SS.FF6 TZD')) AS "order_date",
    -- Use the visit ID as the unique order ID
    "visit_id" AS "order_id",
    -- Map the visitor ID to the user ID
    "visitor_id" AS "user_id",
    -- Calculate the total revenue for the order
    SUM("transaction_revenue_currency") AS "revenue_sum"
  FROM 
    KEBOOLA_9900.WORKSPACE_94831764."transactions"
  WHERE 
    -- Only include events where the action is "Add to Cart"
    "event_action" = 'Add to Cart'
  GROUP BY 
    ALL
;

-- Create the "items" table containing unique item details from the transactions
CREATE OR REPLACE TABLE KEBOOLA_9900.WORKSPACE_94831764."items" AS
  SELECT DISTINCT
    -- Include unique identifiers and attributes for each item
    "item_sku",
    "item_name",
    "item_category_l1",
    "item_category_l2",
    "item_category_l3",
    "item_price"
  FROM 
    KEBOOLA_9900.WORKSPACE_94831764."transactions"
;

-- Create the "orders_items" table mapping orders to items based on "Add to Cart" events
CREATE OR REPLACE TABLE KEBOOLA_9900.WORKSPACE_94831764."orders_items" AS
  SELECT DISTINCT
    -- Extract the order date from the event timestamp
    TO_DATE(TO_TIMESTAMP("event_timestamp_utc", 'YYYY-MM-DD HH24:MI:SS.FF6 TZD')) AS "order_date",
    -- Use the visit ID as the unique order ID
    "visit_id" AS "order_id",
    -- Map the visitor ID to the user ID
    "visitor_id" AS "user_id",
    -- Include the item SKU for the added item
    "item_sku"
  FROM 
    KEBOOLA_9900.WORKSPACE_94831764."transactions"
  WHERE 
    -- Only include events where the action is "Add to Cart"
    "event_action" = 'Add to Cart'
;

-- Create the "orders_items_all" table combining details from "orders_items" and "items"
CREATE OR REPLACE TABLE KEBOOLA_9900.WORKSPACE_94831764."orders_items_all" AS
  SELECT DISTINCT
    -- Include order details
    "orders_items"."order_date",
    "orders_items"."order_id",
    "orders_items"."user_id",
    -- Include item details
    "items"."item_sku",
    "items"."item_name",
    "items"."item_category_l1",
    "items"."item_category_l2",
    "items"."item_category_l3",
    "items"."item_price"
  FROM 
    KEBOOLA_9900.WORKSPACE_94831764."items"
  LEFT JOIN 
    KEBOOLA_9900.WORKSPACE_94831764."orders_items"
        ON "items"."item_sku" = "orders_items"."item_sku"
  WHERE 
    -- Filter out items with missing top-level category information
    "items"."item_category_l1" != ''
;

/*
This query extracts unique transaction entries by combining visit ID and visitor ID.
The goal is to identify the first L2 category of interest per given customer and order.
It ensures that each combination has a single row based on the earliest timestamp, 
with ties broken randomly using a uniform distribution. The output includes the 
timestamp, order ID, user ID, and second-level item category for further analysis.
Only rows with a non-empty first-level item category are considered valid for inclusion.
*/

SELECT 
    TO_TIMESTAMP("event_timestamp_utc", 'YYYY-MM-DD HH24:MI:SS.FF6 TZD') AS "timestamp",
    "visit_id" AS "order_id",
    "visitor_id" AS "user_id",
    "item_category_l2"
FROM 
    KEBOOLA_9900.WORKSPACE_94831764."transactions"
WHERE 
    "item_category_l1" != ''
QUALIFY 
    ROW_NUMBER() OVER (PARTITION BY "visit_id", "visitor_id"  ORDER BY "timestamp", UNIFORM(0, 1, RANDOM())) = 1 
;