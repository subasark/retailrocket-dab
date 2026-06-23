-- Databricks notebook source
CREATE OR REPLACE TABLE poc_prod.retail_rpt.fact_clickstream AS

SELECT
    visitor_id,
    item_id,
    event_type,
    transaction_id,
    event_timestamp,
    CAST(event_timestamp AS DATE) AS event_date
FROM poc_prod.retail_primary.clickstream_clean;

-- COMMAND ----------

CREATE OR REPLACE TABLE poc_prod.retail_rpt.customer_funnel AS

SELECT
    event_type,
    COUNT(*) AS total_events,
    COUNT(DISTINCT visitor_id) AS unique_visitors
FROM poc_prod.retail_rpt.fact_clickstream
GROUP BY event_type;

-- COMMAND ----------

CREATE OR REPLACE TABLE poc_prod.retail_rpt.product_performance AS

SELECT
    item_id,

    SUM(CASE WHEN event_type='view' THEN 1 ELSE 0 END) views,

    SUM(CASE WHEN event_type='addtocart' THEN 1 ELSE 0 END) carts,

    SUM(CASE WHEN event_type='transaction' THEN 1 ELSE 0 END) purchases

FROM poc_prod.retail_rpt.fact_clickstream

GROUP BY item_id;

-- COMMAND ----------

CREATE OR REPLACE TABLE poc_prod.retail_rpt.session_metrics AS

WITH base AS (

SELECT
    visitor_id,
    event_timestamp,

    LAG(event_timestamp)
    OVER(
        PARTITION BY visitor_id
        ORDER BY event_timestamp
    ) prev_event

FROM poc_prod.retail_rpt.fact_clickstream

),

session_flags AS (

SELECT
    *,

    CASE
        WHEN prev_event IS NULL THEN 1

        WHEN
            TIMESTAMPDIFF(
                MINUTE,
                prev_event,
                event_timestamp
            ) > 30
        THEN 1

        ELSE 0
    END AS new_session

FROM base

)

SELECT
    visitor_id,
    SUM(new_session) AS total_sessions
FROM session_flags
GROUP BY visitor_id;