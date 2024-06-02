-- author: Qasim Ali Khan
-- https://www.linkedin.com/pulse/creating-ga4-funnels-big-query-qasim-ali-khan/
with ga4_data AS (
SELECT *
FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_2020*` 
)

,view_item AS (
SELECT 
      device.category AS category
     ,event_name
     ,(SELECT value.int_value FROM UNNEST(event_params) WHERE event_name = 'view_item' AND key = 'ga_session_id') AS step1_id,
  FROM
    ga4_data
  WHERE
    event_name = "view_item"
)

,begin_checkout AS (
  SELECT
     (SELECT value.int_value FROM UNNEST(event_params) WHERE event_name = 'begin_checkout' AND key = 'ga_session_id') AS step2_id
  FROM
    ga4_data
  WHERE
    event_name = "begin_checkout" 
)


,purchase AS (
  SELECT
    (SELECT value.int_value FROM UNNEST(event_params) WHERE event_name = 'purchase' AND key = 'ga_session_id') AS step3_id
  FROM 
    ga4_data
  WHERE
    event_name = "purchase"
)


,funnel_conditions AS (
SELECT      t1.*,t2.*,t3.*
FROM        view_item       t1 
LEFT JOIN   begin_checkout  t2 ON step2_id = step1_id
LEFT JOIN   purchase        t3 ON step3_id = step1_id

)

,funnel AS (
SELECT
  category,
  COUNT(DISTINCT step1_id) AS view_item_events,
  COUNT(DISTINCT step2_id) AS begin_checkout_events,
  COUNT(DISTINCT step3_id) AS purchase_events
FROM funnel_conditions
GROUP BY 1)

SELECT *
FROM funnel
;