-- author: Qasim Ali Khan
-- https://www.linkedin.com/pulse/creating-ga4-funnels-big-query-qasim-ali-khan/
with ga4_data AS (
SELECT *
FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_2020*` 
WHERE user_pseudo_id='84534898.7989426950' 
)


,select_item AS (
SELECT 
      device.category AS category
     ,event_name
     ,(SELECT value.int_value FROM UNNEST(event_params) WHERE event_name = 'select_item' AND key = 'ga_session_id') AS step0_id,
  FROM
    ga4_data
  WHERE
    event_name = "select_item"
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

,add_to_cart AS (
  SELECT
     (SELECT value.int_value FROM UNNEST(event_params) WHERE event_name = 'add_to_cart' AND key = 'ga_session_id') AS step2_id
  FROM
    ga4_data
  WHERE
    event_name = "add_to_cart" 
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
SELECT      t0.*,t1.*,t2.*,t3.*
FROM        select_item     t0
LEFT JOIN   view_item       t1 ON step2_id = step0_id
LEFT JOIN   add_to_cart     t2 ON step2_id = step0_id
LEFT JOIN   purchase        t3 ON step3_id = step0_id

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