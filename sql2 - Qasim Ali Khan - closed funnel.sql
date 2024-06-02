-- author: Qasim Ali Khan
-- https://www.linkedin.com/pulse/creating-ga4-funnels-big-query-qasim-ali-khan/

SELECT
  category,
  COUNT(DISTINCT step1_id) AS view_item_events,
  COUNT(DISTINCT step2_id) AS begin_checkout_events,
  COUNT(DISTINCT step3_id) AS purchase_events
FROM (
  SELECT
     device.category AS category,
     (
  SELECT value.int_value
    FROM
    UNNEST(event_params)
    WHERE event_name = 'view_item' AND key = 'ga_session_id') AS step1_id,
    step2_id,
    step3_id
  FROM
    `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_2020*` 
  

LEFT JOIN (
  SELECT
     (
  SELECT value.int_value
    FROM
    UNNEST(event_params)
    WHERE event_name = 'begin_checkout' AND key = 'ga_session_id') AS step2_id
  FROM
    `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_2020*` 
  WHERE
    event_name = "begin_checkout" )
  ON
    (
  SELECT value.int_value
    FROM
    UNNEST(event_params)
    WHERE event_name = 'view_item' AND key = 'ga_session_id') = step2_id


 LEFT JOIN (
  SELECT
    (
  SELECT value.int_value
    FROM
    UNNEST(event_params)
    WHERE event_name = 'purchase' AND key = 'ga_session_id') AS step3_id
  FROM
    `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_2020*` 
  WHERE
    event_name = "purchase")
  ON
    (
  SELECT value.int_value
    FROM
    UNNEST(event_params)
    WHERE event_name = 'view_item' AND key = 'ga_session_id')  = step3_id


WHERE
    event_name = "view_item" )

GROUP BY
1;