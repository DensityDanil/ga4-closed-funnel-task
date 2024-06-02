with test_data AS (
SELECT 'user_1' AS user_pseudo_id, TIMESTAMP('2020-11-05 08:00:00') AS event_timestamp, 'select_item' AS event_name, NULL AS event_params, ARRAY<STRUCT<item_name STRING>>[STRUCT('item_1')] AS items, NULL AS ecommerce
UNION ALL
SELECT 'user_1', TIMESTAMP('2020-11-05 08:00:00'), 'view_item', NULL, ARRAY<STRUCT<item_name STRING>>[STRUCT('item_1')], NULL
UNION ALL
SELECT 'user_1', TIMESTAMP('2020-11-05 08:05:00'), 'add_to_cart', NULL, ARRAY<STRUCT<item_name STRING>>[STRUCT('item_1')], NULL
UNION ALL
SELECT 'user_1', TIMESTAMP('2020-11-05 08:10:00'), 'purchase', NULL, ARRAY<STRUCT<item_name STRING>>[STRUCT('item_1')], NULL
UNION ALL
SELECT 'user_1', TIMESTAMP('2020-11-06 09:00:00'), 'select_item', NULL, ARRAY<STRUCT<item_name STRING>>[STRUCT('item_1')], NULL
UNION ALL
SELECT 'user_1', TIMESTAMP('2020-11-06 09:00:00'), 'view_item', NULL, ARRAY<STRUCT<item_name STRING>>[STRUCT('item_1')], NULL

)

SELECT *
FROM test_data