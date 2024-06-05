-- output example: https://docs.google.com/spreadsheets/d/1pHL9ZqoUYdCOcUgByXOsgBR8sowVrjtZh-JWYmv7oVA/edit#gid=0

WITH data AS (
    SELECT '84534898.7989426950' AS user_pseudo_id, TIMESTAMP('2020-12-17 04:28:08.049080 UTC') AS event_timestamp, 'select_item' AS event_name, 'Google Black Cork Journal' AS item_name UNION ALL
    SELECT '84534898.7989426950' AS user_pseudo_id, TIMESTAMP('2020-12-17 04:28:08.049080 UTC') AS event_timestamp, 'view_item'   AS event_name, 'Google Black Cork Journal' AS item_name UNION ALL
    SELECT '84534898.7989426950' AS user_pseudo_id, TIMESTAMP('2020-12-17 04:38:36.881569 UTC') AS event_timestamp, 'view_item'   AS event_name, 'Google Black Cork Journal' AS item_name UNION ALL
    SELECT '84534898.7989426950' AS user_pseudo_id, TIMESTAMP('2020-12-17 04:38:43.972254 UTC') AS event_timestamp, 'view_item'   AS event_name, 'Google Black Cork Journal' AS item_name UNION ALL
    SELECT '84534898.7989426950' AS user_pseudo_id, TIMESTAMP('2020-12-17 04:42:25.473803 UTC') AS event_timestamp, 'select_item' AS event_name, 'Google Black Cork Journal' AS item_name UNION ALL
    SELECT '84534898.7989426950' AS user_pseudo_id, TIMESTAMP('2020-12-17 04:42:25.473803 UTC') AS event_timestamp, 'view_item'   AS event_name, 'Google Black Cork Journal' AS item_name UNION ALL
    SELECT '84534898.7989426950' AS user_pseudo_id, TIMESTAMP('2020-12-17 10:43:57.858017 UTC') AS event_timestamp, 'view_item'   AS event_name, 'Google Black Cork Journal' AS item_name UNION ALL
    SELECT '84534898.7989426950' AS user_pseudo_id, TIMESTAMP('2020-12-17 10:44:03.011708 UTC') AS event_timestamp, 'add_to_cart' AS event_name, 'Google Black Cork Journal' AS item_name UNION ALL
    SELECT '84534898.7989426950' AS user_pseudo_id, TIMESTAMP('2020-12-17 11:51:30.499570 UTC') AS event_timestamp, 'purchase'    AS event_name, 'Google Black Cork Journal' AS item_name UNION ALL
    SELECT '84534898.7989426950' AS user_pseudo_id, TIMESTAMP('2020-12-17 23:11:25.040744 UTC') AS event_timestamp, 'purchase'    AS event_name, 'Google Black Cork Journal' AS item_name UNION ALL
    SELECT '84534898.7989426950' AS user_pseudo_id, TIMESTAMP('2020-12-17 23:31:48.402142 UTC') AS event_timestamp, 'purchase'    AS event_name, 'Google Black Cork Journal' AS item_name UNION ALL
    SELECT '84534898.7989426950' AS user_pseudo_id, TIMESTAMP('2020-12-17 23:35:32.864993 UTC') AS event_timestamp, 'purchase'    AS event_name, 'Google Black Cork Journal' AS item_name UNION ALL
    SELECT '84534898.7989426950' AS user_pseudo_id, TIMESTAMP('2020-12-17 23:52:55.369926 UTC') AS event_timestamp, 'purchase'    AS event_name, 'Google Black Cork Journal' AS item_name UNION ALL
    SELECT '84534898.7989426950' AS user_pseudo_id, TIMESTAMP('2020-12-19 00:29:27.027164 UTC') AS event_timestamp, 'purchase'    AS event_name, 'Google Black Cork Journal' AS item_name
)


,user_item_day_ranking AS ( 
SELECT subq2.*
      ,SUM( IF( event_name != prev_event_name ,1,0) ) OVER( PARTITION BY user_pseudo_id                         -- this help match sequence like `select_item,select_item,view_item,view_item...` as 1,1,2,2...
                                                                        ,item_name
                                                                        ,event_timestamp_date 

                                                            ORDER BY     event_timestamp
                                                                        ,event_name_order_id ) event_seq_id_by_user_day_item
FROM (
        SELECT subq.*
              ,LAG(event_name) OVER( PARTITION BY user_pseudo_id
                                                 ,item_name
                                                 ,event_timestamp_date 
                                     ORDER BY event_timestamp,event_name_order_id ) prev_event_name
        FROM 
                  (SELECT  t.*
                          ,DATE(event_timestamp) AS event_timestamp_date
                          ,CASE   
                                  WHEN event_name = 'select_item'   THEN 1
                                  WHEN event_name = 'view_item'     THEN 2
                                  WHEN event_name = 'add_to_cart'   THEN 3
                                  WHEN event_name = 'purchase'      THEN 4                                      
                          END as event_name_order_id
                  FROM data t
                  ) AS subq

     ) subq2
ORDER BY user_pseudo_id,item_name,event_timestamp
) 


SELECT *
FROM user_item_day_ranking
