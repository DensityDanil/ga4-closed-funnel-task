with prepare_sample AS ( 
SELECT  
         user_pseudo_id 
        ,TIMESTAMP_MICROS(event_timestamp) event_timestamp 
        ,event_name 
        ,event_params 
        ,items 
        ,ecommerce 
 
-- https://developers.google.com/analytics/bigquery/web-ecommerce-demo-dataset 
-- from 2020-11-01 to 2021-01-31 
 
FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_2020*` 
WHERE  _table_suffix  
--  todo: timezone normalization?
    -- between '0101' and '1031'  --> There is no data to display.
    BETWEEN '1101' and '1231'
    AND event_name in ( 
                       'select_item' 
                     , 'view_item' 
                     , 'add_to_cart' 
                     , 'purchase') 
) 
 
 
 
,unnested_items AS ( 
SELECT   
         t1.user_pseudo_id 
        ,t1.event_timestamp 
        ,t1.event_name
        ,DATE(t1.event_timestamp) AS event_timestamp_date
        ,t2.item_name 
        -- revenue? 
FROM prepare_sample t1 
LEFT JOIN UNNEST(items) t2 
GROUP BY 1 
        ,2 
        ,3 
        ,4
        ,5
) 

,one_user_date_timestamp_many_events AS (
-- rows=324,073
SELECT   
         user_pseudo_id
        ,item_name
        ,event_timestamp
FROM unnested_items
GROUP BY 1,2,3
HAVING COUNT(DISTINCT event_name)>1
)

,user_item_day_ranking AS ( 
SELECT subq2.*
      ,SUM( IF( event_name != prev_event_name ,1,0) ) OVER( PARTITION BY user_pseudo_id                         -- this help match sequence like `select_item,select_item,view_item,view_item...` as 1,1,2,2...
                                                                        ,item_name
                                                                        ,event_timestamp_date 

                                                            ORDER BY     event_timestamp
                                                                        ,event_name_order_id ) user_pseudo_id_rn 
FROM (
        SELECT subq.*
              ,LAG(event_name) OVER( PARTITION BY user_pseudo_id
                                                 ,item_name
                                                 ,event_timestamp_date 
                                     ORDER BY event_timestamp,event_name_order_id ) prev_event_name
        FROM 
                  (SELECT t.*
                          ,CASE   
                                  WHEN event_name = 'select_item'   THEN 1
                                  WHEN event_name = 'view_item'     THEN 2
                                  WHEN event_name = 'add_to_cart'   THEN 3
                                  WHEN event_name = 'purchase'      THEN 4                                      -- if similar timestamp on two or more events event -> i decide to build order
                          END as event_name_order_id
                  FROM unnested_items t
                  WHERE  (user_pseudo_id,item_name,event_timestamp)
                                                NOT IN (
                                                        SELECT (user_pseudo_id,item_name,event_timestamp)
                                                        FROM one_user_date_timestamp_many_events
                                                )
                  -- WHERE user_pseudo_id='84534898.7989426950' and item_name='Google Black Cork Journal'
                  ) AS subq
         -- does this change ranking? - no  
     ) subq2
ORDER BY user_pseudo_id,item_name,event_timestamp
) 
 

,user_event_timestamp_and_items  AS (
SELECT *
FROM 
        (SELECT   t.*
                 ,ROW_NUMBER() OVER( PARTITION BY  user_pseudo_id
                                                  ,event_timestamp_date
                                                  ,item_name
                                                  ,event_name
                                                  ,user_pseudo_id_rn
                                     ORDER BY event_timestamp ) user_item_decrease_duplicated_consecutive_events -- this help decrease duplicated events like `select_item,select_item,view_item,view_item...` -> `select_item,view_item...`
                -- one user many purchases after select->view->add->purchase example: https://docs.google.com/spreadsheets/d/1R9e9_j5ZgYKEKmw5izQuYmhoZ0XCEGKkg0qEsporwCE/edit#gid=1034448389&range=B1
        FROM user_item_day_ranking t) subq
WHERE user_item_decrease_duplicated_consecutive_events=1
ORDER BY user_pseudo_id,item_name,event_timestamp_date,user_pseudo_id_rn
)
 
 
--  what about sequence of `select_item->select_item->view_item->add_to_cart->add_to_cart->purchase`??? 
,consecutive_funnel AS  (
SELECT   
         t1.* 
        ,t2.event_timestamp AS view_item_event_timestamp 
        ,t3.event_timestamp AS add_to_cart_event_timestamp 
        ,t4.event_timestamp AS purchase_event_timestamp 

-- take a look at `(t(N+1).user_pseudo_id_rn - t(N).user_pseudo_id_rn)=1` in join 
FROM (SELECT  *  
      FROM  user_event_timestamp_and_items  
      WHERE  event_name = 'select_item') AS t1 -- V there problem cause for 1 event we can join two next rows
 
LEFT JOIN user_event_timestamp_and_items AS t2 
    ON  t1.user_pseudo_id = t2.user_pseudo_id 
    AND t2.event_name = 'view_item' 
    AND t2.item_name = t1.item_name
    AND t2.event_timestamp_date = t1.event_timestamp_date
    AND (t2.user_pseudo_id_rn - t1.user_pseudo_id_rn)=1 
 
LEFT JOIN user_event_timestamp_and_items AS t3 
    ON  t1.user_pseudo_id = t3.user_pseudo_id 
    AND t3.event_name = 'add_to_cart' 
    AND t3.item_name = t2.item_name 
    AND t3.event_timestamp_date = t2.event_timestamp_date
    AND (t3.user_pseudo_id_rn - t2.user_pseudo_id_rn)=1 
 
LEFT JOIN user_event_timestamp_and_items AS t4 
    ON  t1.user_pseudo_id = t4.user_pseudo_id 
    AND t4.event_name = 'purchase' 
    AND t4.item_name = t3.item_name 
    AND t4.event_timestamp_date = t3.event_timestamp_date
    AND (t4.user_pseudo_id_rn - t3.user_pseudo_id_rn)=1 
 

) 
 
-- when two different events have same timestamp? 
,consecutive_funnel_processing AS (
SELECT *
FROM (
        SELECT
                 subq1.*
                ,ROW_NUMBER() OVER( PARTITION BY user_pseudo_id 
                                                ,item_name -- hard moment: is it ok to left other user items?
                                                ,event_timestamp_date 
                                    ORDER BY user_item_funnel_sum DESC ) AS max_user_item_day_funnel_sum
                -- what if duplicates?
                ,COUNT(1) OVER(PARTITION BY user_pseudo_id,event_timestamp) user_select_item_freq -- optional 
 
        FROM 
                (SELECT 
                         t.*
                        ,IF(event_timestamp                     IS NOT NULL,    1,      0)
                                +IF(view_item_event_timestamp   IS NOT NULL,    1,      0)
                                +IF(add_to_cart_event_timestamp IS NOT NULL,    1,      0)
                                +IF(purchase_event_timestamp    IS NOT NULL,    1,      0) AS user_item_funnel_sum -- this help avoid count of `selet_item,view_item` AND `selet_item,view_item,add_to_cart` for one user i one day; 
                FROM consecutive_funnel t 
                ) AS subq1
     ) AS subq2
WHERE max_user_item_day_funnel_sum=1
)

-- one user may have many items 
,funnel_by_items_pivot AS(
SELECT  
         item_name 
        ,COUNT(DISTINCT CONCAT(user_pseudo_id,event_timestamp))             AS select_item 
        ,COUNT(DISTINCT CONCAT(user_pseudo_id,view_item_event_timestamp))   AS view_item 
        ,COUNT(DISTINCT CONCAT(user_pseudo_id,add_to_cart_event_timestamp)) AS add_to_cart 
        ,COUNT(DISTINCT CONCAT(user_pseudo_id,purchase_event_timestamp))    AS purchase 
FROM consecutive_funnel_processing 
GROUP BY 1 
ORDER BY purchase DESC,select_item DESC,view_item DESC,add_to_cart DESC
)

SELECT *
FROM funnel_by_items_pivot


-- ,user_item_properties AS
-- (SELECT   item_name
--         ,user_pseudo_id
--         ,COUNT(DISTINCT event_timestamp)                AS select_item
--         ,COUNT(DISTINCT view_item_event_timestamp)      AS view_item
--         ,COUNT(DISTINCT add_to_cart_event_timestamp)    AS add_to_cart
--         ,COUNT(DISTINCT purchase_event_timestamp)       AS purchase 
-- FROM consecutive_funnel_processing
-- GROUP BY 1,2)

-- ,properties_pivot AS 
-- (SELECT 
--          select_item 
--         ,view_item 
--         ,add_to_cart 
--         ,purchase
--         ,COUNT(DISTINCT CONCAT(item_name,user_pseudo_id))
-- FROM user_item_properties
-- GROUP BY 1,2,3,4)


-- ,user_lookup AS 
-- (SELECT *
-- FROM consecutive_funnel_processing
-- WHERE user_pseudo_id IN (
--                                         SELECT user_pseudo_id
--                                         FROM   consecutive_funnel_processing
--                                         GROUP BY 1
--                                         HAVING  COUNT(DISTINCT event_timestamp_date)=1 
--                                                 AND COUNT(DISTINCT add_to_cart_event_timestamp)>1
--                                         -- ORDER BY purchase_event_timestamp
--                                         LIMIT 1
--                         )
--         AND add_to_cart_event_timestamp IS NOT NULL
-- )

-- ,user_day_item AS (
-- SELECT   user_pseudo_id
--         ,event_timestamp_date
--         ,item_name
--         ,COUNT(DISTINCT event_timestamp) event_timestamp_dcnt
--         ,STRING_AGG(event_name ORDER BY event_timestamp ASC) event_seq
-- FROM user_event_timestamp_and_items
-- GROUP BY 1,2,3)


-- ,analyse_view_item_select_item AS
-- (SELECT   event_seq
--         ,COUNT(DISTINCT event_timestamp_dcnt)
--         ,CASE
--                 WHEN STRPOS(event_seq, 'view_item')<STRPOS(event_seq, 'select_item') THEN 'view first'
--                 WHEN STRPOS(event_seq, 'view_item')>STRPOS(event_seq, 'select_item') THEN 'select first'
--         END AS which_event_first
-- FROM user_day_item
-- WHERE 
--         (  event_seq LIKE '%view_item,select_item%' 
--         OR event_seq LIKE '%select_item,view_item%'
--         )
-- GROUP BY event_seq
-- ORDER BY 2 DESC,which_event_first DESC
-- )
