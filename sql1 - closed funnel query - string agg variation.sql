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

,one_user_item_timestamp_many_events AS (
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
                                                        FROM one_user_item_timestamp_many_events
                                                )
                  -- AND user_pseudo_id='84534898.7989426950' and item_name='Google Black Cork Journal'
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

SELECT   q.*
FROM 
        (SELECT   t.*
                 ,STRING_AGG(event_name) OVER(PARTITION BY  user_pseudo_id
                                                           ,event_timestamp_date
                                                           ,item_name 
                                                           
                                         ORDER BY           event_timestamp ASC
                                         ) event_seq
                 ,ROW_NUMBER() OVER( PARTITION BY   user_pseudo_id
                                                   ,event_timestamp_date
                                                   ,item_name
                                    ORDER BY        event_timestamp DESC) user_item_day_last_record
        FROM    user_event_timestamp_and_items t )q

WHERE user_item_day_last_record=1 -- here we see only the last sequnce for item,user,date
) 
 
,funnel_each_last_sequence_for_user_item_day AS (
SELECT           event_seq
                ,COUNT(DISTINCT CONCAT(user_pseudo_id,event_timestamp))
FROM            consecutive_funnel
GROUP BY        1
)

SELECT *
FROM funnel_each_last_sequence_for_user_item_day

