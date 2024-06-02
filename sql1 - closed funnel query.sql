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
 
 
 
,user_event_timestamp AS ( 
-- here row number help to enumerate sequence based on needed event_name list and respective timestamps 
SELECT   subq.* 
        ,ROW_NUMBER() OVER( PARTITION BY user_pseudo_id ORDER BY event_timestamp ) user_pseudo_id_rn -- ties cases i ignore here temporary 
         
FROM  
    (SELECT  user_pseudo_id 
            ,event_timestamp 
            ,event_name 
    FROM prepare_sample 
    GROUP BY 1,2,3) subq 
) 
 
,unnested_items AS ( 
SELECT   
         t1.user_pseudo_id 
        ,t1.event_timestamp 
        ,t1.event_name 
        ,t2.item_id 
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
 
,user_event_timestamp_and_items AS ( 
-- join user events with items important after row_number() above 
SELECT    
         t1.* 
        ,t2.item_name 
         
FROM user_event_timestamp t1 
LEFT JOIN unnested_items t2 
    USING(user_pseudo_id 
         ,event_timestamp 
         ,event_name) 
GROUP BY 1 
        ,2 
        ,3 
        ,4 
        ,5 
) 
 
 
--  what about sequence of `select_item->select_item->view_item->add_to_cart->add_to_cart->purchase`??? 
,consecutive_funnel AS  
(SELECT   
         t1.* 
        ,t2.event_timestamp AS view_item_event_timestamp 
        ,t3.event_timestamp AS add_to_cart_event_timestamp 
        ,t4.event_timestamp AS purchase_event_timestamp 
        ,COUNT(1) OVER(PARTITION BY t1.user_pseudo_id) user_freq -- optional 
 
-- take a look at `(t(N+1).user_pseudo_id_rn - t(N).user_pseudo_id_rn)=1` in join 
FROM (SELECT  *  
      FROM  user_event_timestamp_and_items  
      WHERE  event_name = 'select_item') t1 
 
LEFT JOIN user_event_timestamp_and_items t2 
    ON  t1.user_pseudo_id = t2.user_pseudo_id 
    AND t2.event_name = 'view_item' 
    AND t2.item_name = t1.item_name 
    AND (t2.user_pseudo_id_rn - t1.user_pseudo_id_rn)=1 
 
LEFT JOIN user_event_timestamp_and_items t3 
    ON  t1.user_pseudo_id = t3.user_pseudo_id 
    AND t3.event_name = 'add_to_cart' 
    AND t3.item_name = t2.item_name 
    AND (t3.user_pseudo_id_rn - t2.user_pseudo_id_rn)=1 
 
LEFT JOIN user_event_timestamp_and_items t4 
    ON  t1.user_pseudo_id = t4.user_pseudo_id 
    AND t4.event_name = 'purchase' 
    AND t4.item_name = t3.item_name 
    AND (t4.user_pseudo_id_rn - t3.user_pseudo_id_rn)=1 
 
ORDER BY user_freq DESC 
) 
 
-- when two different events have same timestamp? 
 
-- one user may have many items 
SELECT  
         item_name 
        ,COUNT(DISTINCT CONCAT(user_pseudo_id,event_timestamp))             AS select_item 
        ,COUNT(DISTINCT CONCAT(user_pseudo_id,view_item_event_timestamp))   AS view_item 
        ,COUNT(DISTINCT CONCAT(user_pseudo_id,add_to_cart_event_timestamp)) AS add_to_cart 
        ,COUNT(DISTINCT CONCAT(user_pseudo_id,purchase_event_timestamp))    AS purchase 
FROM consecutive_funnel 
GROUP BY 1 
ORDER BY purchase DESC