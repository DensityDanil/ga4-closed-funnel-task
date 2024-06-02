with tcase2 AS (

  select 1 user_pseudo_id,  'select_item'   event_name,     1 event_timestamp,  1 as item_name union all
  select 1 user_pseudo_id,  'view_item'     event_name,     2 event_timestamp,  1 as item_name union all
  select 1 user_pseudo_id,  'select_item'   event_name,     3 event_timestamp,  2 as item_name union all
  select 1 user_pseudo_id,  'view_item'     event_name,     4 event_timestamp,  2 as item_name union all
  select 1 user_pseudo_id,  'add_to_cart'   event_name,     5 event_timestamp,  1 as item_name union all
  select 1 user_pseudo_id,  'purchase'      event_name,     6 event_timestamp,  1 as item_name union all
  select 1 user_pseudo_id,  'add_to_cart'   event_name,     7 event_timestamp,  2 as item_name union all
  select 1 user_pseudo_id,  'purchase'      event_name,     8 event_timestamp,  2 as item_name
)

, tcase_with_duplicates AS (

  select 1 user_pseudo_id,  'select_item'   event_name,     1 event_timestamp,  1 as item_name union all
  select 1 user_pseudo_id,  'view_item'     event_name,     2 event_timestamp,  1 as item_name union all
  select 1 user_pseudo_id,  'view_item'     event_name,     2 event_timestamp,  1 as item_name union all -- duplicate

  select 1 user_pseudo_id,  'select_item'   event_name,     3 event_timestamp,  2 as item_name union all
  select 1 user_pseudo_id,  'view_item'     event_name,     4 event_timestamp,  2 as item_name union all
  select 1 user_pseudo_id,  'add_to_cart'   event_name,     5 event_timestamp,  1 as item_name union all
  select 1 user_pseudo_id,  'purchase'      event_name,     6 event_timestamp,  1 as item_name union all
  select 1 user_pseudo_id,  'add_to_cart'   event_name,     7 event_timestamp,  2 as item_name union all
  select 1 user_pseudo_id,  'purchase'      event_name,     8 event_timestamp,  2 as item_name
)

, tcase_with_many_same_events AS (

  select 1 user_pseudo_id,  'select_item'   event_name,     1 event_timestamp,  1 as item_name union all
  select 1 user_pseudo_id,  'view_item'     event_name,     2 event_timestamp,  1 as item_name union all -- focus, view_item repeated twice at one timestamp
  select 1 user_pseudo_id,  'view_item'     event_name,     3 event_timestamp,  1 as item_name union all -- focus

  select 1 user_pseudo_id,  'select_item'   event_name,     3 event_timestamp,  2 as item_name union all
  select 1 user_pseudo_id,  'view_item'     event_name,     4 event_timestamp,  2 as item_name union all
  select 1 user_pseudo_id,  'add_to_cart'   event_name,     5 event_timestamp,  1 as item_name union all
  select 1 user_pseudo_id,  'purchase'      event_name,     6 event_timestamp,  1 as item_name union all
  select 1 user_pseudo_id,  'add_to_cart'   event_name,     7 event_timestamp,  2 as item_name union all
  select 1 user_pseudo_id,  'purchase'      event_name,     8 event_timestamp,  2 as item_name
)


, tcase_with_many_same_events_shuffled AS (

  SELECT *
  FROM 
      (select 1 user_pseudo_id,  'select_item'  event_name,    1 event_timestamp,  1 as item_name union all
      select 1 user_pseudo_id,  'view_item'     event_name,    1 event_timestamp,  1 as item_name union all
      select 1 user_pseudo_id,  'view_item'     event_name,     2 event_timestamp,  1 as item_name union all -- focus
      select 1 user_pseudo_id,  'view_item'     event_name,     3 event_timestamp,  1 as item_name union all -- focus

      select 1 user_pseudo_id,  'select_item'   event_name,     3 event_timestamp,  2 as item_name union all
      select 1 user_pseudo_id,  'view_item'     event_name,     4 event_timestamp,  2 as item_name union all
      select 1 user_pseudo_id,  'add_to_cart'   event_name,     5 event_timestamp,  1 as item_name union all
      select 1 user_pseudo_id,  'purchase'      event_name,     6 event_timestamp,  1 as item_name union all
      select 1 user_pseudo_id,  'add_to_cart'   event_name,     7 event_timestamp,  2 as item_name union all
      select 1 user_pseudo_id,  'purchase'      event_name,     8 event_timestamp,  2 as item_name) subq
  ORDER BY RAND()
)


,processing_seq_id AS (
SELECT subq1.*
      -- i skip date in partition
      ,SUM( IF( event_name != prev_event_name ,1,0) ) OVER( PARTITION BY user_pseudo_id,item_name ORDER BY event_timestamp,event_name_order_id ) seq_id
FROM (
      SELECT subq.*
            ,LAG(event_name) OVER( PARTITION BY user_pseudo_id,item_name ORDER BY event_timestamp,event_name_order_id ) prev_event_name
      FROM 
      (SELECT t.*
            ,CASE   
                  WHEN event_name = 'select_item'   THEN 1
                  WHEN event_name = 'view_item'     THEN 2
                  WHEN event_name = 'add_to_cart'   THEN 3
                  WHEN event_name = 'purchase'      THEN 4 -- custom sorting https://stackoverflow.com/questions/31978082/does-big-query-support-custom-sorting
            END as event_name_order_id
      FROM tcase_with_many_same_events_shuffled t) AS subq
      ) AS subq1
  ORDER BY user_pseudo_id,item_name,event_timestamp -- does this change ranking? - no  
)


SELECT *
FROM processing_seq_id