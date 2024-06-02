with user_event AS (

  select 1 user_pseudo_id,'a' event_name,1 event_timestamp union all
  select 1 user_pseudo_id,'c' event_name,1 event_timestamp union all
  select 1 user_pseudo_id,'b' event_name,1 event_timestamp union all
  select 1 user_pseudo_id,'e' event_name,1 event_timestamp union all

  select 1 user_pseudo_id,'e' event_name,2 event_timestamp union all
  select 1 user_pseudo_id,'a' event_name,2 event_timestamp 
)

select t.*
      ,ROW_NUMBER() OVER( PARTITION BY user_pseudo_id ORDER BY 
                                                                 event_timestamp
                                                                ,CASE   
                                                                      WHEN event_name='a' THEN 1
                                                                      WHEN event_name='b' THEN 2
                                                                      WHEN event_name='c' THEN 3
                                                                      WHEN event_name='d' THEN 4
                                                                      WHEN event_name='e' THEN 5
                                                                END ASC -- https://stackoverflow.com/questions/31978082/does-big-query-support-custom-sorting
      
       ) 
from user_event t