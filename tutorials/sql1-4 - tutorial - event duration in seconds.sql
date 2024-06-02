with prepare_sample AS ( 
SELECT   subq.*
        ,DATE(event_timestamp) event_timestamp_date
        ,ROW_NUMBER() OVER( PARTITION BY DATE(event_timestamp),user_pseudo_id,event_name ORDER BY event_timestamp ASC) user_day_event_rn
FROM 
    (SELECT  
             user_pseudo_id 
            ,TIMESTAMP_MICROS(event_timestamp) event_timestamp 
            ,event_name
    
    -- https://developers.google.com/analytics/bigquery/web-ecommerce-demo-dataset 
    -- from 2020-11-01 to 2021-01-31 
    
    FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_2020*` 
    ) subq
) 
 


,event_duration AS
(SELECT  
         subq1.*
        ,DATE_DIFF(  event_timestamp_next 
                   , event_timestamp
                   , SECOND) AS event_seconds_duration -- experimental calc
FROM 
    (SELECT 
         t.*
        ,LEAD(event_timestamp) OVER( PARTITION BY user_pseudo_id,event_timestamp_date ORDER BY event_timestamp ) event_timestamp_next -- how to calc last row? simplify to min,max
    FROM prepare_sample t
    WHERE user_day_event_rn=1
    ) AS subq1
)


,daily_stats AS (
SELECT   t.*
        ,AVG(event_seconds_duration) OVER(PARTITION BY event_timestamp_date) all_events_per_day_avg_duration
FROM event_duration t

)

,event_stats AS
(SELECT  event_timestamp_date
        ,event_name
        ,all_events_per_day_avg_duration
        ,AVG(event_seconds_duration) event_seconds_duration_avg
        --,MEDIAN(event_seconds_duration)
        ,COUNT(DISTINCT user_pseudo_id) user_pseudo_id_dcnt

FROM    daily_stats
GROUP BY 1,2,3
ORDER BY 1 DESC,event_seconds_duration_avg ASC
)

,filtered_event_stats AS 
(SELECT *
FROM event_stats
WHERE event_name in ( 
                       'select_item' 
                     , 'view_item' 
                     , 'add_to_cart' 
                     , 'purchase') 
)


SELECT user_day_event_rn
FROM prepare_sample
GROUP BY 1