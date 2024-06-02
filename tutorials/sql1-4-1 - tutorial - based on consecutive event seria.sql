with prepare_sample AS ( 
SELECT  
         user_pseudo_id 
        ,TIMESTAMP_MICROS(event_timestamp) event_timestamp 
        ,event_name

-- https://developers.google.com/analytics/bigquery/web-ecommerce-demo-dataset 
-- from 2020-11-01 to 2021-01-31 

FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_2020*` 
) 
 

,build_sequence_row_number AS (
SELECT subq2.*
      ,SUM( IF( event_name != prev_event_name ,1,0) ) OVER( PARTITION BY user_pseudo_id 
                                                                        ,event_timestamp_date 

                                                            ORDER BY     event_timestamp
                                                                        ) AS seq_id 
FROM (
        SELECT subq.*
              ,LAG(event_name) OVER( PARTITION BY user_pseudo_id
                                                 ,event_timestamp_date 
                                     ORDER BY event_timestamp ) prev_event_name
        FROM 
                  (SELECT  t.*
                          ,DATE(event_timestamp) AS event_timestamp_date
                  FROM prepare_sample t
                  ) AS subq

     ) subq2
)


,calc_event_duration AS(
SELECT   
         user_pseudo_id
        ,event_name
        ,event_timestamp_date
        ,seq_id
        ,DATE_DIFF(  MAX(event_timestamp)
                   , MIN(event_timestamp), SECOND) AS event_start_end_second_duration --review calc timestamp diff
FROM build_sequence_row_number
GROUP BY 1
        ,2
        ,3
        ,4
ORDER BY event_start_end_second_duration DESC
)


,daily_stats AS (
SELECT   t.*
        ,AVG(event_start_end_second_duration) OVER(PARTITION BY event_timestamp_date) all_events_per_day_avg_duration
FROM calc_event_duration t

)

,event_stats_for_all_the_time AS(
-- output example: https://docs.google.com/spreadsheets/d/1W0gWywlDRRIro0V4lQ0MrLNd_ITyleGC1LPyx5Rjac8/edit#gid=0
SELECT   event_start_end_second_duration
        ,COUNT(DISTINCT user_pseudo_id) user_pseudo_id_dcnt
FROM daily_stats
GROUP BY 1
)


,event_stats_daily AS(
-- this shows low values for durations
SELECT   event_timestamp_date
        ,event_name
        ,all_events_per_day_avg_duration
        ,AVG(event_start_end_second_duration) event_seconds_duration_avg

FROM    daily_stats
GROUP BY 1,2,3
ORDER BY 1 DESC,event_seconds_duration_avg ASC
)

SELECT *
FROM event_stats_daily