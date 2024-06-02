with ga4_data AS 
(
SELECT   user_pseudo_id
        ,(SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS ga_session_id
FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*` 
GROUP BY 1,2
)


SELECT   t.*
        ,COUNT(DISTINCT user_pseudo_id) OVER(PARTITION BY ga_session_id) unique_user_cnt_per_one_session
FROM ga4_data t
WHERE ga_session_id IN 
                        (SELECT ga_session_id
                        FROM ga4_data
                        GROUP BY 1
                        HAVING COUNT(DISTINCT user_pseudo_id)>1
                        )
ORDER BY unique_user_cnt_per_one_session DESC,2