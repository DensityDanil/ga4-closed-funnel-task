# main query  
`sql1 - closed funnel query.sql` - https://github.com/DensityDanil/ga4-closed-funnel-task/blob/main/sql1%20-%20closed%20funnel%20query.sql

# How Funnel Works
1. The funnel works for only those events: 'select_item', 'view_item', 'add_to_cart', 'purchase'.
2. All events are filtered to be consecutive in time for users.
3. The unit of calculation is user - event date - item (means that we analyze each user item for each user day).
4. For a sequence like `select_item,select_item,view_item,view_item` for 1 user, 1 item, and 1 day, then I zipped sequence to `select_item,view_item` and chose first by time[1].
5. For each day, for each item, for each user, I select only the last highest funnel sum by timestamp [2].
6. I filter events when they happen at the same time (I don't know what to do with them temporarily).
7. The query outputs a pivot by item name, which means that the total sum for each funnel step will be higher than real.
8. I decide to ignore session partition and search sequences during the day.

* [1] - run this code for better understanding: https://github.com/DensityDanil/ga4-closed-funnel-task/blob/main/tutorials/sql1-1%20-%20tutorial%20-%20match%20sequence%20using%20comulative%20sum%20and%20statement%20with%20partition.sql
* [2] - if one user have `select_item->view_item->add_to_cart->purchase` for one item,one day then funnel steps like down below will be ignored in the cte of `consecutive_funnel_processing`:
1. `select_item->view_item->add_to_cart`
2. `select_item->view_item`
3. `select_item`


## TODO
1. Add timezones relevant to country-city (using python libs like pycountry, countryinfo  etc.)
2. Is it correct to use 'select_item' at first?
3. Is it okay to calculate user all items?
4. Is it okay to calculate user items through days?
5. What if events have the same timestamp?
6. What if item name has many item IDs?
7. Why are all item IDs missing in the purchase stage when joining?
8. when `item_name` have many `item_id` and vise versa https://docs.google.com/spreadsheets/d/18kc-EcTmbUiWqSF3DgG7VpVQvVEawqYk7COvIiQb-Qs/edit#gid=0?
9. add cleanup for products like `Android Iconic 4" Decal` and `Android Iconic 4&quot; Decal`
10. why when add join statement with `item_id` then me cannot see any purchase funnel stage in `funnel_by_items_pivot`?

## Articles
1. [Closed Funnel in BigQuery Using GA4 Data](https://www.linkedin.com/pulse/creating-ga4-funnels-big-query-qasim-ali-khan/): 
* Session is not unique, and may appear for many different `user_pseudo_id`, so it would be better to review this query
* there is no consecutive order between events.
2. [Closed Funnel Description](https://www.optimizesmart.com/open-and-closed-funnels-in-ga4-with-examples/)
3. [Open Funnel vs. Closed Funnel in Google Analytics 4](https://www.analyticsmania.com/post/open-funnel-vs-closed-funnel-in-google-analytics-4/)
