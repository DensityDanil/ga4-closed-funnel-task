# How Funnel Works
1. The funnel works for only those events: 'select_item', 'view_item', 'add_to_cart', 'purchase'.
2. All events are filtered to be consecutive in time for users.
3. The unit of calculation is user - event date - item (means that we analyze each user item for each user day).
4. For a sequence like `select_item,select_item,view_item,view_item` for 1 user, 1 item, and 1 day, then I filter it to `select_item,view_item`.
5. For each day, for each item, for each user, I select only the last highest funnel sum by timestamp.
6. I filter events when they happen at the same time (I don't know what to do with them temporarily).
7. The query outputs a pivot by item name, which means that the total sum for each funnel step will be higher than real.
8. I decide to ignore session partition and search sequences during the day.

## TODO
1. Add timezones relevant to country-city (using pycountry, countryinfo, etc.).


## Questions
1. Is it correct to use 'select_item' at first?
2. Is it okay to calculate user all items?
3. Is it okay to calculate user items through days?
4. What if events have the same timestamp?
6. What if item name has many item IDs?
7. Why are all item IDs missing in the purchase stage when joining?

## References
- [Closed Funnel in BigQuery Using GA4 Data](https://www.linkedin.com/pulse/creating-ga4-funnels-big-query-qasim-ali-khan/): 
* Session is not unique, so it would be better to review this query
* there is no consecutive order between events.

- [Closed Funnel Description](https://www.optimizesmart.com/open-and-closed-funnels-in-ga4-with-examples/)
- [Open Funnel vs. Closed Funnel in Google Analytics 4](https://www.analyticsmania.com/post/open-funnel-vs-closed-funnel-in-google-analytics-4/)
