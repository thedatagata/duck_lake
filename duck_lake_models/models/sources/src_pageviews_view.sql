WITH 
    raw_page_views_deduped
        AS (
            {{ dbt_utils.deduplicate(
                relation=source('duck_lake', 'pond_pageviews'),
                partition_by='user_id, pageview_id',
                order_by="pageview_timestamp desc",
            )
            }}
        )
SELECT * 
FROM raw_page_views_deduped p