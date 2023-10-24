import pandas as pd
import duckdb 
import uuid 
from io import BytesIO
from dagster import (
    asset, 
    AssetIn,
    AssetOut,
)

@asset(
        key="pond_sessions", 
        ins={"sessions_df": AssetIn("gcs_io")},
        outs={"pageviews_df": AssetOut(io_manager_key="db_io")}
    )
def fill_pond_sessions(sessions_df) -> pd.DataFrame:
    sessions_cols_ordered = ['session_id','session_start_time','session_sequence_number','session_date','session_pageview_cnt','session_order_cnt','session_revenue','session_total_revenue','session_is_new_user','session_duration','session_os','session_is_mobile','session_device_category','session_browser','session_landing_screen','session_exit_screen','session_source','session_medium','session_marketing_channel','session_city','session_region','session_country']
    return sessions_df[sessions_cols_ordered]

@asset(
        key="pond_pageviews", 
        ins={"pageviews_df": AssetIn(io_manager_key="gcs_io")}, 
        outs={"pageviews_df": AssetOut(io_manager_key="db_io")}
    )
def fill_pond_pageviews(pageviews_df) -> pd.DataFrame:
    db = duckdb.connect(database=':memory:', read_only=False)
    db.register('session_pvs', pageviews_df)
    qry = """
        SELECT 
            sp.user_id,
            sp.session_id,
            sp.session_start_time,
            pv.pageview_timestamp,
            pv.hostname,
            pv.pagePath,
            pv.pageTitle,
            pv.pagePathLevel1,
            pv.pagePathLevel2,
            pv.pagePathLevel3,
            pv.pagePathLevel4,
            pv.total_product_impressions
        FROM session_pvs AS sp,
            UNNEST(session_pageviews) AS t(pv)
    """
    pageviews_df = db.query(qry).df() 
    db.close()
    pageviews_df['pageview_id'] = [uuid.uuid4().hex for _ in range(pageviews_df.shape[0])]
    return pageviews_df