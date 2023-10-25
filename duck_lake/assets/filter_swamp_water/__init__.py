import pandas as pd
import json 
import ast 
import random 
import fsspec
import gcsfs
from datetime import datetime, timedelta

from ...configs import SwampConfig

from dagster import (
    op,
    In, 
    Out,
    Output,
    graph_asset,
    multi_asset,
    AssetIn,
    AssetOut,
)

@op(out=Out(pd.DataFrame))
def get_swamp_water(config: SwampConfig):
    assert config.ga_dump_file 
    ga_data = pd.read_csv('gs://data-swamp/' + config.ga_dump_file, encoding='utf-8')
    ga_data.rename(columns={'fullVisitorId': 'user_id', 'socialEngagementType':'user_social_engagement', 'customDimensions':'user_region','visitId':'session_id','visitStartTime':'session_start_time','hits':'session_events','date':'session_date','channelGrouping':'session_marketing_channel','visitNumber':'session_sequence_number'}, inplace=True)
    return Output(ga_data)

@op(ins={'ga_data': In(pd.DataFrame)}, out=Out(pd.DataFrame))
def filter_swamp_water(ga_data):
    ga_data['session_date'] = pd.to_datetime(ga_data['session_date'], format='%Y%m%d')
    ga_data['session_date'] = ga_data['session_date'].dt.strftime('%Y-%m-%d')
    ga_data['device'] = ga_data['device'].apply(lambda x: json.loads(x))
    ga_data['session_browser'] = ga_data['device'].apply(lambda x: x['browser'])
    ga_data['session_os'] = ga_data['device'].apply(lambda x: x['operatingSystem'])
    ga_data['session_is_mobile'] = ga_data['device'].apply(lambda x: x['isMobile'])
    ga_data['session_device_category'] = ga_data['device'].apply(lambda x: x['deviceCategory'])
    ga_data['geoNetwork'] = ga_data['geoNetwork'].apply(lambda x: json.loads(x))
    ga_data['session_country'] = ga_data['geoNetwork'].apply(lambda x: x['country'])
    ga_data['session_city'] = ga_data['geoNetwork'].apply(lambda x: x['city'])
    ga_data['session_region'] = ga_data['geoNetwork'].apply(lambda x: x['region'])
    ga_data['trafficSource'] = ga_data['trafficSource'].apply(lambda x: json.loads(x))
    ga_data['session_source'] = ga_data['trafficSource'].apply(lambda x: x['source'])
    ga_data['session_medium'] = ga_data['trafficSource'].apply(lambda x: x['medium'])
    ga_data['totals'] = ga_data['totals'].apply(lambda x: json.loads(x))
    ga_data['session_revenue'] = ga_data['totals'].apply(lambda x: (int(x['transactionRevenue'])/1000000) if 'transactionRevenue' in x else 0)
    ga_data['session_total_revenue'] = ga_data['totals'].apply(lambda x: (int(x['totalTransactionRevenue'])/1000000) if 'totalTransactionRevenue' in x else 0)
    ga_data['session_order_cnt'] = ga_data['totals'].apply(lambda x: int(x['transactions']) if 'transactions' in x else 0)
    ga_data['session_pageview_cnt'] = ga_data['totals'].apply(lambda x: int(x['pageviews']) if 'pageviews' in x else 0) 
    ga_data['session_duration'] = ga_data['totals'].apply(lambda x: int(x['timeOnSite']) if 'timeOnSite' in x else 0)
    ga_data['session_is_first_visit'] = ga_data['totals'].apply(lambda x: (int(x['newVisits'])==1) if 'newVisits' in x else False)
    ga_data.drop(columns=['device','geoNetwork','totals','trafficSource'], inplace=True)
    ga_data.loc[ga_data['session_city'].str.contains('available'), 'session_city'] = None
    ga_data.loc[ga_data['session_region'].str.contains('available'), 'session_region'] = None
    ga_data['session_medium'] = ga_data['session_medium'].replace('(none)', None)
    ga_data['session_source'] = ga_data['session_source'].replace('(direct)', 'direct')
    ga_data['session_events'] = ga_data['session_events'].apply(lambda x: ast.literal_eval(x))
    ga_data['session_landing_screen'] = ga_data['session_events'].apply(lambda x: {p['appInfo']['landingScreenName'] for p in x if 'appInfo' in p})
    ga_data['session_landing_screen'] = ga_data['session_landing_screen'].apply(lambda x: list(x)[0] if len(x) > 0 else None)
    ga_data['session_exit_screen'] = ga_data['session_events'].apply(lambda x: {p['appInfo']['exitScreenName'] for p in x if 'appInfo' in p})
    ga_data['session_exit_screen'] = ga_data['session_exit_screen'].apply(lambda x: list(x)[0] if len(x) > 0 else None)
    return Output(ga_data)

@graph_asset
def process_swamp_water():
    filtered_swamp_water = filter_swamp_water(get_swamp_water())
    return filtered_swamp_water 

@multi_asset(ins={"filtered_swamp_water":AssetIn("process_swamp_water")},outs={'sessions_df': AssetOut(io_manager_key="gcs_io"), 'events_df': AssetOut(io_manager_key="gcs_io")})
def get_filtered_swamp_water(filtered_swamp_water):

    def filter_keys(event_list):
        keys_to_keep = ['hour', 'minute', 'page', 'appInfo', 'product']
        return [{key: event[key] for key in keys_to_keep if key in event} for event in event_list]
    
    def construct_pageview_event_list(events, session_date):
        pageview_events = []
        for event in events:
            pageview_event = {}
            timestamp = pd.to_datetime(str(session_date) + ' ' + str(event['hour']) + ':' + str(event['minute']) + ':00', format='%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d %H:%M:%S')
            pageview_event['pageview_timestamp'] = timestamp
            for key in ['hostname', 'pagePath', 'pageTitle', 'pagePathLevel1', 'pagePathLevel2', 'pagePathLevel3', 'pagePathLevel4']:
                pageview_event[key] = event.get('page', {}).get(key, None)
            pageview_event['total_product_impressions'] = len(event.get('product', []))
            pageview_events.append(pageview_event)
        return pageview_events

    def adjust_timestamps(pageview_list):
        if len(pageview_list) <= 1:
            return pageview_list
        current_timestamp = datetime.strptime(pageview_list[0]['pageview_timestamp'], '%Y-%m-%d %H:%M:%S')
        for i in range(1, len(pageview_list)):
            minutes_increment = random.randint(1, 29)
            current_timestamp += timedelta(minutes=minutes_increment)
            pageview_list[i]['pageview_timestamp'] = current_timestamp.strftime('%Y-%m-%d %H:%M:%S')
        return pageview_list
    
    session_cols = ['session_marketing_channel','session_date','user_id','session_id','session_sequence_number','session_start_time','session_browser','session_os','session_is_mobile','session_device_category','session_country','session_city','session_region','session_source','session_medium','session_revenue','session_total_revenue','session_order_cnt','session_pageview_cnt','session_duration','session_is_first_visit','session_landing_screen','session_exit_screen']
    sessions_df = filtered_swamp_water[session_cols]
    event_cols = ['user_id','session_id','session_start_time','session_date','session_events'] 
    events_df = filtered_swamp_water[event_cols]
    events_df['session_events'] = events_df['session_events'].apply(filter_keys)
    events_df['session_pageviews'] = events_df.apply(lambda row: construct_pageview_event_list(row['session_events'], row['session_date']), axis=1)
    mask = events_df['session_pageviews'].apply(len) > 1
    events_df.loc[mask, 'session_pageviews'] = events_df.loc[mask, 'session_pageviews'].apply(adjust_timestamps)
    events_df.drop(columns=['session_events'], inplace=True)
    return sessions_df, events_df