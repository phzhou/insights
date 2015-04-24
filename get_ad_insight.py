########## flow ##########
# get all ad set from an ad account
# store into database
########## flow ##########

from facebookads.api import FacebookAdsApi
from facebookads.objects import AdAccount

import MySQLdb as mdb
import sys

########################## parameter starts ##########################
my_app_id = '<APP_ID>'
my_app_secret = '<APP_SECRET>'
my_access_token = '<ACCESS_TOKEN>'
FacebookAdsApi.init(my_app_id, my_app_secret, my_access_token)

# list of ad account
my_ad_account_ids = [
    'act_<AD_ACCOUNT_ID>',
]

# report date
report_date = '<YYYY-MM-DD>' # YYYY-MM-DD

# db configuration
db_host = '<DB_HOST>'
db_username = '<DB_USERNAME>'
db_password = '<DB_PASSWORD>'
db_name = '<DB_NAME>'

# action types that we want to save to db
# format is action_type_returned_from_api: db_column_name
action_type_columns = {
    'link_click': 'website_clicks',
    'offsite_conversion.checkout': 'checkouts',
    'offsite_conversion.add_to_cart': 'adds_to_cart',
    'offsite_conversion.key_page_view': 'key_web_page_views',
    'offsite_conversion.lead': 'leads',
    'offsite_conversion.other': 'other_website_conversions',
    'offsite_conversion.registration': 'registrations',
    'app_custom_event.fb_mobile_purchase': 'mobile_purchase',
    'app_custom_event.fb_mobile_add_to_cart': 'mobile_add_to_cart',
    'mobile_app_install': 'mobile_app_install',
    'app_custom_event.fb_mobile_activate_app': 'mobile_activate_app',
}

# action values that we want to save to db
# format is action_value_returned_from_api: db_column_name
action_value_columns = { 
    'offsite_conversion': 'website_action_value',
    'app_custom_event.fb_mobile_purchase': 'mobile_purchase_value',
}
########################## parameter ends ##########################

########################## function starts ##########################
def writeAdInsight(ad_insight, con, report_date):
    impression_device = 0
    if 'impression_device' in ad_insight:
        impression_device = ad_insight['impression_device']

    action_device = 0
    if 'action_device' in ad_insight:
        action_device = ad_insight['action_device']

    key_value = {
        'start_date': report_date,
        'end_date': report_date,
        'campaign': ad_insight['campaign_group_name'],
        'adset': ad_insight['campaign_name'],
        'adset_id': ad_insight['campaign_id'],
        'impressions': ad_insight['impressions'],
        'clicks': ad_insight['clicks'],
        'spend': ad_insight['spend'],
        'reach': ad_insight['reach'],
        'impression_device': impression_device,
        'conversion_device': action_device
    }
    if 'actions' in ad_insight:
        actions = ad_insight['actions']
        for action in actions:
            t = action['action_type']
            if t in action_type_columns:
                key_value[action_type_columns[t]] = action['28d_click']

    if 'action_values' in ad_insight:
        action_values = ad_insight['action_values']
        for action_value in action_values:
            t = action['action_type']
            if t in action_value_columns:
                key_value[action_value_columns[t]] = action['28d_click']

    key_str = ""
    value_str = ""
    count = 0
    for (key, value) in key_value.items():
        if count > 0:
            key_str += ", "
            value_str += ", "
        key_str += key
        value_str += "'" + str(value) + "'"
        count += 1

    stat = "INSERT INTO ad_set_insight (" + key_str + ") VALUES (" + value_str + ")";

    cur = con.cursor()
    cur.execute(stat)

def deleteAdSetsInsight(con, report_date):
    stat = "DELETE FROM ad_set_insight WHERE start_date = '{0}' AND end_date = '{1}'".format(report_date, report_date)

    cur = con.cursor()
    cur.execute(stat)
########################## function ends ##########################

########################## main ##########################
# get all ad campaign from an ad account
con = mdb.connect(db_host, db_username, db_password, db_name)
with con:
    deleteAdSetsInsight(con, report_date)
    for my_ad_account_id in my_ad_account_ids:
        ad_account = AdAccount(my_ad_account_id)
        fields = [
            'campaign_group_name',
            'campaign_name',
            'campaign_id',
            'impressions',
            'clicks',
            'spend',
            'reach',
            'actions',
            'action_values'
        ]
        params = {
            'time_range': {
                'since': report_date,
                'until': report_date
            },
            'action_attribution_windows': ['28d_click'],
            'breakdowns': ['impression_device', 'placement'],
            'level': 'campaign',
            'limit': 100000
        }
        ad_insights = ad_account.get_insights(fields, params)

        for ad_insight in ad_insights:
            writeAdInsight(ad_insight, con, report_date)
