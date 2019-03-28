import json, os
from datetime import timedelta, datetime
import re
import time
import csv
import snowflake.connector as connector

# DATETIME_FORMAT = '%m/%d/%Y'

# CSV_COLUMNS = ['DATE', 'campaign_name', 'campaign_id', 'bid', 'bid_currency', 'status',
#                'externalWebsitePostClickConversions', 'viralImpressions', 'adUnitClicks', 'companyPageClicks',
#                'viralOneClickLeads', 'textUrlClicks', 'costInLocalCurrency', 'viralLikes', 'viralOtherEngagements',
#                'viralExternalWebsiteConversions', 'shares', 'viralCardClicks', 'pivot', 'cardClicks',
#                'viralExternalWebsitePostViewConversions', 'viralTotalEngagements', 'viralCompanyPageClicks',
#                'likes', 'viralComments', 'actionClicks', 'viralShares', 'pivotValue', 'comments',
#                'externalWebsitePostViewConversions', 'costInUsd', 'oneClickLeads', 'landingPageClicks',
#                'viralCardImpressions', 'follows', 'oneClickLeadFormOpens', 'viralOneClickLeadFormOpens',
#                'conversionValueInLocalCurrency', 'viralFollows', 'impressions', 'otherEngagements',
#                'viralLandingPageClicks', 'viralExternalWebsitePostClickConversions', 'externalWebsiteConversions',
#                'cardImpressions', 'leadGenerationMailContactInfoShares', 'leadGenerationMailInterestedClicks',
#                'opens', 'clicks', 'totalEngagements', 'viralClicks', 'pivotValues_sponsoredAccount',
#                'pivotValues_share'
# ]

CSV_COLUMNS = ['DATE', 'campaign_name', 'campaign_id', 'bid', 'bid_currency', 'status', 'costType', 'type',
               'objectiveType', 'optimizationTargetType', 'costInLocalCurrency',
               'shares', 'pivot', 'likes', 'comments', 'costInUsd', 'follows', 'conversionValueInLocalCurrency',
               'impressions', 'opens', 'clicks', 'totalEngagements', 'share_urn'
]


RAW_DB_COLUMNS = \
    """
    DATE date, 
    campaign_name string,
    campaign_id string,
    bid float,
    bid_currency string,
    status string,
    costType string,
    type string,
    objectiveType string,
    optimizationTargetType string,
    costInLocalCurrency float, 
    shares integer, 
    pivot string, 
    likes integer, 
    comments integer,
    costInUsd float, 
    follows integer, 
    conversionValueInLocalCurrency string, 
    impressions integer, 
    opens integer, 
    clicks integer, 
    totalEngagements integer,
    share_urn string
    """

#
# RAW_DB_COLUMNS = \
#     """
#     externalWebsitePostClickConversions integer,
#     viralImpressions integer,
#     DATE date,
#     adUnitClicks integer,
#     companyPageClicks integer,
#     viralOneClickLeads integer,
#     textUrlClicks integer,
#     costInLocalCurrency string,
#     viralLikes integer,
#     viralOtherEngagements integer,
#     viralExternalWebsiteConversions integer,
#     shares integer,
#     viralCardClicks integer,
#     pivot string,
#     cardClicks integer,
#     viralExternalWebsitePostViewConversions integer,
#     viralTotalEngagements integer,
#     viralCompanyPageClicks integer,
#     likes integer,
#     viralComments integer,
#     actionClicks integer,
#     viralShares integer,
#     pivotValue string,
#     comments integer,
#     externalWebsitePostViewConversions integer,
#     costInUsd integer,
#     oneClickLeads integer,
#     landingPageClicks integer,
#     viralCardImpressions integer,
#     follows integer,
#     oneClickLeadFormOpens integer,
#     viralOneClickLeadFormOpens integer,
#     conversionValueInLocalCurrency string,
#     viralFollows integer,
#     impressions integer,
#     otherEngagements integer,
#     viralLandingPageClicks integer,
#     viralExternalWebsitePostClickConversions integer,
#     externalWebsiteConversions integer,
#     cardImpressions integer,
#     leadGenerationMailContactInfoShares integer,
#     leadGenerationMailInterestedClicks integer,
#     opens integer,
#     clicks integer,
#     totalEngagements integer,
#     viralClicks integer,
#     pivotValues_sponsoredAccount string,
#     pivotValues_share string
#     """

QUERY_STATS_DB_COLUMNS = \
    """SOURCE string, 
    DATE date, 
    TARGET_URL string,
    TOTAL_PAGE_AVG_CLICK_POSITION integer,
    TOTAL_PAGE_AVG_IMPRESSION_POSITION integer,
    TOTAL_PAGE_CLICS integer,
    TOTAL_PAGE_IMPRESSIONS integer,
    QUERY_AVG_CLICK_POSITION integer,
    QUERY_AVG_IMPRESSION_POSITION integer,
    QUERY_CLICKS integer,
    QUERY_IMPRESSIONS integer, 
    QUERY string
    """

V16_DB_COLUMNS = \
    """
    DATE date, 
    PROVIDER string, 
    MEDIUM string,
    CHANNELGROUPING string, 
    DEVICECATEGORY string,
    KEYWORD string,
    FULLURL string,
    ADCLICKS integer,
    IMPRESSIONS integer,
    ISDATAGOLDEN varchar,
    LANDINGPAGEPATH string,
    SOURCE string,
    CAMPAIGN string,
    ADGROUP string,
    SUBCHANNEL string,
    PROFILEID integer,
    CAMPAIGNSTATUS string,
    ADGROUPSTATUS string,
    ADSTATUS string,
    ADCONTENT string,
    PROVIDERCAMPAIGN string,
    EMAILCAMPAIGN string,
    BOUNCES integer,
    TRANSACTIONS integer,
    SESSIONS integer,
    SESSIONDURATION integer,
    USERS integer,
    NEWUSERS integer,
    TRANSACTIONREVENUE integer,
    PAGEVIEWS integer,
    ADCOST NUMBER(38,4),
    IMPRESSIONSHARE varchar,
    SAMPLINGLEVEL string,
    EMAILCLICKS integer,
    EMAILOPENS integer,
    EMAILUNIQUESENDS integer,
    EMAILSENDS integer,
    EMAILHARDBOUNCES integer,
    EMAILSOFTBOUNCES integer,
    EMAILOTHERBOUNCES integer,
    EMAILUNSUBSCRIBES integer,
    VIDEOVIEWS integer,

    goal1completions integer,
    goal2completions integer,
    goal3completions integer,
    goal4completions integer,
    goal5completions integer,
    goal6completions integer,
    goal7completions integer,
    goal8completions integer,
    goal9completions integer,
    goal10completions integer,
    goal11completions integer,
    goal12completions integer,
    goal13completions integer,
    goal14completions integer,
    goal15completions integer,
    goal16completions integer,
    goal17completions integer,
    goal18completions integer,
    goal19completions integer,
    goal20completions integer
    """


# def normalize_backfill_start_end_time(start_date, end_date):
#     end_time = (end_date + timedelta(days=1) - timedelta(seconds=1)).strftime(DATETIME_FORMAT)
#     start_time = start_date.strftime(DATETIME_FORMAT)
#     return start_time, end_time


def establish_db_conn(user, password, account, db, warehouse):
    conn = connector.connect(
        user=user,
        password=password,
        account=account
    )
    conn.cursor().execute('USE DATABASE {}'.format(db))
    conn.cursor().execute('USE WAREHOUSE {}'.format(warehouse))
    return conn


def get_client_config(conf_path, client_name=None):
    if client_name is None:
        with open(conf_path, 'r') as f:
            conf = json.load(f)
    else:
        with open(conf_path, 'r') as f:
            conf = json.load(f).get(client_name)
    assert conf is not None
    return conf


def perform_db_routines(client_name, sql):
    # configfile = get_resource_path()[0]

    # client_config = get_client_config(client_name, configfile)
    client_config = get_client_config(
        r'/Users/siromanto/ralabs/0.projects/conDati/LinkedinAds/configs/Linkedin.json')
    config_db = get_client_config(
        '/Users/siromanto/ralabs/0.projects/conDati/LinkedinAds/configs/Siromanto_account.json')

    conn = establish_db_conn(config_db['user'],
                             config_db['pwd'],
                             config_db['account'],
                             client_config['raw_db'],
                             client_config['warehouse'])
    conn.autocommit(False)
    curr = conn.cursor()
    queries_list = sql.split(';')
    try:
        curr.execute('BEGIN')
        for q in queries_list:
            curr.execute(q)
        curr.execute('COMMIT')
    finally:
        curr.close()
        conn.close()


def get_data_by_chunks(items_list, n):
    for i in range(0, len(items_list), n):
        yield items_list[i:i + n]


def parse_date(string_date):
    a = re.search(r'\d+', string_date)
    timestamp = a.group(0)
    return time.strftime("%Y-%m-%d", time.gmtime(int(timestamp) / 1000.0))


def prepare_header_for_clear_csv(file, headers):
    writer = csv.DictWriter(file, fieldnames=headers)
    writer.writeheader()
    return writer
