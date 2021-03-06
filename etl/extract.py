# -*- coding: utf-8 -*-

import re
from datetime import *

from configs import config, helpers
from linkedin.api import LinkedinAdsApi
# from airflow import configuration as conf
# from LinkedinAds.configs import config, helpers
# from LinkedinAds.linkedin.api import LinkedinAdsApi


def prepare_api(client_name):
    credentials = helpers.get_client_config(config.CLIENT_CONFIG_PATH, client_name)
    access_token = credentials.get("access_token")
    access_headers = {'Authorization': 'Bearer {}'.format(access_token)}

    return LinkedinAdsApi(headers=access_headers)


def join_params_to_uri(params):
    return "&".join("".join("{}={}".format(k, v) for k, v in t.items()) for t in params)


def per_delta(start, end, delta):
    curr = start
    while curr < end:
        yield curr
        curr += delta


def extract_daily(client_name, **kwargs):
    dates = [s.strftime('%Y-%m-%d') for s in per_delta(
        date.today() - timedelta(days=helpers.DAYLOAD),
        date.today(),
        timedelta(days=1)
    )]
    start_date, end_date = dates[0], dates[-1]

    extract_data(client_name, start_date, end_date)


def extract_data(client_name, start_date='2017-01-04', end_date=date.today().strftime('%Y-%m-%d')):
    all_compaigns = get_all_campaignes_info(client_name)

    with open(config.DATA_PATH.format(client_name), mode='w', encoding='utf8') as raw_csv:
        writer = helpers.prepare_header_for_clear_csv(raw_csv, helpers.CSV_COLUMNS)

        for campaign in all_compaigns:
            daily_data = get_campaign_data(client_name, campaign, start_date, end_date)
            print('DATA COLUMNS --- {}'.format(len(daily_data)))

            for item in daily_data:
                row = {}
                row.update({
                    'DATE': "{}-{}-{}".format(
                        item["dateRange"]["start"]["year"],
                        item["dateRange"]["start"]["month"],
                        item["dateRange"]["start"]["day"]
                    ),
                    'campaign_name': campaign['campaign_name'],
                    'campaign_id': campaign['id'],
                    'bid': campaign['bid'],
                    'bid_currency': campaign['bid_currency'],
                    'status': campaign['status'].lower(),
                    'costType': campaign['costType'],
                    'type': campaign['type'],
                    'objectiveType': campaign['objectiveType'],
                    'optimizationTargetType': campaign['optimizationTargetType'],

                    'costInLocalCurrency': item.get('costInLocalCurrency'),
                    'shares': item.get('shares'),
                    'pivot': item.get('pivot'),
                    'likes': item.get('likes'),
                    'comments': item.get('comments'),
                    'costInUsd': item.get('costInUsd'),
                    'follows': item.get('follows'),
                    'conversionValueInLocalCurrency': item.get('conversionValueInLocalCurrency'),
                    'impressions': item.get('impressions'),
                    'opens': item.get('opens'),
                    'clicks': item.get('clicks'),
                    'totalEngagements': item.get('totalEngagements'),
                    'share_urn': re.search(r'\d+', item.get('pivotValues')[-1]).group() if len(item.get('pivotValues')) > 1 else ''
                })

                writer.writerow(row)
            print('*'*200)


def get_campaign_data(client_name, campaign, start_date, end_date):
    start_year, start_month, start_day = start_date.split('-')
    end_year, end_month, end_day = end_date.split('-')

    company_id = campaign["id"]
    api = prepare_api(client_name)
    print('START WORKING WITH CAMPAIGN --- {}, ID --- {}, STATUS --- {}'.format(campaign['campaign_name'], campaign["id"], campaign["status"]))

    uri_params = {
        'ACTIVE': [
            {'q': 'statistics'},
            {'dateRange.start.month': start_month},
            {'dateRange.start.day': start_day},
            {'dateRange.start.year': start_year},
            {'dateRange.end.month': end_month},
            {'dateRange.end.day': end_day},
            {'dateRange.end.year': end_year},
            {'timeGranularity': 'DAILY'},
            {'pivots': 'ACCOUNT'},
            {'pivots': 'SHARE'},
            {'campaigns': 'urn:li:sponsoredCampaign:{}'.format(company_id)}
        ],
        'OTHER': [
            {'q': "statistics"},
            {'dateRange.start.month': start_month},
            {'dateRange.start.day': start_day},
            {'dateRange.start.year': start_year},
            {'dateRange.end.month': end_month},
            {'dateRange.end.day': end_day},
            {'dateRange.end.year': end_year},
            {'timeGranularity': 'DAILY'},
            {'pivots': 'ACCOUNT'},
            {'campaigns': 'urn:li:sponsoredCampaign:{}'.format(company_id)}
        ],
    }

    if campaign["status"] == 'ACTIVE':
        query_params = join_params_to_uri(uri_params['ACTIVE'])
    else:
        query_params = join_params_to_uri(uri_params['OTHER'])

    daily_data = api.adAnalyticsV2(params=query_params).get("elements")
    return daily_data


def get_all_campaignes_info(client_name):
    statuses = ['ACTIVE', 'PAUSED', 'ARCHIVED', 'COMPLETED', 'CANCELED', 'DRAFT']
    all_compaigns = []

    for status in statuses:
        raw_data = get_data_from_response(status, client_name)

        if raw_data:
            data = [{
                "campaign_name": n['name'],
                'id': n['id'],
                'bid': n["unitCost"]["amount"],
                'bid_currency': n["unitCost"]["currencyCode"],
                'status': n['status'],
                'costType': n['costType'],
                'type': n['type'],
                'objectiveType': n.get('objectiveType', 'not set'),
                'optimizationTargetType': n.get('optimizationTargetType', 'not set')
            } for n in raw_data]

            print('STATUS --- {}, AVAILABLE DATA --- {}'.format(status, len(data)))

            all_compaigns.extend(data)

    print(len(all_compaigns))
    return all_compaigns


def get_data_from_response(status, client_name):
    api = prepare_api(client_name)
    client_config = helpers.get_client_config(config.CLIENT_CONFIG_PATH, client_name)

    uri_params = [
        {'q': 'search'},
        {'search.status.values[0]': status},
        {'search.account.values[0]': 'urn:li:sponsoredAccount:{}'.format(client_config['account_id'])}
    ]

    # Snowflake: 507488250
    # CrimChack: 502416102
    data = api.adCampaignsV2(params=join_params_to_uri(uri_params)).get("elements")
    return data


if __name__ == '__main__':
    # extract_data('snowflake')
    # extract_data('crimcheck')
    # extract_data(start_date='2019-05-26', end_date='2019-05-19')
    # extract_data('crimcheck', start_date='2018-06-04', end_date='2019-06-08')
    extract_daily('snowflake')
    # extract_daily('crimcheck')

