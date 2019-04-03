# -*- coding: utf-8 -*-

import httplib2
import json
import re
from datetime import *

from configs import config, helpers
from linkedin.api import LinkedinAdsApi


def per_delta(start, end, delta):
    curr = start
    while curr < end:
        yield curr
        curr += delta


def extract_daily():
    dates = [s.strftime('%Y-%m-%d') for s in per_delta(
        date.today() - timedelta(days=helpers.DAYLOAD),
        date.today(),
        timedelta(days=1)
    )]
    start_date, end_date = dates[0], dates[-1]

    extract_data(start_date, end_date)


def extract_data(start_date='2017-01-04', end_date=date.today().strftime('%Y-%m-%d')):
# def extract_data(start_date='2019-04-01', end_date=date.today().strftime('%Y-%m-%d')):
    all_compaigns = get_all_campaignes_info()

    with open(config.DATA_PATH, mode='w', encoding='utf8') as raw_csv:
        writer = helpers.prepare_header_for_clear_csv(raw_csv, helpers.CSV_COLUMNS)

        for campaign in all_compaigns:
            daily_data = get_campaign_data(campaign, start_date, end_date)
            print(f'DATA COLUMNS --- {len(daily_data)}')

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


def get_campaign_data(campaign, start_date, end_date):
    start_year, start_month, start_day = start_date.split('-')
    end_year, end_month, end_day = end_date.split('-')


    credentials = helpers.get_client_config(
        conf_path=r'/Users/siromanto/ralabs/0.projects/conDati/LinkedinAds/configs/Linkedin1.json')
    # credentials = helpers.get_client_config(conf_path=r'/opt/workbench/users/afuser/airflow/dags/credentials/AmazonAdsKeys/Toweltech.json')

    access_token = credentials.get("access_token")
    access_headers = {'Authorization': 'Bearer {}'.format(access_token)}
    company_id = campaign["id"]

    api = LinkedinAdsApi(headers=access_headers)

    print('START WORKING WITH CAMPAIGN --- {}, ID --- {}, STATUS --- {}'.format(campaign['campaign_name'], campaign["id"], campaign["status"]))

    test_params = {
        'ACTIVE': [
            {"q": "statistics"},
            {"dateRange.start.month": start_month},
            {"dateRange.start.day": start_day},
            {"dateRange.start.year": start_year},
            {"dateRange.end.month": end_month},
            {"dateRange.end.day": end_day},
            {"dateRange.end.year": end_year},
            {"timeGranularity": "DAILY"},
            {"pivots": "ACCOUNT"},
            {"pivots": "SHARE"},
            {"campaigns": "urn:li:sponsoredCampaign:{}".format(company_id)}
        ],
        'OTHER': [
            {"q": "statistics"},
            {"dateRange.start.month": start_month},
            {"dateRange.start.day": start_day},
            {"dateRange.start.year": start_year},
            {"dateRange.end.month": end_month},
            {"dateRange.end.day": end_day},
            {"dateRange.end.year": end_year},
            {"timeGranularity": "DAILY"},
            {"pivots": "ACCOUNT"},
            {"campaigns": "urn:li:sponsoredCampaign:{}".format(company_id)}
        ],
    }

    if campaign["status"] == 'ACTIVE':
        query_params = "&".join("".join("{}={}".format(k, v) for k, v in t.items()) for t in test_params['ACTIVE'])
    else:
        query_params = "&".join("".join("{}={}".format(k, v) for k, v in t.items()) for t in test_params['OTHER'])

    daily_data = api.adAnalyticsV2(params=query_params).get("elements")
    return daily_data


def get_all_campaignes_info():
    statuses = ['ACTIVE', 'PAUSED', 'ARCHIVED', 'COMPLETED', 'CANCELED', 'DRAFT']
    all_compaigns = []

    for status in statuses:
        raw_data = get_data_from_response(status)

        data = [{
            "campaign_name": n['name'],
            'id': n['id'],
            'bid': n["unitCost"]["amount"],
            'bid_currency': n["unitCost"]["currencyCode"],
            'status': n['status'],
            'costType': n['costType'],
            'type': n['type'],
            'objectiveType': n['objectiveType'],
            'optimizationTargetType': n['optimizationTargetType']

        } for n in raw_data]

        print(f'STATUS --- {status}, AVAILABLE DATA --- {len(data)}')

        all_compaigns.extend(data)

    print(len(all_compaigns))
    return all_compaigns


def get_data_from_response(status):  # TODO: Refacor this
    h = httplib2.Http("/tmp/.cache", timeout=50)
    _uri = f"https://api.linkedin.com/v2/adCampaignsV2?q=search&search.status.values[0]={status}"

    resp, content = h.request(_uri, 'GET', headers={
        'Authorization': 'Bearer AQXm6MfmRtaAht_dlXzZ-nBMasAza-79lPP-9lpL08B-glGRDZaAgucdeBpB5Mob9lWjO7V8vzybzdeJDV1-jGa9Cu6c2u18iJdop_t0iNXWFQk9DKpJh3xasyMyLNhtPAMiwI-53puEbvkuviRi807cKrsojEgomSy2em7XJmvf_zQZZoHqJUHnJNKADP3fV3lxR6fWAM2dPhYN0XRXpQoOI6omPIDPtDfSX0nTV9VwNM1OdtpoTpPDdvCs9IzXeO976O8iFEe1N_E_-Hbp1OLcJ-FGt8btfBxVm0zepgMZQ9jRtimQm9AURXkdSs_9JhX1kSiD95yKH_qD0wJbEyzGdM4lOw'})

    data = json.loads(content.decode('utf-8')).get("elements")
    return data


if __name__ == '__main__':
    # extract_data()
    # extract_data(start_date='2017-01-04', end_date='2019-04-01')
    extract_daily()

