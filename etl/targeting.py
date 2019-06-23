import re
from datetime import *
import httplib2
import json
import re

from configs import config, helpers
from linkedin.api import LinkedinAdsApi
# from airflow import configuration as conf
# from LinkedinAds.configs import config, helpers
# from LinkedinAds.linkedin.api import LinkedinAdsApi


def prepare_api(client_name):
    credentials = helpers.get_client_config(config.CLIENT_CONFIG_PATH, client_name)
    access_token = credentials.get("access_token")
    access_headers = {'Authorization': 'Bearer {}'.format(access_token),
                      'X-Restli-Protocol-Version': '2.0.0'}

    return LinkedinAdsApi(headers=access_headers)


def join_params_to_uri(params):
    return "&".join("".join("{}={}".format(k, v) for k, v in t.items()) for t in params)


def per_delta(start, end, delta):
    curr = start
    while curr < end:
        yield curr
        curr += delta


def extract_criteria_data(client_name, campaign_id):
    credentials = helpers.get_client_config(config.CLIENT_CONFIG_PATH, client_name)
    access_token = credentials.get("access_token")

    endpt = 'https://api.linkedin.com/v2/adCampaignsV2/{campaignID}'
    uri = endpt.format(campaignID=campaign_id)
    print(uri)

    h = httplib2.Http("/tmp/.cache", timeout=80)

    headers = {'Authorization': 'Bearer {}'.format(access_token),
               'Content-Type': 'application/json'}

    resp, content = h.request(uri, 'GET', headers=headers)

    data = json.loads(content.decode('utf-8'))

    print(data)

    targeting_сriteria = data.get('targetingCriteria')

    print(targeting_сriteria)

    target_params = transform_criteria(targeting_сriteria)
    return target_params

def transform_criteria(response_data):
    manual_str = "(include:(and:List((or:(urn%3Ali%3AadTargetingFacet%3Agroups:List(urn%3Ali%3Agroup%3A126178,urn%3Ali%3Agroup%3A163323,urn%3Ali%3Agroup%3A1824590,urn%3Ali%3Agroup%3A2668462,urn%3Ali%3Agroup%3A3766450,urn%3Ali%3Agroup%3A3907415,urn%3Ali%3Agroup%3A45624,urn%3Ali%3Agroup%3A63931))),(or:(urn%3Ali%3AadTargetingFacet%3Aseniorities:List(urn%3Ali%3Aseniority%3A8,urn%3Ali%3Aseniority%3A6,urn%3Ali%3Aseniority%3A4,urn%3Ali%3Aseniority%3A5,urn%3Ali%3Aseniority%3A9,urn%3Ali%3Aseniority%3A7))),(or:(urn%3Ali%3AadTargetingFacet%3Alocations:List(urn%3Ali%3Acountry%3Aca,urn%3Ali%3Acountry%3Aus))),(or:(urn%3Ali%3AadTargetingFacet%3AinterfaceLocales:List(urn%3Ali%3Alocale%3Aen_US))))))"
    output_str = str(response_data)

    substitutions = {
        "'": "",
        "{": "(",
        "}": ")",
        " ": "",
        "[": "List(",
        "]": ")",
        ":": "%3A"
    }

    parsed_str = output_str.replace("'", '').replace("{", '(').replace("}", ')').replace(' ', '').\
        replace('[', 'List(').replace(']', ')').replace(':', '%3A').replace('include%3A', 'include:').\
        replace('and%3A', 'and:').replace('or%3A', 'or:').replace('%3AList', ':List')

    # print('start working with origin string, {} with test string'.format('the same' if manual_str == output_str else 'not matches'))

    print(parsed_str == manual_str)

    print('test')
    return parsed_str


def get_target_criteria(client_name='snowflake', campaign_id='126706406'):
    api = prepare_api(client_name)
    target_params = extract_criteria_data(client_name, campaign_id)

    uri_params = [
        {'q': 'targetingCriteriaV2'},
        {'targetingCriteria': target_params}
    ]

    data = api.audienceCountsV2(params=join_params_to_uri(uri_params)).get("elements")
    return data



if __name__ == '__main__':
    # extract_data('snowflake', start_date='2018-06-04', end_date='2019-06-08')
    # extract_daily('snowflake')
    # extract_data('snowflake')
    get_target_criteria(client_name='snowflake', campaign_id='126706406')
