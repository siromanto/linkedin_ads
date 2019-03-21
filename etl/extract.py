import httplib2
import json
from urllib.parse import urlencode

from configs import config, helpers

# TEMP
import httplib2
import json
from urllib.parse import urlencode

CSV_COLUMNS = ['externalWebsitePostClickConversions', 'viralImpressions', 'DATE', 'adUnitClicks', 'companyPageClicks',
               'viralOneClickLeads', 'textUrlClicks', 'costInLocalCurrency', 'viralLikes', 'viralOtherEngagements',
               'viralExternalWebsiteConversions', 'shares', 'viralCardClicks', 'pivot', 'cardClicks',
               'viralExternalWebsitePostViewConversions', 'viralTotalEngagements', 'viralCompanyPageClicks',
               'likes', 'viralComments', 'actionClicks', 'viralShares', 'pivotValue', 'comments',
               'externalWebsitePostViewConversions', 'costInUsd', 'oneClickLeads', 'landingPageClicks',
               'viralCardImpressions', 'follows', 'oneClickLeadFormOpens', 'viralOneClickLeadFormOpens',
               'conversionValueInLocalCurrency', 'viralFollows', 'impressions', 'otherEngagements',
               'viralLandingPageClicks', 'viralExternalWebsitePostClickConversions', 'externalWebsiteConversions',
               'cardImpressions', 'leadGenerationMailContactInfoShares', 'leadGenerationMailInterestedClicks',
               'opens', 'clicks', 'totalEngagements', 'viralClicks', 'pivotValues_sponsoredAccount', 'pivotValues_share'
]


def extract_data():
    h = httplib2.Http("/tmp/.cache", timeout=50)
    _uri = "https://api.linkedin.com/v2/adAnalyticsV2?q=analytics&dateRange.start.month=1&dateRange.start.day=1&dateRange.start.year=2018&timeGranularity=MONTHLY&pivot=ACCOUNT&campaigns=urn:li:sponsoredCampaign:126706406"
    _uri2 = "https://api.linkedin.com/v2/adAnalyticsV2?q=statistics&dateRange.start.month=1&dateRange.start.day=1&dateRange.start.year=2018&timeGranularity=DAILY&pivots=ACCOUNT&pivots=SHARE&campaigns=urn:li:sponsoredCampaign:126706406"
    resp, content = h.request(_uri, 'GET', headers={
        'Authorization': 'Bearer AQXm6MfmRtaAht_dlXzZ-nBMasAza-79lPP-9lpL08B-glGRDZaAgucdeBpB5Mob9lWjO7V8vzybzdeJDV1-jGa9Cu6c2u18iJdop_t0iNXWFQk9DKpJh3xasyMyLNhtPAMiwI-53puEbvkuviRi807cKrsojEgomSy2em7XJmvf_zQZZoHqJUHnJNKADP3fV3lxR6fWAM2dPhYN0XRXpQoOI6omPIDPtDfSX0nTV9VwNM1OdtpoTpPDdvCs9IzXeO976O8iFEe1N_E_-Hbp1OLcJ-FGt8btfBxVm0zepgMZQ9jRtimQm9AURXkdSs_9JhX1kSiD95yKH_qD0wJbEyzGdM4lOw'})

    resp1, content1 = h.request(_uri2, 'GET', headers={
        'Authorization': 'Bearer AQXm6MfmRtaAht_dlXzZ-nBMasAza-79lPP-9lpL08B-glGRDZaAgucdeBpB5Mob9lWjO7V8vzybzdeJDV1-jGa9Cu6c2u18iJdop_t0iNXWFQk9DKpJh3xasyMyLNhtPAMiwI-53puEbvkuviRi807cKrsojEgomSy2em7XJmvf_zQZZoHqJUHnJNKADP3fV3lxR6fWAM2dPhYN0XRXpQoOI6omPIDPtDfSX0nTV9VwNM1OdtpoTpPDdvCs9IzXeO976O8iFEe1N_E_-Hbp1OLcJ-FGt8btfBxVm0zepgMZQ9jRtimQm9AURXkdSs_9JhX1kSiD95yKH_qD0wJbEyzGdM4lOw'})

    monthly_data = json.loads(content).get("elements")
    daily_data = json.loads(content1).get("elements")


    print(len(monthly_data))
    print(len(daily_data))

    print('asdsad')

    with open(config.DATA_PATH, mode='w', encoding='utf8') as raw_csv:
        writer = helpers.prepare_header_for_clear_csv(raw_csv, CSV_COLUMNS)

        print(f'DATA COLUMNS === {len(daily_data)}')
        r = 0

        for item in daily_data:
            r += 1


            row = {}
            row.update({
                'externalWebsitePostClickConversions': item.get("externalWebsitePostClickConversions"),
                'viralImpressions': item.get('viralImpressions'),
                'DATE': f'{item["dateRange"]["start"]["year"]}-{item["dateRange"]["start"]["month"]}-{item["dateRange"]["start"]["day"]}',
                'adUnitClicks': item.get('adUnitClicks'),
                'companyPageClicks': item.get('companyPageClicks'),
                'viralOneClickLeads': item.get('viralOneClickLeads'),
                'textUrlClicks': item.get('textUrlClicks'),
                'costInLocalCurrency': item.get('costInLocalCurrency'),
                'viralLikes': item.get('viralLikes'),
                'viralOtherEngagements': item.get('viralOtherEngagements'),
                'viralExternalWebsiteConversions': item.get('viralExternalWebsiteConversions'),
                'shares': item.get('shares'),
                'viralCardClicks': item.get('viralCardClicks'),
                'pivot': item.get('pivot'),
                'cardClicks': item.get('cardClicks'),
                'viralExternalWebsitePostViewConversions': item.get('viralExternalWebsitePostViewConversions'),
                'viralTotalEngagements': item.get('viralTotalEngagements'),
                'viralCompanyPageClicks': item.get('viralCompanyPageClicks'),
                'likes': item.get('likes'),
                'viralComments': item.get('viralComments'),
                'actionClicks': item.get('actionClicks'),
                'viralShares': item.get('viralShares'),
                'pivotValue': item.get('pivotValue'),
                'comments': item.get('comments'),
                'externalWebsitePostViewConversions': item.get('externalWebsitePostViewConversions'),
                'costInUsd': item.get('costInUsd'),
                'oneClickLeads': item.get('oneClickLeads'),
                'landingPageClicks': item.get('landingPageClicks'),
                'viralCardImpressions': item.get('viralCardImpressions'),
                'follows': item.get('follows'),
                'oneClickLeadFormOpens': item.get('oneClickLeadFormOpens'),
                'viralOneClickLeadFormOpens': item.get('viralOneClickLeadFormOpens'),
                'conversionValueInLocalCurrency': item.get('conversionValueInLocalCurrency'),
                'viralFollows': item.get('viralFollows'),
                'impressions': item.get('impressions'),
                'otherEngagements': item.get('otherEngagements'),
                'viralLandingPageClicks': item.get('viralLandingPageClicks'),
                'viralExternalWebsitePostClickConversions': item.get('viralExternalWebsitePostClickConversions'),
                'externalWebsiteConversions': item.get('externalWebsiteConversions'),
                'cardImpressions': item.get('cardImpressions'),
                'leadGenerationMailContactInfoShares': item.get('leadGenerationMailContactInfoShares'),
                'leadGenerationMailInterestedClicks': item.get('leadGenerationMailInterestedClicks'),
                'opens': item.get('opens'),
                'clicks': item.get('clicks'),
                'totalEngagements': item.get('totalEngagements'),
                'viralClicks': item.get('viralClicks'),
                'pivotValues_sponsoredAccount': item.get('pivotValues')[0],
                'pivotValues_share': item.get('pivotValues')[0]
            })

            print(f'ROW --- {r}, DATE --- {row["DATE"]}')

            writer.writerow(row)



if __name__ == '__main__':
    extract_data()