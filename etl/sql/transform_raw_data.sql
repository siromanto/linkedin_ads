CREATE OR REPLACE TEMPORARY TABLE {traffic_by_day_table} AS
(SELECT
DATE as DATE,
'Linkedin' as PROVIDER,
CAMPAIGN_NAME as CAMPAIGN,
'cpc' as MEDIUM,
'Paid Social' as CHANNELGROUPING,
'linkedin' as  SOURCE,
STATUS as CAMPAIGNSTATUS,
CLICKS as ADCLICKS,
COSTINLOCALCURRENCY as ADCOST,
IMPRESSIONS as IMPRESSIONS,
True as ISDATAGOLDEN
from {db_table_raw} t1
order by DATE);


INSERT INTO {prod_table_traffic_by_day}
(
DATE, PROVIDER, CAMPAIGN, MEDIUM, CHANNELGROUPING, SOURCE, CAMPAIGNSTATUS, ADCLICKS, ADCOST, IMPRESSIONS, ISDATAGOLDEN,

DEVICECATEGORY,
KEYWORD,
FULLURL,
LANDINGPAGEPATH,
ADGROUP,
SUBCHANNEL,
PROFILEID,

ADGROUPSTATUS,
ADSTATUS,
ADCONTENT,
PROVIDERCAMPAIGN,
EMAILCAMPAIGN,

BOUNCES,
TRANSACTIONS,
SESSIONS,
SESSIONDURATION,
USERS,
NEWUSERS,
TRANSACTIONREVENUE,
PAGEVIEWS,


IMPRESSIONSHARE,
SAMPLINGLEVEL,

EMAILCLICKS,
EMAILOPENS,
EMAILUNIQUESENDS,
EMAILSENDS,
EMAILHARDBOUNCES,
EMAILSOFTBOUNCES,
EMAILOTHERBOUNCES,
EMAILUNSUBSCRIBES,
VIDEOVIEWS,

goal1completions,
goal2completions,
goal3completions,
goal4completions,
goal5completions,
goal6completions,
goal7completions,
goal8completions,
goal9completions,
goal10completions,
goal11completions,
goal12completions,
goal13completions,
goal14completions,
goal15completions,
goal16completions,
goal17completions,
goal18completions,
goal19completions,
goal20completions
)

select
DATE, PROVIDER, CAMPAIGN, MEDIUM, CHANNELGROUPING, SOURCE, CAMPAIGNSTATUS, ADCLICKS, ADCOST, IMPRESSIONS, True,

'(not set)','','','','','',0,'','','','','',

0,0,0,0,0,0,0,0,'','',0,0,0,0,0,0,0,0,0,
0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0
FROM   {traffic_by_day_table}
WHERE DATE >= CURRENT_DATE()-{dayload}
order by DATE;
