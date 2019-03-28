# -*- coding: utf-8 -*-

import argparse
import os

from configs import helpers


def backfill(client_name, start_date, end_date):
    start_time, end_time = helpers.normalize_backfill_start_end_time(start_date, end_date)
    transform_raw_data(client_name, start_time, end_time)


def transform_daily(**kwargs):
    # start_time, end_time = helpers.get_start_end_time_from_kwargs(helpers.REPORT_TYPE_DAILY, kwargs)
    transform_raw_data()


def transform_weekly():
    transform_raw_data()


def transform_raw_data(client_name=None):
    # client_config = helpers.get_client_config(r'/opt/workbench/users/afuser/airflow/dags/credentials/AmazonAdsKeys/Toweltech.json')
    client_config = helpers.get_client_config(r'/Users/siromanto/ralabs/0.projects/conDati/LinkedinAds/configs/Linkedin.json')

    # with open('AmazonAds/etl/sql/transform_raw_data.sql') as f:
    with open(r'/Users/siromanto/ralabs/0.projects/conDati/LinkedinAds/etl/sql/transform_raw_data.sql') as f:
        transform_sql = f.read()
        transform_sql = transform_sql.format(
            traffic_by_day_table=client_config['traffic_by_day'],
            db_table_raw=client_config['raw_db_table'],
            prod_table_traffic_by_day=client_config['prod_table_traffic_by_day'],
        )
    helpers.perform_db_routines(client_name, transform_sql)


if __name__ == '__main__':
    # parser = argparse.ArgumentParser(
    #     description=__doc__,
    #     formatter_class=argparse.RawDescriptionHelpFormatter)
    # parser.add_argument('--client_name')
    # parser.add_argument('--start_date')
    # parser.add_argument('--end_date')
    # args = parser.parse_args()
    #backfill(args.client_name, datetime.strptime(args.start_date, '%Y-%m-%d'),
    #         datetime.strptime(args.end_date, '%Y-%m-%d'))
    # transform_daily('')
    transform_weekly()
