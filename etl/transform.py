# -*- coding: utf-8 -*-

from configs import config, helpers
# from LinkedinAds.configs import config, helpers


def backfill(client_name, start_date, end_date):
    start_time, end_time = helpers.normalize_backfill_start_end_time(start_date, end_date)
    transform_raw_data(client_name, start_time, end_time)


def transform_daily(**kwargs):
    transform_raw_data()


def transform_raw_data():
    client_config = helpers.get_client_config(config.CLIENT_CONFIG_PATH)
    db_config = helpers.get_client_config(config.DB_CONFIG_PATH)

    # with open('LinkedinAds/etl/sql/transform_raw_data.sql') as f:
    with open(r'/Users/siromanto/ralabs/0.projects/conDati/LinkedinAds/etl/sql/transform_raw_data.sql') as f:
        transform_sql = f.read()
        transform_sql = transform_sql.format(
            traffic_by_day_table=client_config['traffic_by_day'],
            db_table_raw=client_config['raw_db_table'],
            prod_table_traffic_by_day=client_config['prod_table_traffic_by_day'],
            dayload=helpers.DAYLOAD
        )
    helpers.perform_db_routines(transform_sql, client_config, db_config)

    print('DAILY DATA SUCCESSFULLY LOAD...')


if __name__ == '__main__':
    transform_daily()
