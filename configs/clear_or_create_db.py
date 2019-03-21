#!/usr/bin/env python
import snowflake.connector

from configs import config, helpers


def create_connector():
    # Gets the version
    ctx = snowflake.connector.connect(
        user=config.SNOWFLAKE_DB_USERNAME,
        password=config.SNOWFLAKE_DB_PASSWORD,
        account=config.SNOWFLAKE_DB_ACCOUNT
        )
    return ctx


def run(table_name, table_columns):
    ctx = create_connector()
    cs = ctx.cursor()

    try:
        cs.execute("SELECT current_version()")
        one_row = cs.fetchone()
        print(one_row[0])

        cs.execute('USE WAREHOUSE {}'.format(config.SNOWFLAKE_WAREHOUSE))
        cs.execute('USE DATABASE {}'.format(config.SNOWFLAKE_DATABASE))

        cs.execute(
            "CREATE OR REPLACE TABLE "
            "{}({})".format(table_name, table_columns))

        print(f'Database {table_name} successfully created or cleared')
    finally:
        cs.close()
    ctx.close()


if __name__ == '__main__':
    run(table_name='LINKEDIN_CONSOLE_RAW_DATA', table_columns=helpers.RAW_DB_COLUMNS)
    # run(table_name='LINKEDIN_CONSOLE_RAW_DATA', table_columns=helpers.QUERY_STATS_DB_COLUMNS)
    # run(table_name='GAAN_V16_TRAFFICBYDAY_TEST', table_columns=helpers.V16_DB_COLUMNS)
