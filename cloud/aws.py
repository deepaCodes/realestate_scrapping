import csv
from datetime import datetime

import boto3

# Get the service resource.
import pandas as pd
from boto3.dynamodb.conditions import Key

session = boto3.Session(profile_name='alex')
dynamodb = session.resource('dynamodb')

PROPERTY_LISTING_TABLE = 'PROPERTY_LISTING'
CONTROL_TABLE = 'CONTROL_TBL'
SCRAPPER_KEY = 'REDFIN_SCRAPPER'

DATA_TYPE_DICT = {
    "BATHS": "float",
    "BEDS": "float",
    "DAYS_ON_MARKET": "int",
    "PRICE": "float",
    "PRICE_PER_SQUARE_FEET": "float",
    "PROFIT": "float",
    "SQUARE_FEET": "int",
    "YEAR_BUILT": "int",
    "ZIP_OR_POSTAL_CODE": "int"
}


def get_table(table_name):
    """
    :param table_name:
    :return:
    """
    table = dynamodb.Table(table_name)
    print(table.table_arn)
    return table


def bulk_insert_property_listing(csv_file):
    """

    :param csv_file:
    :param table_name:
    :return:
    """

    with open(csv_file) as csv_data:
        reader = csv.DictReader(csv_data)
        data_list = [row for row in reader]

    table = get_table(PROPERTY_LISTING_TABLE)

    with table.batch_writer() as batch:
        print('Writing batch')
        for row in data_list:
            if 'LISTING_ID' not in row:
                row['LISTING_ID'] = row['LISTING_URL'].split('/')[-1]

            row['PROPERTY_LAST_UPDATED_DATE'] = int(float(row['PROPERTY_LAST_UPDATED_DATE']))
            batch.put_item(Item=row)
        print('Total rows inserted into into dynamodb: {}'.format(len(data_list)))


def query_property_listing():
    """

    :return:
    """
    results = query_table(PROPERTY_LISTING_TABLE)
    for row in results:
        time_epoch = int(row['PROPERTY_LAST_UPDATED_DATE'])
        row['PROPERTY_LAST_UPDATED_DATE'] = datetime.fromtimestamp(time_epoch / 1000).strftime('%c')
        row['PROPERTY_LAST_UPDATED_DATE_EPOCH'] = time_epoch

    return results


def query_table(table_name):
    """
    :param table_name:
    :return:
    """

    table = get_table(table_name)
    response = table.scan()
    rows_count = response['ScannedCount']
    items = response['Items']
    print('Total count: {}'.format(rows_count))

    return items


def get_scrapper_last_run_datetime():
    """
    :param table_name:
    :return:
    """

    table = get_table(CONTROL_TABLE)
    response = table.query(KeyConditionExpression=Key('KEY').eq(SCRAPPER_KEY))
    items = response['Items']
    last_run_timestamp = max([int(row['SCRAPPER_LAST_RUN_TIMESTAMP']) for row in items])
    for item in items:
        item['SCRAPPER_LAST_RUN_TIMESTAMP'] = int(item['SCRAPPER_LAST_RUN_TIMESTAMP'])

    print('Item: {}'.format(items))
    print('Last Run Timestamp: {}'.format(datetime.fromtimestamp(last_run_timestamp)))

    return items[-1] if items else None


def update_control_table(key):
    """
    :param table_name:
    :return:
    """

    table = get_table(CONTROL_TABLE)
    response = table.put_item(
        Item={
            'KEY': key,
            'SCRAPPER_LAST_RUN_DATE': datetime.now().strftime("%c"),
            'SCRAPPER_LAST_RUN_TIMESTAMP': int(datetime.now().timestamp())
        }
    )
    print(response)
    print('Updated table {}'.format(CONTROL_TABLE))


def get_listing_keys():
    """
    :return:
    """

    table = get_table(PROPERTY_LISTING_TABLE)
    response = table.scan(AttributesToGet=['LISTING_ID'])
    items = response['Items']
    listing_keys = [row['LISTING_ID'] for row in items]
    print('Total listings: {}'.format(len(listing_keys)))

    return listing_keys


def _save_property_listing_to_file():
    results = query_table(PROPERTY_LISTING_TABLE)
    df = pd.DataFrame(results)
    df.to_csv('./../DATA/property_listing.csv', index=False)
    print(df.count())
    print(df.dtypes)


def _load_property_listing_from_file():
    csv_file = './../DATA/property_listing.csv'
    with open(csv_file) as csv_data:
        reader = csv.DictReader(csv_data)
        data_list = [row for row in reader]

        for row in data_list:
            for k, v in DATA_TYPE_DICT.items():
                print(k, v)
                if v == 'int' and row[k]:
                    row[k] = int(float(row[k]))
                elif v == 'float' and row[k]:
                    row[k] = float(row[k])

    print('total count: {}'.format(len(data_list)))


def main():
    csv_file = './../DATA/property_listing.csv'
    # bulk_insert_property_listing(csv_file)
    # query_property_listing()
    # get_listing_keys()
    # _save_property_listing_to_file()
    # _load_property_listing_from_file()
    # get_last_run_datetime(PROPERTY_LISTING_TABLE)
    # update_control_table(SCRAPPER_KEY)
    get_scrapper_last_run_datetime()


if __name__ == '__main__':
    main()
