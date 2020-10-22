import boto3
import csv
from datetime import datetime
# Get the service resource.
import pandas
import pandas as pd

dynamodb = boto3.resource('dynamodb')

PROPERTY_LISTING_TABLE = 'PROPERTY_LISTING'


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
        row['PROPERTY_LAST_UPDATED_DATE'] = datetime.fromtimestamp(time_epoch/1000).strftime('%c')

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


def main():
    csv_file = './../DATA/Aggregated_data.csv'
    # bulk_insert_property_listing(csv_file)
    query_property_listing()


if __name__ == '__main__':
    main()
