import time
import traceback

import pandas as pd

from scrapper.newman import SoldHomeScrapper

scrapper = SoldHomeScrapper()


def recently_sold_homes():
    urls = ['https://www.redfin.com/city/19701/CA/Temecula/recently-sold',
            'https://www.redfin.com/city/12866/CA/Murrieta/recently-sold',
            'https://www.redfin.com/city/19701/CA/Temecula', ]
    scrapper.scrape_redfin(urls)
    # scrapper.fetch_parcel_accessor()

    listing_url = 'https://www.redfin.com/CA/Temecula/33734-Abbey-Rd-92592/home/12512141'
    # scrapper.fetch_property_details(listing_url)


def apn_search():
    apns = ['945080025', '944-100-019', '959030010', '957551003', '927420007', '966341017', '927-570-007']
    for apn in apns:
        scrapper.fetch_assessor_county_recorder(apn)


def open_data_api():
    listing = {'APN': '949350002'}
    # result = SoldHomeScrapper._multiprocessing_arc_gis_api(listing)
    # print(result)

    apns = ['945080025', '944-100-019', '959030010', '957551003', '927420007', '966341017', '927-570-007']
    listings = [{'APN': apn} for apn in apns]
    result = scrapper.fetch_open_data_attributes(listings)
    print(result)

    # listings = query_property_listing()
    # result = scrapper.fetch_open_data_attributes(listings)
    print(result)

    df = pd.DataFrame(result)
    df.to_csv('With_OpenData.csv', index=False)


def chunk(seq, size):
    return (seq[pos:pos + size] for pos in range(0, len(seq), size))


def one_time_open_data():
    csv_in_file = './../DATA/Parcel_Assessor.csv'

    df = pd.read_csv(csv_in_file, engine='python')
    print(df.count())
    for index, df_chunk in enumerate(chunk(df, 5000)):
        csv_out_file = 'OpenData_updates_{}.csv'.format(index)
        try:
            _scrapper = SoldHomeScrapper()
            _scrapper.one_time_open_data_fetch(df_chunk, csv_out_file)
            time.sleep(60)
        except:
            traceback.print_exc()
            time.sleep(60)


def one_time_scrape_person_info():
    open_data_csv_in = './../DATA/open_data.csv'
    csv_out_file = './../DATA/open_data_with_scrape_data.csv'

    _scrapper = SoldHomeScrapper()
    _scrapper.one_time_scrape_person_info(open_data_csv_in, csv_out_file)


def main():
    # recently_sold_homes()
    # open_data_api()
    # apn_search()
    # one_time_open_data()
    one_time_scrape_person_info()


if __name__ == '__main__':
    main()
