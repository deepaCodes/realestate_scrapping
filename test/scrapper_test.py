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


def one_time_open_data():
    csv_in_file = './../DATA/Parcel_Assessor.csv'
    csv_out_file = 'OpenData_updates.csv'
    scrapper.one_time_open_data_fetch(csv_in_file, csv_out_file)


def main():
    # recently_sold_homes()
    # open_data_api()
    # apn_search()
    one_time_open_data()


if __name__ == '__main__':
    main()
