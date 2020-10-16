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


def main():
    recently_sold_homes()
    # apn_search()


if __name__ == '__main__':
    main()
