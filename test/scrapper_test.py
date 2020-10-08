from scrapper.newman import SoldHomeScrapper


def recently_sold_homes():
    scrapper = SoldHomeScrapper()

    urls = ['https://www.redfin.com/city/19701/CA/Temecula/recently-sold',
            'https://www.redfin.com/city/12866/CA/Murrieta/recently-sold',
            'https://www.redfin.com/city/19701/CA/Temecula', ]
    scrapper.scrape_redfin(urls)
    # scrapper.fetch_parcel_accessor()

    listing_url = 'http://www.redfin.com/CA/Temecula/0-VISTA-DEL-MAR-92591/home/171396037'
    # scrapper.fetch_property_details(listing_url)


def main():
    recently_sold_homes()


if __name__ == '__main__':
    main()
