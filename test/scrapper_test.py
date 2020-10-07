from scrapper.newman import SoldHomeScrapper


def recently_sold_homes():
    scrapper = SoldHomeScrapper()

    urls = ['https://www.redfin.com/city/19701/CA/Temecula/recently-sold',
            'https://www.redfin.com/city/12866/CA/Murrieta/recently-sold',
            'https://www.redfin.com/city/19701/CA/Temecula', ]
    scrapper.scrape_redfin(urls)


def main():
    recently_sold_homes()


if __name__ == '__main__':
    main()
