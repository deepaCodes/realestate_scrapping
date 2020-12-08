import glob
import multiprocessing
import re
import time
from urllib.parse import urlsplit, parse_qs, quote

import pandas as pd
import requests
from tqdm import tqdm

from scrapper.utils import get_mortgage_rate

url = "https://gis.countyofriverside.us/arcgis_public/rest/services/OpenData/ParcelBasic/MapServer/4/query?f=json&where=CITY='Temecula' AND ZIP=92592 AND HOUSE_NO = 35790&outFields=APN,HOUSE_NO,STREET,SUFFIX,CITY,ZIP,ACRE,CAME_FROM,LAND,STRUCTURE,OWNER1_LAST_NAME,OWNER1_FIRST_NAME,OWNER1_MID_NAME,OWNER2_LAST_NAME,OWNER2_FIRST_NAME,OWNER2_MID_NAME,OWNER3_LAST_NAME,OWNER3_FIRST_NAME,OWNER3_MID_NAME&returnGeometry=false&returnDistinctValues=true"


def decode_params():
    query = urlsplit(url).query
    params = parse_qs(query)
    print(params)

    where = "CITY = 'TEMECULA' AND HOUSE_NO >= 35790 AND HOUSE_NO <= 35790 AND ZIP >= 92592 AND ZIP <= 92592"
    where = "CITY='Temecula' AND ZIP=92592 AND HOUSE_NO = 35790"
    print(quote(where))


def extract_number_from_string():
    address = '26444 Arboretum Way 2108'
    print(address)
    result = int(re.search(r'\d+', address)[0])
    print(result)


def match_address():
    url = 'https://cdn.shopify.com/s/files/1/1128/6962/products/98-0123-B_large.jpg?v=1555380247'
    print(url.split('/')[-1].split('_large')[0])


def extract_listing_id():
    url = 'http://www.redfin.com/CA/Temecula/30037-Manzanita-Ct-92591/home/6295163'
    print(url)
    result = url.split('/')
    print(int(result[-1]))


def _mp_test(row):
    print(row)
    time.sleep(1)
    return row


def test_mp():
    with multiprocessing.Pool(processes=4) as pool:
        data_set = [i for i in range(1, 100)]
        results = list(
            tqdm(pool.map(_mp_test, data_set),
                 desc='Open Data API Bulk fetch', total=len(data_set)))


def scrape_mortgage():
    # result = scrape_mortgage_rates()
    result = get_mortgage_rate(2018, 'March')
    print(result)
    print('done')


def merge_csv():
    print('merging csv files')
    df_list = []
    for file in glob.glob('./../DATA/open_data/*.csv'):
        print(file)
        df = pd.read_csv(file)
        df_list.append(df)

    df = pd.concat(df_list)
    print(df.count())

    df.to_csv('./../DATA/open_data.csv', index=False)


def test_scrapping():


    url = "https://www.truepeoplesearch.com/resultaddress?streetaddress=31922 CALLE CABALLOS&citystatezip=TEMECULA, CA 92592"

    payload={}
    headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        # 'accept-encoding': 'gzip, deflate, br',
        'accept-language': 'en-US,en;q=0.9,kn;q=0.8',
        'cache-control': 'no-cache',
        'cookie': '__cfduid=d4e96fea242a64dcfbd33fb9870fac31f1607172869; __cfduid=d4e96fea242a64dcfbd33fb9870fac31f1607172869; TiPMix=6.83616083433673; x-ms-routing-name=self',
        'pragma': 'no-cache',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'same-origin',
        'sec-fetch-user': '?1',
        'service-worker-navigation-preload': 'true',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.198 Safari/537.36'
    }

    response = requests.request("GET", url, headers=headers, data=payload)

    print(response.text)


def test_pickle():
    df = pd.read_pickle("./../DATA/open_data_scrapper_output_scraperapi.pkl")
    print(df.count())
    print(df.to_string())

def main():
    # extract_number_from_string()
    # match_address()
    # extract_listing_id()
    # scrape_mortgage()
    # merge_csv()
    # test_scrapping()
    test_pickle()


if __name__ == '__main__':
    main()
