import itertools
import json
import multiprocessing
import pathlib
import traceback
from datetime import datetime
from io import StringIO

import numpy
import pandas as pd
import requests
from bs4 import BeautifulSoup

from scrapper.constants import RED_FIN_BASE_URL, REDFIN_HEADERS, OPEN_DATA_ARC_GIS_API, OPEN_DATA_ARC_GIS_HEADERS, \
    ARC_GIS_PARAM, ARC_GIS_WHERE_CLAUSE, PROPERTY_DETAILS, DOC_SEARCH_POST_API, \
    DOC_SEARCH_POST_API_PAYLOAD, DOC_SEARCH_POST_HEADERS, DOC_SEARCH_GET_HEADERS, DOC_SEARCH_GET_API, TRUST_KEYS
from scrapper.utils import extract_house_no, calculate_profit

CURRENT_DIR = pathlib.Path(__file__).parent.absolute()
PROJECT_ROOT = pathlib.Path(__file__).parent.parent.absolute()


class SoldHomeScrapper:
    def __init__(self):
        print('Init method')
        self.redfin_headers = REDFIN_HEADERS

    def _scrape_download_url(self, urls):
        """
        scrapping from url
        :param url:
        :return:
        """

        download_links = []

        for url in urls:
            print('scrapping from: {}'.format(url))
            try:
                response = requests.get(url, headers=self.redfin_headers)
                if not response.ok:
                    print(response.text)
                    continue

                html_text = response.text
                soup = BeautifulSoup(html_text, features='lxml')

                a_elm = soup.find('a', {'id': 'download-and-save'})
                if a_elm and hasattr(a_elm, 'attrs'):
                    _link = a_elm.attrs['href']
                    download_links.append('{}{}'.format(RED_FIN_BASE_URL, _link))
            except:
                traceback.print_exc()

        print('download_links: {}'.format(download_links))
        return download_links

    def _download_file(self, download_links):
        """
        Download file
        :param download_links:
        :return:
        """

        df_list = []
        for download_link in download_links:
            print('Downloading from: {}'.format(download_link))
            try:
                response = requests.get(download_link, headers=self.redfin_headers)
                if not response.ok:
                    print(response.text)
                    continue

                data = StringIO(response.text)
                df = pd.read_csv(data)

                df_list.append(df)
            except:
                traceback.print_exc()

        print('All files downloaded')
        df = pd.concat(df_list)

        # rename columns
        rename_columns = {
            'URL (SEE http://www.redfin.com/buy-a-home/comparative-market-analysis FOR INFO ON PRICING)': 'LISTING_URL',
            'MLS#': 'MLS_NUMBER',
            '$/SQUARE FEET': 'PRICE_PER_SQUARE_FEET',
            'HOA/MONTH': 'HOA_MONTH',
        }
        df.rename(columns=rename_columns, inplace=True)
        columns = {col: '_'.join(col.split()) for col in list(df.columns)}
        # print(columns)
        df.rename(columns=columns, inplace=True)
        df.replace({numpy.NaN: None}, inplace=True)

        return df

    @staticmethod
    def fetch_property_details(listing_url):
        """
        Scrape price history and APN from listing url
        :param listing_url:
        :return:
        """
        print('scrapping price history from: {}'.format(listing_url))
        result = {}
        try:
            headers = REDFIN_HEADERS.copy()
            headers['referer'] = listing_url
            headers['Content-Type'] = 'application/json'
            params = {
                'propertyId': listing_url.split('/')[-1],
                'accessLevel': 1,
                'pageType': 2,
            }
            response = requests.get(PROPERTY_DETAILS, params=params, headers=headers)
            if not response.ok:
                print(response.text)
                return None

            json_response = json.loads(response.text.replace('{}&&', ''))
            if 'payload' not in json_response:
                return None

            payload = json_response['payload']
            public_record_info = payload['publicRecordsInfo']
            property_history = payload['propertyHistoryInfo']

            basic_info = public_record_info['basicInfo']

            # APN
            result['APN'] = basic_info['apn'] if 'apn' in basic_info else None
            result['PROPERTY_LAST_UPDATED_DATE'] = basic_info['propertyLastUpdatedDate']

            sell_events = [row for row in property_history['events'] if 'price' in row and 'sourceId' in row]
            sold_info = {row['eventDate']: row['price'] for row in property_history['events']
                         if 'price' in row and 'sourceId' in row and 'Sold' in row['eventDescription']
                         and sell_events[0]['eventDate'] != row['eventDate']}
            if sold_info:
                print('calculate profit from sell history')
                values = list(sold_info.values())
                sell_price = sell_events[0]['price']
                sell_price_previous = values[0]
                result['PROFIT'] = calculate_profit(sell_price, sell_price_previous)
            elif public_record_info['taxInfo']:
                print('calculate profit from tax info')
                tax_info = public_record_info['taxInfo']
                assessed_price = (tax_info['taxableLandValue'] if 'taxableLandValue' in tax_info else 0) + \
                                 (tax_info['taxableImprovementValue'] if 'taxableImprovementValue' in tax_info else 0)
                sold_price = sell_events[0]['price'] if sell_events else 0
                result['PROFIT'] = calculate_profit(sold_price, assessed_price)
            else:
                pass
            print('end of scrapping')
        except:
            traceback.print_exc()
            for key in ['APN', 'PROPERTY_LAST_UPDATED_DATE', 'PROFIT']:
                if key not in result:
                    result[key] = None

        print(result)
        return result

    @staticmethod
    def fetch_assessor_county_recorder(apn):
        """
        get Grantor and Grantee
        :param apn:
        :return:
        """
        print('Doc search for APN: {}'.format(apn))
        result = {}

        try:
            if not apn:
                return result

            apn = apn.replace('-', '')

            response = requests.post(DOC_SEARCH_POST_API, headers=DOC_SEARCH_POST_HEADERS,
                                     data=DOC_SEARCH_POST_API_PAYLOAD.format(apn))

            if not response.ok:
                print(response.text)
                return result

            cookies = [cookie.value for cookie in response.cookies if cookie.name == 'JSESSIONID']
            _headers = DOC_SEARCH_GET_HEADERS.copy()
            _headers['Cookie'] = 'JSESSIONID={}'.format(cookies[0])

            post_result = response.json()
            # print(post_result)

            response = requests.get(DOC_SEARCH_GET_API.format(datetime.now().timestamp()), headers=_headers)
            if not response.ok:
                print(response.text)
                return result

            soup = BeautifulSoup(response.text, 'lxml')
            search_rows = soup.find('li', {'class': 'ss-search-row'})

            search_result = search_rows.find_all('div', {'class': 'searchResultFourColumn'})
            if search_result and len(search_result) == 4:
                grantors = [row.text for row in search_result[1].find_all('li')[1:]]
                # print(grantors)
                result['GRANTOR'] = '; '.join(grantors)

                grantees = [row.text for row in search_result[2].find_all('li')[1:]]
                # print(grantees)
                result['GRANTEE'] = '; '.join(grantees)

                result['TRUST'] = 'Y' if [trust_txt for trust_txt in TRUST_KEYS if
                                          trust_txt in result['GRANTOR'].lower()] else 'N'

            print('APN {}, result: {}'.format(apn, result))

        except:
            traceback.print_exc()

        return result

    def scrape_redfin(self, urls):
        """
        Recently sold home - scrape data
        :param urls:
        :return:
        """

        start_time = datetime.now()

        print('Scrapping recently sold homes from :{}'.format(urls))

        download_links = self._scrape_download_url(urls)
        df = self._download_file(download_links)
        print(df.count())
        print(df.head(25).to_string())

        df.to_csv('{}/DATA/Download.csv'.format(PROJECT_ROOT), index=False)

        df[['APN', 'PROPERTY_LAST_UPDATED_DATE', 'PROFIT']] = df.apply(
            lambda row: self.fetch_property_details(row['LISTING_URL']), axis=1, result_type='expand')

        df.replace({numpy.NaN: None}, inplace=True)

        df[['GRANTOR', 'GRANTEE', 'TRUST']] = df.apply(lambda row: self.fetch_assessor_county_recorder(row['APN']),
                                                       axis=1, result_type='expand')

        df.replace({numpy.NaN: None}, inplace=True)

        print(df.to_string())
        df.to_csv('{}/DATA/Aggregated_data.csv'.format(PROJECT_ROOT), index=False)

        print('end of scrapping')

        end_time = datetime.now()

        print('Start time: {}, end time: {}'.format(start_time, end_time))

    @staticmethod
    def _multiprocessing_arc_gis_api(listing):
        """
        fetch opendata information - Run data against API Homeowner and Parcel Data:
        :param listing:
        :return:
        """

        house_no = extract_house_no(listing['ADDRESS'])
        if not house_no:
            return None

        try:
            params = ARC_GIS_PARAM
            params['where'] = ARC_GIS_WHERE_CLAUSE.format(listing['CITY'], int(listing['ZIP_OR_POSTAL_CODE']), house_no)
            response = requests.get(OPEN_DATA_ARC_GIS_API, params=params, headers=OPEN_DATA_ARC_GIS_HEADERS)
            if not response.ok:
                print(response.text)
                return None

            arc_gis_result = response.json()
            if 'features' in arc_gis_result and arc_gis_result['features']:
                result = [row['attributes'] for row in arc_gis_result['features'] if 'attributes' in row]
                print(result)

            return result
        except:
            traceback.print_exc()

        return None

    def fetch_parcel_accessor(self, redfin_df):

        workers = multiprocessing.cpu_count()
        print('CPU count: {}'.format(workers))

        with multiprocessing.Pool(processes=1) as pool:
            listings = redfin_df.to_dict('records')
            result = pool.map(SoldHomeScrapper._multiprocessing_arc_gis_api, listings)

            # expand list of list into list
            result = list(itertools.chain.from_iterable(result))
            print(result)
