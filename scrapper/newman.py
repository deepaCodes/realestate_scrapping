import json
import json
import multiprocessing
import pathlib
import time
import traceback
from datetime import datetime
from io import StringIO

import numpy
import pandas as pd
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

from cloud.aws import get_listing_keys, bulk_insert_property_listing, update_control_table, SCRAPPER_KEY
from scrapper.constants import RED_FIN_BASE_URL, REDFIN_HEADERS, OPEN_DATA_ARC_GIS_API, OPEN_DATA_ARC_GIS_HEADERS, \
    ARC_GIS_PARAM, ARC_GIS_WHERE_CLAUSE, PROPERTY_DETAILS, DOC_SEARCH_POST_API, \
    DOC_SEARCH_POST_API_PAYLOAD, DOC_SEARCH_POST_HEADERS, DOC_SEARCH_GET_HEADERS, DOC_SEARCH_GET_API, TRUST_KEYS
from scrapper.utils import calculate_profit, get_mortgage_rate

CURRENT_DIR = pathlib.Path(__file__).parent.absolute()
PROJECT_ROOT = pathlib.Path(__file__).parent.parent.absolute()

workers = multiprocessing.cpu_count()
print('CPU count: {}'.format(workers))


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

        df['LISTING_ID'] = df['LISTING_URL'].str.split('/').str[-1]

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

            apn = str(apn).replace('-', '')

            response = requests.post(DOC_SEARCH_POST_API, headers=DOC_SEARCH_POST_HEADERS,
                                     data=DOC_SEARCH_POST_API_PAYLOAD.format(apn))

            if not response.ok:
                print(response.text)
                return result

            cookies = [cookie.value for cookie in response.cookies if cookie.name == 'JSESSIONID']
            _headers = DOC_SEARCH_GET_HEADERS.copy()
            _headers['Cookie'] = 'JSESSIONID={}'.format(cookies[0])

            # post_result = response.json()
            # print(post_result)

            response = requests.get(DOC_SEARCH_GET_API.format(datetime.now().timestamp()), headers=_headers)
            if not response.ok:
                print(response.text)
                return result

            soup = BeautifulSoup(response.text, 'lxml')
            search_rows = soup.find('li', {'class': 'ss-search-row'})
            if not search_rows:
                return result

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

                recording_dates = [row.text.split()[0] for row in search_result[0].find_all('li')[1:]]
                if recording_dates:
                    recording_date = datetime.strptime(recording_dates[0], '%m/%d/%Y')
                    year = recording_date.strftime("%Y")
                    month = recording_date.strftime("%B")
                    mortgage_rate = get_mortgage_rate(year, month)
                    result.update(
                        {'RECORDING_DATE': recording_dates[0], 'RECORDING_YEAR': year, 'RECORDING_MONTH': month,
                         'MORTGAGE_RATE': mortgage_rate})

            # print('APN {}, result: {}'.format(apn, result))

        except:
            print('Error for APN: {}'.format(apn))
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

        listing_keys = get_listing_keys()
        df = df[~df['LISTING_ID'].isin(listing_keys)]
        print(df.count())
        print(df.head(5).to_string())

        date_str = datetime.now().strftime('%Y_%m_%d_%H_%M_%S')
        df.to_csv('{}/DATA/Download_{}.csv'.format(PROJECT_ROOT, date_str), index=False)

        df[['APN', 'PROPERTY_LAST_UPDATED_DATE', 'PROFIT']] = df.apply(
            lambda row: self.fetch_property_details(row['LISTING_URL']), axis=1, result_type='expand')

        df.replace({numpy.NaN: None}, inplace=True)

        return_columns = ['GRANTOR', 'GRANTEE', 'TRUST', 'RECORDING_DATE', 'RECORDING_YEAR', 'RECORDING_MONTH',
                          'MORTGAGE_RATE']
        df[return_columns] = df.apply(lambda row: self.fetch_assessor_county_recorder(row['APN']), axis=1,
                                      result_type='expand')

        df.replace({numpy.NaN: None}, inplace=True)

        print(df.head().to_string())

        aggregated_csv_file = '{}/DATA/Aggregated_data_{}.csv'.format(PROJECT_ROOT, date_str)
        df.to_csv(aggregated_csv_file, index=False)

        bulk_insert_property_listing(aggregated_csv_file)

        # update control table
        update_control_table(SCRAPPER_KEY)

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

        apn = listing['APN']
        if not apn:
            return listing

        print('Querying open data for apn: {}'.format(apn))
        apn = apn.replace('-', '')
        try:
            time.sleep(1)
            params = ARC_GIS_PARAM
            params['where'] = ARC_GIS_WHERE_CLAUSE.format(apn)
            response = requests.get(OPEN_DATA_ARC_GIS_API, params=params, headers=OPEN_DATA_ARC_GIS_HEADERS)
            if not response.ok:
                print(response.text)
                return listing

            arc_gis_result = response.json()
            if 'features' in arc_gis_result and arc_gis_result['features']:
                result = [row['attributes'] for row in arc_gis_result['features'] if 'attributes' in row]
                for row in result:
                    for key in row.keys():
                        if not (row[key] and type(row[key]) == str and str(row[key]).strip()):
                            row[key] = None
                    for key in ['HOUSE_NO', 'STREET', 'SUFFIX', 'CITY', 'ZIP']:
                        row['OPENDATA_{}'.format(key)] = row[key]
                        del row[key]

                del listing['APN']
                listing.update(result[-1])
                return listing
        except:
            traceback.print_exc()

        return listing

    def fetch_open_data_attributes(self, listings):

        with multiprocessing.Pool(processes=10) as pool:
            # listings = redfin_df.to_dict('records')
            result = list(tqdm(pool.map(SoldHomeScrapper._multiprocessing_arc_gis_api, listings), desc='Open Data API',
                               total=len(listings), dynamic_ncols=True, miniters=0))

            # expand list of list into list
            result = list(filter(None, result))
            # result = list(itertools.chain.from_iterable(result))
            return result

    @staticmethod
    def _multiprocessing_apn_search_fn(row):
        try:
            result = SoldHomeScrapper.fetch_assessor_county_recorder(row['APN'])
            row.update(result)
        except:
            traceback.print_exc()
        return row

    @staticmethod
    def one_time_open_data_fetch(df, csv_out_file):
        """

        :param csv_file:
        :return:
        """

        # print(df.head().to_string())
        print(df.count())

        data_set = df.to_dict('records')
        print(len(data_set))

        with multiprocessing.Pool(processes=workers) as pool:
            results = list(
                tqdm(pool.map(SoldHomeScrapper._multiprocessing_apn_search_fn, data_set),
                     desc='Open Data API Bulk fetch', total=len(data_set), dynamic_ncols=True, miniters=0))

            df = pd.DataFrame(results)
            df.to_csv(csv_out_file, index=False)
        print('Bulk fetch completed')

    @staticmethod
    def _multiprocessing_person_scrape_fn(row):
        try:
            if row['MAIL_TO_STREET'] and str(row['MAIL_TO_STREET']).strip():
                address = '{} {} {}'.format(row['HOUSE_NO'], row['STREET'], row['SUFFIX']).strip()
                city_state = '{}, CA {}'.format(row['CITY'], row['ZIP'])
                print('{} {}'.format(address, city_state))

                owners = [{'first_name': row['OWNER{}_FIRST_NAME'.format(i)],
                           'last_name': row['OWNER{}_LAST_NAME'.format(i)]} for i in range(1, 4)]

                owners = [{'first_name': row['first_name'].strip() if row['first_name'] and row[
                    'first_name'].strip() else None,
                           'last_name': row['last_name'].strip() if row['last_name'] and row[
                               'last_name'].strip() else None
                           } for row in owners]
                print(owners)

                url1 = 'https://www.truepeoplesearch.com/resultaddress'
                params = {'streetaddress': address, 'citystatezip': city_state}

                headers = {
                    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
                    # 'accept-encoding': 'gzip, deflate, br',
                    'accept-language': 'en-US,en;q=0.9,kn;q=0.8',
                    'cache-control': 'no-cache',
                    # 'cookie': '__cfduid=d43703fa88aa9fe9d694834ed93e372031607172491; aegis_uid=40108693173; aegis_tid=organic%20/%20google; _splyutt1set=set; _ga=GA1.2.1480912600.1607172491; _gid=GA1.2.856449766.1607172491; _splyutt1=Original; cookieconsent_status=dismiss; x-ms-routing-name=self; TiPMix=7.47872819540963; _gat=1; _gali=btnSubmit-m-addr',
                    'pragma': 'no-cache',
                    # 'referer': 'https://www.truepeoplesearch.com/resultaddress?streetaddress=31922%20CALLE%20CABALLOS&citystatezip=TEMECULA,%20CA%2092592',
                    'sec-fetch-dest': 'document',
                    'sec-fetch-mode': 'navigate',
                    'sec-fetch-site': 'same-origin',
                    'sec-fetch-user': '?1',
                    'service-worker-navigation-preload': 'true',
                    'upgrade-insecure-requests': '1',
                    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.198 Safari/537.36'
                }

                response = requests.get(url1, params=params, headers=headers)
                print(response.url)

                captcha_text = 'Human test, sorry for the inconvenience'
                rate_limit = 'You are being rate limited'

                if captcha_text in response.text or rate_limit in response.text:
                    print('Captcha presented. Cannot get details for : {}, {}'.format(address, city_state))
                    row.update({'CAPTCHA': True})
                    time.sleep(100)
                    raise ValueError('Captcha presented.')

                soup = BeautifulSoup(response.text, features='lxml')

                result = [{'name': div_el.find('div', {'class': 'h4'}).text.strip().upper(),
                           'details_url': div_el.find('a').attrs['href']
                           } for div_el in soup.find_all('div', {'class': 'card-summary'})]

                url_found = None
                for _row in result:
                    for owner in owners:
                        if owner['first_name'] and owner['first_name'] in _row['name'] and \
                                owner['last_name'] and owner['last_name'] in _row['name']:
                            print('match found: {}'.format(_row['name']))
                            url_found = _row['details_url']
                            break
                    if url_found:
                        break

                if url_found:
                    print('url_found: {}'.format(url_found))

                    url2 = '{}{}'.format('https://www.truepeoplesearch.com', url_found)

                    response = requests.get(url2, headers=headers)
                    print(response.url)

                    if captcha_text in response.text or rate_limit in response.text:
                        row.update({'CAPTCHA': True})
                        print('Captcha presented. Cannot get details for : {}, {}'.format(address, city_state))
                        time.sleep(100)
                        raise ValueError('Captcha presented.')

                    soup = BeautifulSoup(response.text, features='lxml')
                    person_details_div = soup.find('div', {'id': 'personDetails'})

                    details = {'PERSON_NAME_AGE': person_details_div.find('div').text.strip()}

                    if person_details_div.find('a', {'data-link-to-more': 'address'}):
                        details['CURRENT_ADDRESS'] = person_details_div.find('a', {
                            'data-link-to-more': 'address'}).text.strip()

                    if person_details_div.find('a', {'data-link-to-more': 'phone'}):
                        details['PHONES'] = [a_el.text.strip() for a_el in
                                             person_details_div.find_all('a', {'data-link-to-more': 'phone'})]

                    if person_details_div.find('a', {'data-link-to-more': 'aka'}):
                        details['AKA'] = [a_el.text.strip()
                                          for a_el in person_details_div.find_all('a', {'data-link-to-more': 'aka'})]

                    if person_details_div.find_all('div', {'class': 'content-value'}):
                        details['EMAILS'] = [div_el.text.strip() for div_el in
                                             person_details_div.find_all('div', {'class': 'content-value'})
                                             if '@' in div_el.text]
                    if person_details_div.find_all('a', {'data-link-to-more': 'address'}):
                        details['PREVIOUS_ADDRESS'] = [a_el.text.strip()
                                                       for a_el in person_details_div.find_all('a', {
                                'data-link-to-more': 'address'})]

                    print('details: {}'.format(details))

                    row.update(details)

            row.update({'CAPTCHA': False})
            print('Scraping completed for {}'.format(row['APN']))
        except:
            traceback.print_exc()
        return row

    @staticmethod
    def one_time_scrape_person_info(open_data_csv_in, csv_out_file):
        """
        scrape person information
        :param open_data_csv_in:
        :param csv_out_file:
        :return:
        """
        print('Loading file {}'.format(open_data_csv_in))

        df = pd.read_csv(open_data_csv_in)
        df = df.replace({numpy.nan: None})
        print(df.count())

        df['HOUSE_NO'] = df['HOUSE_NO'].astype(int)
        df['ZIP'] = df['ZIP'].astype(int)

        data_set = df.to_dict('records')

        """
        with multiprocessing.Pool(processes=1) as pool:
            data_set = data_set[1:50]
            results = list(
                tqdm(pool.map(SoldHomeScrapper._multiprocessing_person_scrape_fn, data_set),
                     desc='Open Data Person Bulk scraping', total=len(data_set), dynamic_ncols=True, miniters=0))
        """

        results = []

        for row in data_set:
            updated_row = SoldHomeScrapper._multiprocessing_person_scrape_fn(row)
            results.append(updated_row)

        df = pd.DataFrame(results)
        # df.to_csv(csv_out_file, index=False)
        df.to_pickle('./../DATA/open_data_scrapper_output.pkl')
        print('Bulk scrapping completed')


def main():
    scrapper = SoldHomeScrapper()
    urls = ['https://www.redfin.com/city/19701/CA/Temecula/recently-sold',
            'https://www.redfin.com/city/12866/CA/Murrieta/recently-sold',
            'https://www.redfin.com/city/19701/CA/Temecula', ]
    scrapper.scrape_redfin(urls)


if __name__ == '__main__':
    main()
