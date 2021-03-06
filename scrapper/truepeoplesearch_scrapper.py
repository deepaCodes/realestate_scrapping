import multiprocessing
import traceback
from urllib import parse

import numpy
import pandas as pd
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

from scrapper.utils import scraper_api_call

properties = {'proxy': False, 'count': 0}


def _multiprocessing_person_scrape_fn(row):
    try:

        if properties.get('count', 0) > 250:
            properties['count'] = 0
            properties['proxy'] = False

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

        api_url = '{}?{}'.format(url1, parse.urlencode(params))
        # print('api_url: {}'.format(api_url))

        headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            # 'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'en-US,en;q=0.9,kn;q=0.8',
            'cache-control': 'no-cache',
            # 'cookie': '__cfduid=d43703fa88aa9fe9d694834ed93e372031607172491; aegis_uid=40108693173; aegis_tid=organic%20/%20google; _splyutt1set=set; _ga=GA1.2.1480912600.1607172491; _gid=GA1.2.856449766.1607172491; _splyutt1=Original; cookieconsent_status=dismiss; x-ms-routing-name=self; TiPMix=7.47872819540963; _gat=1; _gali=btnSubmit-m-addr',
            'pragma': 'no-cache',
            'referer': api_url,
            'sec-fetch-dest': 'document',
            'sec-fetch-mode': 'navigate',
            'sec-fetch-site': 'same-origin',
            'sec-fetch-user': '?1',
            'service-worker-navigation-preload': 'true',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.198 Safari/537.36'
        }

        if properties.get('proxy', False):
            response = scraper_api_call(url1, params, headers)
            properties['count'] += 1
        else:
            response = requests.get(url1, params=params, headers=headers)

        # print(response.url)

        response_text = response.text
        captcha_text = 'Human test, sorry for the inconvenience'
        rate_limit = 'You are being rate limited'

        if captcha_text in response_text or rate_limit in response_text:
            properties['proxy'] = True
            properties['count'] = 0
            print('Captcha presented. Cannot get details for : {}, {}'.format(address, city_state))
            # time.sleep(100)
            raise ValueError('Captcha presented.')

        soup = BeautifulSoup(response_text, features='lxml')

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

            if properties.get('proxy', False):
                response = scraper_api_call(url2, None, headers)
            else:
                response = requests.get(url2, headers=headers)

            # print(response.url)

            response_text = response.text

            if captcha_text in response_text or rate_limit in response_text:
                properties['proxy'] = True
                properties['count'] = 0
                print('Captcha presented. Cannot get details for : {}, {}'.format(address, city_state))
                # time.sleep(100)
                raise ValueError('Captcha presented.')

            soup = BeautifulSoup(response_text, features='lxml')
            person_details_div = soup.find('div', {'id': 'personDetails'})

            if not person_details_div:
                return row

            details = {}

            name_and_age = person_details_div.find('div').text.strip()
            name_list = [name for name in name_and_age.split('\n') if name]

            details['PERSON_NAME'] = name_list[0] if name_list else None
            details['AGE'] = name_list[1] if name_list and len(name_list) > 1 else None

            if person_details_div.find('a', {'data-link-to-more': 'address'}):
                details['CURRENT_ADDRESS'] = person_details_div.find('a', {
                    'data-link-to-more': 'address'}).text.strip().replace('\n', ', ')

            if person_details_div.find('a', {'data-link-to-more': 'phone'}):
                details['PHONES'] = '\n'.join([a_el.parent.text.strip() for a_el in
                                               person_details_div.find_all('a', {'data-link-to-more': 'phone'})])

            if person_details_div.find('a', {'data-link-to-more': 'aka'}):
                details['AKA'] = '\n'.join([a_el.text.strip() for a_el
                                            in person_details_div.find_all('a', {'data-link-to-more': 'aka'})])

            if person_details_div.find_all('div', {'class': 'content-value'}):
                details['EMAILS'] = '\n'.join([div_el.text.strip() for div_el in
                                               person_details_div.find_all('div', {'class': 'content-value'})
                                               if '@' in div_el.text])

            if person_details_div.find_all('a', {'data-link-to-more': 'address'}):
                previous_address = [a_el.parent.text.strip()
                                    for a_el in person_details_div.find_all('a', {'data-link-to-more': 'address'})]
                details['PREVIOUS_ADDRESS'] = '\n'.join(
                    [address.replace('\n', ', ') for address in previous_address]
                ).replace(', , ,', '').replace('Map', '')

            print('details: {}'.format(details))

            row.update(details)

        print('Scraping completed for {}'.format(row['APN']))
    except:
        traceback.print_exc()
    return row


def chunk(seq, size):
    return (seq[pos:pos + size] for pos in range(0, len(seq), size))


def one_time_scrape_person_info(open_data_csv_in, out_file):
    """
    scrape person information
    :param open_data_csv_in:
    :param out_file:
    :return:
    """
    print('Loading file {}'.format(open_data_csv_in))

    df = pd.read_csv(open_data_csv_in)
    df = df.replace({numpy.nan: None})
    print(df.count())

    df['HOUSE_NO'] = df['HOUSE_NO'].astype(int)
    df['ZIP'] = df['ZIP'].astype(int)

    df1 = df[df['MAIL_TO_STREET'].str.len() < 2]
    # df2 = df[df['MAIL_TO_STREET'].str.len() > 2]
    for index, df_chunk in enumerate(chunk(df1, 2500)):
        csv_out_file = './../DATA/open_data_with_scrape_data_scrapperapi_{}.csv'.format(index)
        try:
            # time.sleep(60)
            with multiprocessing.Pool(processes=multiprocessing.cpu_count()) as pool:
                data_set = df_chunk.to_dict('records')
                results = list(
                    tqdm(pool.imap(_multiprocessing_person_scrape_fn, data_set),
                         desc='Open Data Person Bulk scraping', total=len(data_set), dynamic_ncols=True, miniters=0))
            df = pd.DataFrame(results)
            df.to_csv(csv_out_file, index=False)
            print('Bulk scrapping completed for chunk index: {}'.format(index))
        except:
            print('error for chunk index: {}'.format(index))
            traceback.print_exc()
            # time.sleep(60)

    """
    
    """
    """
    results = []

    for row in data_set[:10]:
        updated_row = _multiprocessing_person_scrape_fn(row)
        results.append(updated_row)
    
    df = pd.DataFrame(results)
    # df.to_csv(out_file, index=False)
    # df.to_pickle(out_file)
    
    """

    print('Bulk scrapping completed')


def main():
    open_data_csv_in = './../DATA/open_data.csv'
    # out_file = './../DATA/open_data_scrapper_output_scraperapi_full.pkl'
    out_file = './../DATA/open_data_with_scrape_data_scrapperapi.csv'

    one_time_scrape_person_info(open_data_csv_in, out_file)


if __name__ == '__main__':
    main()
