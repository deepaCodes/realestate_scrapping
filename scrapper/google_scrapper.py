import multiprocessing
import time
import traceback
from datetime import datetime
from glob import glob

import numpy
import pandas as pd
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

from scrapper.constants import REDFIN_HEADERS
from scrapper.utils import scraper_api_call, chunk

reffin_headers = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'accept-encoding': 'gzip, deflate, br',
    'accept-language': 'en-US,en;q=0.9',
    'cache-control': 'max-age=0',
    'referer': 'https://www.google.com/',
    'sec-fetch-dest': 'document',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'same-origin',
    'sec-fetch-user': '?1',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36'
}

google_headers = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'accept-language': 'en-US,en;q=0.9,kn;q=0.8',
    'referer': 'https://www.google.com/',
    'cache-control': 'no-cache',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36',
}

GOOGLE_SEARCH_URL = 'https://www.google.com/search?hl=en'

USE_PROXY = False


class GoogleScrapper:
    def __init__(self):
        print('Google scrapper init')

    @staticmethod
    def _get_google_query(row):
        """

        :param row:
        :return:
        """
        address = '{} {} {}'.format(row['HOUSE_NO'], row['STREET'], row['SUFFIX']).strip()
        city_zip_state = '{}, CA {}'.format(row['CITY'], row['ZIP']).strip()
        full_address = '{}, {}'.format(address, city_zip_state)
        google_search_text = '{} site:redfin.com'.format(full_address)
        # print(google_search_text)

        return google_search_text

    @staticmethod
    def scrape_redfin_url(data_attributes):
        # print('Scrapping url: {}'.format(redfin_url))
        # data_attributes = {}

        time.sleep(1)
        redfin_url = data_attributes['GOOGLE_REDFIN_LISTING_URL']
        if not redfin_url:
            return data_attributes

        try:
            headers = REDFIN_HEADERS.copy()
            headers['referer'] = redfin_url
            headers['Content-Type'] = 'application/json'
            params = {
                'propertyId': redfin_url.split('/')[-1],
                'accessLevel': 1,
                'pageType': 2,
            }
            if USE_PROXY:
                response = scraper_api_call(redfin_url, params=params, headers=headers)
            else:
                response = requests.get(redfin_url, params=params, headers=headers)

            if not response.ok:
                print(response.text)
                data_attributes['CAPTCHA_PRESENTED'] = True
                return data_attributes

            soup = BeautifulSoup(response.text, features='lxml')
            price_div = soup.find('div', {'data-rf-test-id': 'avm-price'})
            if price_div:
                data_attributes['REDFIN_ESTIMATE'] = price_div.find('div').text

            listed_price_div = soup.find('div', {'data-rf-test-id': 'abp-price'})
            if listed_price_div:
                data_attributes['REDFIN_LISTED_OR_LAST_SOLD_PRICE'] = listed_price_div.find('div').text

            image_tag = soup.find('img')
            if image_tag and 'src' in image_tag.attrs:
                data_attributes['HOME_IMAGE_URL'] = image_tag.attrs['src']

            # print('scrapping completed for {}'.format(redfin_url))
        except Exception as ex:
            data_attributes['ERROR'] = str(ex)
            traceback.print_exc()

        return data_attributes

    @staticmethod
    def google_search(row):

        try:
            google_query = GoogleScrapper._get_google_query(row)

            params = {'hl': 'en', 'q': google_query, 'start': 0, 'num': 10}
            if USE_PROXY:
                response = scraper_api_call(GOOGLE_SEARCH_URL, params=params, headers=google_headers)
            else:
                response = requests.get(GOOGLE_SEARCH_URL, params=params, headers=google_headers)

            if 'Our systems have detected unusual traffic from your computer network' in response.text:
                row['GOOGLE_CAPTCHA_SHOWN'] = datetime.now()
                print('get CAPTCHA')
                time.sleep(300)
                return row

            if not response.ok:
                print(response.text)
                return row

            soup = BeautifulSoup(response.text, features='lxml')

            redfin_url = None
            search_result_div = soup.find('div', {'id': 'search'})
            if search_result_div:
                rso_div = search_result_div.find('div', {'id': 'rso'})
                if rso_div:
                    results = [div_el.find('a')
                               for div_el in rso_div.find_all('div', {'class': 'g'}) if div_el.find('a')]
                    redfin_url = results[0].attrs['href'] if results else None
                    row['GOOGLE_REDFIN_LISTING_URL'] = redfin_url

            """
            if redfin_url:
                data_attributes = GoogleScrapper.scrape_url(row)
                row.update(data_attributes)
            """
        except Exception as ex:
            row['ERROR'] = str(ex)
            traceback.print_exc()

        # print('end of google search')
        return row

    def start_scrapper(self, open_data_csv_in, out_file):
        """
        scrape redfin estimate and image information
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

        df = df[df['MAIL_TO_STREET'].str.len() < 2]

        print(df.count())

        g_df = pd.read_csv('./../DATA/google/open_data_with_redfin_estimate_googlesearch_url_old.csv')
        df = df[~df['APN'].isin(g_df['APN'])]

        print(df.count())

        print(df.head(25).to_string())

        """
        results = []
        data_set = df.to_dict('records')
        for row in data_set[:5]:
            result = GoogleScrapper.google_search(row)
            results.append(result)
            
        df = pd.DataFrame(results)
        df.to_csv(out_file, index=False)
        """
        for index, df_chunk in enumerate(chunk(df, 5000)):
            csv_out_file = './../DATA/google/open_data_with_redfin_estimate_googlesearch_url_{}.csv'.format(index)
            print('scrapping started for batch: {}'.format(index))
            try:
                with multiprocessing.Pool(processes=multiprocessing.cpu_count()) as pool:
                    data_set = df_chunk.to_dict('records')
                    results = list(
                        tqdm(pool.imap(GoogleScrapper.google_search, data_set),
                             desc='Google search Bulk scraping', total=len(data_set), dynamic_ncols=True, miniters=0))
                df = pd.DataFrame(results)
                df.to_csv(csv_out_file, index=False)
                print('Bulk scrapping completed for chunk index: {}'.format(index))
                # time.sleep(500)
            except:
                print('error for chunk index: {}'.format(index))
                traceback.print_exc()
                time.sleep(60)

        print('End of scrapping')


def scrape_redfin():
    print('loading csv files from directory')

    csv_files = './../DATA/google/*.csv'
    df_list = []
    for file in glob(csv_files):
        print('loading file: {}'.format(file))
        df_list.append(pd.read_csv(file))

    df = pd.concat(df_list)
    df.drop_duplicates(inplace=True)
    df.replace({numpy.nan: None}, inplace=True)
    print(df.count())

    for index, df_chunk in enumerate(chunk(df, 2500)):
        csv_out_file = './../DATA/google/open_data_with_redfin_estimate_final_scrapping_{}.csv'.format(index)
        print('scrapping started for batch: {}'.format(index))
        try:
            with multiprocessing.Pool(processes=1) as pool:
                data_set = df_chunk.to_dict('records')
                results = list(
                    tqdm(pool.imap(GoogleScrapper.scrape_redfin_url, data_set),
                         desc='Redfin Bulk scraping', total=len(data_set), dynamic_ncols=True, miniters=0))
            df = pd.DataFrame(results)
            df.to_csv(csv_out_file, index=False)
            print('Bulk scrapping completed for chunk index: {}'.format(index))
            # time.sleep(500)
        except:
            print('error for chunk index: {}'.format(index))
            traceback.print_exc()
            time.sleep(60)

    print('End of scrapping')


def main():
    scrapper = GoogleScrapper()

    redfin_url = 'https://www.redfin.com/CA/Temecula/44634-Via-Lucido-92592/home/6250095'
    # scrapper.scrape_url(redfin_url)
    open_data_csv_in = './../DATA/open_data.csv'
    out_file = './../DATA/open_data_with_redfin_estimate_googlesearch.csv'
    scrapper.start_scrapper(open_data_csv_in, out_file)


if __name__ == '__main__':
    # main()
    scrape_redfin()
