import multiprocessing
import time
import traceback
from datetime import datetime

import numpy
import pandas as pd
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

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
    'accept-language': 'en-US,en;q=0.9',
    'cookie': 'CGIC=EhQxQzFDSFpOX2VuVVM5MzBVUzkzMCKHAXRleHQvaHRtbCxhcHBsaWNhdGlvbi94aHRtbCt4bWwsYXBwbGljYXRpb24veG1sO3E9MC45LGltYWdlL2F2aWYsaW1hZ2Uvd2VicCxpbWFnZS9hcG5nLCovKjtxPTAuOCxhcHBsaWNhdGlvbi9zaWduZWQtZXhjaGFuZ2U7dj1iMztxPTAuOQ; SID=4Ac1Dalw7QAu5yh8FyLZZnH6MKd07sOZ3OaXsIU5dAYA1gAwT70qKIN3u6GSUNZouGdpRw.; __Secure-3PSID=4Ac1Dalw7QAu5yh8FyLZZnH6MKd07sOZ3OaXsIU5dAYA1gAwSKokzK4iyTL3iuyB_Wqx_g.; HSID=AJ_KC7O2K-JCfag-9; SSID=AglTptvgyJuhyvBYK; APISID=6m2ybgJTfRZndZUJ/AhxCyGVNTntQH-8iM; SAPISID=wENJ0rDHENuZ5U0V/AO0sNJm7FwAP0RldG; __Secure-3PAPISID=wENJ0rDHENuZ5U0V/AO0sNJm7FwAP0RldG; SEARCH_SAMESITE=CgQIppEB; ANID=AHWqTUkRoQbRrjWL04rElrcKGijxYaL_OLkmUkiVeFWmcj-I8dpScoOXi9SngG_9; 1P_JAR=2020-12-10-11; NID=204=NJJyrRYLsBr1jkZTE_2A8sa_Xv6ezLgWES4ISQlzPV_18Ca7RdYsJpDdKYEBqqXGEaQTmMC4akA9K0weJ58MBP59uJZgw2dZl8Bqo6gHMELQ_ao3xCb8n7Qa_8P01011pb08xUEmrjmu_77IAzHKNbFcbBlhxxDX9wSS1he3tG6b3e2HStqXScrOkyNZ21YmOtl163alqoyBjDCGjOh3tTbBiiJcEdzp9R1lXtTKeg; SIDCC=AJi4QfHgdy4A3jOV_rAaFeuv7qeRtrX9964Uy02pJKALHiehLtp6Px7Q3z3nHq6WtyZimCiORHE; __Secure-3PSIDCC=AJi4QfEc_KO9LR4PgSdTXH4JNAULnRb8T4MjIWct_zJolSQaDtIWKZQIocyZRIGttwh-VjC92w; CGIC=IocBdGV4dC9odG1sLGFwcGxpY2F0aW9uL3hodG1sK3htbCxhcHBsaWNhdGlvbi94bWw7cT0wLjksaW1hZ2UvYXZpZixpbWFnZS93ZWJwLGltYWdlL2FwbmcsKi8qO3E9MC44LGFwcGxpY2F0aW9uL3NpZ25lZC1leGNoYW5nZTt2PWIzO3E9MC45; NID=204=uT2m91qfLRddN6JC1dkZ4wP5yHLoxuoAXSgQDERVbQyFC5CxMD0mln8cmq72LcxX_bBpzeczzoCokSRJFySM2gncbNjcq_cBa-PAgz6pweDh2I8w0bvhoDX6HVr11kGRHUw5jW3O4qrJh6J8wx7z2DyLtOg1lzKiz1a5spdGukA; 1P_JAR=2020-12-10-12; SIDCC=AJi4QfF5toJeaFQThxhLECNxCtT3OdTBybbs62-bHwHjhOzZMKfoiYKZ7euODt6KHqqcT5feZ-Y; __Secure-3PSIDCC=AJi4QfG43rEVuFTmil9iMPz8U2IH3vcHF8-C9rgUce1U-F7uufRoXuH9sZJFtOZvNlh8ik6-wg',
    'sec-fetch-dest': 'document',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'none',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36',
}

GOOGLE_SEARCH_URL = 'https://google.com/search'

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
    def scrape_url(redfin_url):
        # print('Scrapping url: {}'.format(redfin_url))

        data_attributes = {}
        try:
            response = requests.get(redfin_url, headers=reffin_headers)
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

            params = {'q': google_query, 'num': 10}
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
                data_attributes = GoogleScrapper.scrape_url(redfin_url)
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
        for index, df_chunk in enumerate(chunk(df, 500)):
            csv_out_file = './../DATA/google/open_data_with_redfin_estimate_googlesearch_url_{}.csv'.format(index)
            print('scrapping started for batch: {}'.format(index))
            try:
                with multiprocessing.Pool(processes=1) as pool:
                    data_set = df_chunk.to_dict('records')
                    results = list(
                        tqdm(pool.imap(GoogleScrapper.google_search, data_set),
                             desc='Google search Bulk scraping', total=len(data_set), dynamic_ncols=True, miniters=0))
                df = pd.DataFrame(results)
                df.to_csv(csv_out_file, index=False)
                print('Bulk scrapping completed for chunk index: {}'.format(index))
                time.sleep(500)
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
    main()
