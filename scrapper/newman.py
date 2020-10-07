import traceback
from io import StringIO
import pandas as pd
import requests
from bs4 import BeautifulSoup

from scrapper.constants import RED_FIN_BASE_URL, HEADERS


class SoldHomeScrapper:
    def __init__(self):
        print('Init method')
        self.headers = HEADERS

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
                response = requests.get(url, headers=self.headers)
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
                response = requests.get(download_link, headers=self.headers)
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
        print(df.columns)
        columns = {col: '_'.join(col.split()) for col in list(df.columns)}
        print(columns)
        df.rename(columns=columns, inplace=True)
        return df

    def scrape_redfin(self, urls):
        """
        Recently sold home - scrape data
        :param urls:
        :return:
        """

        print('Scrapping recently sold homes from :{}'.format(urls))

        download_links = self._scrape_download_url(urls)
        df = self._download_file(download_links)
        print(df.count())
        print(df.head().to_string())

        print('end of scrapping')
