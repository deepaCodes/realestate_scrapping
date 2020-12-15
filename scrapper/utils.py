import re
import traceback
from urllib import parse

import requests
from bs4 import BeautifulSoup
from scraper_api import ScraperAPIClient

from scrapper.constants import SELL_COMMISSION_PCT


def extract_house_no(address):
    """
    Extract house number from address
    :param address:
    :return:
    """
    try:
        house_no = int(re.search(r'\d+', address)[0])
        print(house_no)
        return house_no
    except:
        traceback.print_exc()

    return None


def calculate_profit(sell_price, purchase_price):
    """

    :param sell_price:
    :param purchase_price:
    :return:
    """
    profit = (sell_price - SELL_COMMISSION_PCT / 100) - purchase_price
    return profit


def scrape_mortgage_rates():
    """
    :return:
    """
    url = 'http://www.freddiemac.com/pmms/pmms30.html'

    response = requests.get(url)
    if not response.ok:
        print(response.text)
        return

    soup = BeautifulSoup(response.text, 'lxml')
    tables = soup.find_all('div', {'class': 'overflow-horizontal'})
    result = {}

    for div_table in tables:
        table = div_table.find('table')
        thead = table.find('thead')
        years = [int(row.text) for row in thead.find_all('th') if str(row.text).isdigit()]
        # print(years)
        tbody_list = table.find_all('tbody')
        # months = [row.text for row in tbody_list[1].find_all('th')[:12]]
        monthly_rates = {
            tr_el.find('th').text: [float(td_el.text) if td_el.text.replace('.', '', 1).isdigit() else None
                                    for index, td_el in enumerate(tr_el.find_all('td')) if index % 2 == 0]
            for tr_el in tbody_list[1].find_all('tr')[:12]}

        for index, year in enumerate(years):
            result[year] = {key: value[index] for key, value in monthly_rates.items()}

    # print(result)
    # print('Mortgage rates scrapped')
    return result


mortgage_rates = scrape_mortgage_rates()


def get_mortgage_rate(year, month):
    """

    :param year:
    :param month:
    :return:
    """

    if year and month:
        monthly_rate = mortgage_rates[int(year)]
        pct_rate = monthly_rate[month]
        # print('mortgage pct rate: {}'.format(pct_rate))
        return pct_rate

    return None


def chunk(seq, size):
    return (seq[pos:pos + size] for pos in range(0, len(seq), size))


client = ScraperAPIClient('1ee0040760aa540e5a689a76f10aaa84')


def scraper_api_call(api, params, headers):
    if params:
        api_url = '{}?{}'.format(api, parse.urlencode(params))
    else:
        api_url = api

    # print('proxy api')
    # print('SCRAPER API CLIENT url: {}'.format(api_url))
    response = client.get(url=api_url, headers=headers, country_code='US')

    return response
