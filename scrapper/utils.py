import re
import traceback

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
    profit = (sell_price - SELL_COMMISSION_PCT / 100) - purchase_price
    return profit
