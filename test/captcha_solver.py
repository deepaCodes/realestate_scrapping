import time

from datetime import datetime
from random import uniform, randint
import numpy as np

from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from selenium import webdriver
from selenium.webdriver import DesiredCapabilities
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait

# Randomization Related
MIN_RAND = 0.64
MAX_RAND = 1.27
LONG_MIN_RAND = 4.78
LONG_MAX_RAND = 11.1


def solve_captcha(url):
    driver = get_chrome_driver()
    driver.get(url)

    time.sleep(5)
    driver.switch_to.frame(driver.find_elements_by_tag_name("iframe")[0])
    check_box = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.ID, "recaptcha-anchor")))

    WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, '//*[@id="recaptcha-anchor"]'))).click()
    driver.switch_to.default_content()

    print(driver.page_source)
    print('end')


chromedriver_path = 'D:/chromedriver_win32/87.0.4280.88/chromedriver.exe'
headless = False
user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.198 Safari/537.36'


def get_chrome_driver():
    """
    :return:
    """
    capabilities = DesiredCapabilities.CHROME.copy()
    capabilities['acceptSslCerts'] = True
    capabilities['acceptInsecureCerts'] = True

    # chrome driver
    options = Options()

    # disables these features to avoid detecting bot by website
    options.add_argument('--no-sandbox')  # Bypass OS security model
    options.add_argument('--start-maximized')
    options.add_argument('--disable-infobars')
    options.add_argument('--disable-extensions')
    options.add_argument('--disable-plugins-discovery')
    options.add_argument("--disable-blink-features")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ['enable-automation'])

    if headless:
        options.add_argument("--headless")  # Runs Chrome in headless mode.

    options.add_argument('user-agent={}'.format(user_agent))

    driver: webdriver.Chrome = webdriver.Chrome(executable_path=chromedriver_path,
                                                options=options,
                                                desired_capabilities=capabilities)

    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    driver.execute_cdp_cmd('Network.setUserAgentOverride', {"userAgent": user_agent})
    print(driver.execute_script("return navigator.userAgent;"))

    driver.maximize_window()

    print('web driver loaded')
    print(driver.capabilities)

    return driver


# Use time.sleep for waiting and uniform for randomizing
def wait_between(a, b):
    rand = uniform(a, b)
    time.sleep(rand)


def do_captcha(url):
    driver = get_chrome_driver()
    driver.get(url)
    driver.switch_to.default_content()
    print("Switch to new frame")
    iframes = driver.find_elements_by_tag_name("iframe")
    driver.switch_to.frame(driver.find_elements_by_tag_name("iframe")[0])

    print("Wait for recaptcha-anchor")
    check_box = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.ID, "recaptcha-anchor")))

    print("Wait")
    wait_between(MIN_RAND, MAX_RAND)

    action = ActionChains(driver)

    print("Click")
    check_box.click()

    print("Wait")
    wait_between(MIN_RAND, MAX_RAND)

    print("Switch Frame")
    driver.switch_to.default_content()
    iframes = driver.find_elements_by_tag_name("iframe")
    driver.switch_to.frame(iframes[2])

    print("Wait")
    wait_between(LONG_MIN_RAND, LONG_MAX_RAND)

    print("Find solver button")
    """
    capt_btn = WebDriverWait(driver, 50).until(
        EC.element_to_be_clickable((By.XPATH, '//button[@id="solver-button"]'))
    )
    """

    capt_btn = WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.TAG_NAME, 'button'))
    )

    print("Wait")
    wait_between(LONG_MIN_RAND, LONG_MAX_RAND)

    print("Click")
    capt_btn.click()

    print("Wait")
    wait_between(LONG_MIN_RAND, LONG_MAX_RAND)

    print("Wait")
    driver.implicitly_wait(5)
    print("Switch")
    driver.switch_to.frame(driver.find_elements_by_tag_name("iframe")[0])


if __name__ == '__main__':
    url = 'https://www.truepeoplesearch.com/details?streetaddress=32082%20CALLE%20BALAREZA&citystatezip=TEMECULA%2C%20CA%2092592&rid=0x0'
    #   solve_captcha(url)
    do_captcha(url)
