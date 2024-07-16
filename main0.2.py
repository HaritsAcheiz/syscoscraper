import csv
import pickle
import re
import sqlite3
from selectolax.parser import HTMLParser
from selenium.webdriver import Keys
from seleniumbase import SB, Driver
from httpx import AsyncClient, Cookies, Client
import os
from dataclasses import dataclass
from urllib.parse import urljoin
from dotenv import load_dotenv
from seleniumbase.common.exceptions import WebDriverException

load_dotenv()


@dataclass
class SyscoScraper:
    cookies: Cookies = None
    base_url: str = 'https://www.sysco.com'
    shop_url: str = 'https://shop.sysco.com'
    user_agent: str = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'

    # Supporting function
    def extract_integer(self, input_string):
        pattern = r'\((\d+)\)'
        match = re.search(pattern, input_string)
        if match:
            return int(match.group(1))
        else:
            return None

    def extract_price(self, input_string):
        pattern = r'\$\d+\.\d{2}'
        match = re.search(pattern, input_string)
        if match:
            return match.group(0)  # Return the matched string
        return None

    def scrape(self):
        try:
            driver = Driver(
                proxy='udksqrvp-2:0irkbd10fjkn@144.48.39.171:80',
                headless=True,
                headless2=True,
                block_images=True,
                incognito=True)

            print('Login...', end='')
            # Index page
            driver.open(self.base_url)
            breakout = False
            while not breakout:
                try:
                    driver.click('a[aria-label="Navigate to Sign In"]')
                    breakout = True
                except WebDriverException as e:
                    print(e)
                    driver.refresh()

            # Login page
            breakout = False
            while not breakout:
                try:
                    driver.refresh()
                    driver.type('input[data-id="txt_login_email"]', text=os.getenv('SYSCOEMAIL'), timeout=30)
                    driver.wait_for_element_present('button[data-id="btn_next"]', timeout=20).click()
                    driver.wait_for_element_present('input#okta-signin-password', timeout=30)
                    breakout = True
                except WebDriverException as e:
                    print(e)
                    if driver.find_element('div.login_input_email_error'):
                        driver.refresh()

            breakout = False
            while not breakout:
                try:
                    # Pass input page
                    driver.type('input#okta-signin-password', text=os.getenv('SYSCOPASS') + Keys.RETURN, timeout=20)
                    driver.wait_for_element_present('div.category-grid-button', timeout=120)
                    breakout = True
                except WebDriverException as e:
                    print(e)
                    driver.refresh()

            cookies = driver.get_cookies()
            pickle.dump(cookies, open('cookies.pkl', 'wb'))
            print('Completed')

            cats = self.get_categories(driver)
            self.fetch_all_data(driver, cats=cats[0:1])

        finally:
            driver.quit()


    def get_categories(self, driver):
        print('Getting Categories...', end='')
        # Catalog page
        driver.open(urljoin(self.shop_url, '/app/catalog'))
        breakout = False
        while not breakout:
            try:
                driver.wait_for_element_present('button.btn.btn-link.sysco-6.btn-sm', timeout=70)
                breakout = True
            except WebDriverException as e:
                print(e)
                driver.refresh()

        cat_elements = driver.find_elements('button.btn.btn-link.sysco-6.btn-sm')
        cats = []
        for elem in cat_elements[1:]:
            number_of_product = self.extract_integer(elem.text)
            cat_url = urljoin(self.shop_url, f'/app/catalog?BUSINESS_CENTER_ID={elem.get_attribute("value")}')
            cats.append((number_of_product, cat_url))
        print('Completed')

        return cats


    def fetch_data(self, driver, url):
        print(f'Extracting {url} ...', end='')
        driver.open(url)
        breakout = False
        while not breakout:
            try:
                driver.wait_for_element_present('div.catalog-cards-wrapper', timeout=70)
                breakout = True
            except WebDriverException as e:
                print(e)
                driver.refresh()

        price_selector = 'div > div > a > div > div.row.product > div.price-wrapper > div > div > div > div > div > span'
        breakout = False
        while not breakout:
            if self.extract_price(driver.find_element(price_selector).text):
                html = driver.get_page_source()
                breakout = True
            driver.sleep(1)
        print('Completed')

        return url, html


    def fetch_all_data(self, driver, cats):
        for cat in cats:
            urls = [f'{cat[1]}&typeAhead=false&page={page}&sort=0' for page in range(1, round(cat[0]/24))]
            for url in urls:
                htmls = list()
                html = self.fetch_data(driver, url=url)
                htmls.append(html)

                self.insert_to_db(htmls)


    def insert_to_db(self, htmls):
        print('Inserting to database...', end='')
        conn = sqlite3.connect("sysco.db")
        curr = conn.cursor()
        curr.execute(
            """
            CREATE TABLE IF NOT EXISTS products(
            url TEXT,
            html BLOB
            ) 
            """
        )

        for html in htmls:
            curr.execute(
                "INSERT INTO products (url, html) VALUES(?,?)",
                html)
            conn.commit()
        print('Completed')


    def load_from_db(self):
        print('Loading data ...', end='')
        conn = sqlite3.connect("sysco.db")
        curr = conn.cursor()
        curr.execute("SELECT * FROM products")
        htmls = curr.fetchall()
        print('Completed')

        return htmls


    def write_to_csv(self, datas):
        print('Writing to csv ...', end='')
        header = ['id', 'brand', 'name', 'image', 'cs_price_usd', 'ea_price_usd', 'cs_price_before_usd',
                  'ea_price_before_usd', 'link']
        with open('sysco_products_sample.csv', 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=header)
            writer.writeheader()
            writer.writerows(datas)
        print('Completed')


    def get_data(self, htmls):
        print('Parsing data...', end='')
        products = list()
        for html in htmls:
            tree = HTMLParser(html[1])
            product_elems = tree.css('div.catalog-cards-wrapper > div.fd.product-card-container')
            for elem in product_elems:
                product = dict()
                product['id'] = elem.css_first('div.selectable-supc-label').text(strip=True)
                product['brand'] = elem.css_first('div.brand').text(strip=True)
                product['name'] = elem.css_first('div.product-name').text(strip=True)
                product['image'] = elem.css_first('img').attributes.get('src')

                price_elems = elem.css('span.price-value')
                if len(price_elems) > 1:
                    product['cs_price_usd'] = self.extract_price(price_elems[0].text(strip=True))
                    product['ea_price_usd'] = self.extract_price(price_elems[1].text(strip=True))
                else:
                    product['cs_price_usd'] = self.extract_price(elem.css_first('span.price-value').text(strip=True))
                    product['ea_price_usd'] = None

                price_before_elems = elem.css('div.row.original')
                if len(price_before_elems) > 1:
                    product['cs_price_before_usd'] = self.extract_price(price_before_elems[0].text(strip=True))
                    product['ea_price_before_usd'] = self.extract_price(price_before_elems[1].text(strip=True))
                else:
                    product['cs_price_before_usd'] = self.extract_price(elem.css_first('div.row.original').text(strip=True))
                    product['ea_price_before_usd'] = None

                product['link'] = urljoin(self.shop_url, elem.css_first('a.product-card-link').attributes.get('href'))

                products.append(product.copy())
        print('Completed')

        return products

if __name__ == '__main__':
    scraper = SyscoScraper()
    # scraper.scrape()
    htmls = scraper.load_from_db()
    products = scraper.get_data(htmls)
    scraper.write_to_csv(products)
    print('Done Successfully')

