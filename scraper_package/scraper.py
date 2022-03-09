from calendar import c
from selenium.webdriver import Chrome
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager
import time
from typing import Optional
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException
from selenium.common.exceptions import TimeoutException
import boto3
from sqlalchemy import create_engine
import urllib.request
import tempfile
import uuid
import pandas as pd
from tqdm import tqdm
import yaml

class Scraper:
    '''
    This class is a scraper that can be used for browsing different websites
    Parameters
    ----------
    url: str
        The link that we want to visit
    
    creds: str
        The file that contains the credentials for the database
    Attribute
    ---------
    driver:
        THis is the webdriver object
    '''
    def __init__(self, url: str, creds: str='config/RDS_creds.yaml'):
        self.driver = Chrome(ChromeDriverManager().install())
        self.driver.get(url)
        with open(creds, 'r') as f:
            creds = yaml.safe_load(f)
        DATABASE_TYPE = creds['DATABASE_TYPE']
        DBAPI = creds['DBAPI']
        HOST = creds['HOST']
        USER = creds['USER']
        PASSWORD = creds['PASSWORD']
        DATABASE = creds['DATABASE']
        PORT = creds['PORT']

        self.engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
        self.client = boto3.client('s3')

    def accept_cookies(self, xpath: str, iframe: Optional[str] = None):
        '''
        This method looks for and click on the accept ccokies button
        Parameters
        ----------
        xpath: str
            The xpath of the accept cookies button
        iframe: Optional[str]
            The id of the iframe in case there is one in front of the accept cookies button
        '''
        try:
            time.sleep(2)
            self.driver.switch_to.frame(iframe)
            cookies_button = (
                WebDriverWait(self.driver, 10)
                .until(EC.presence_of_element_located((
                    By.XPATH, xpath))
                    )
            )
            print(cookies_button)
            self.driver.find_element(By.XPATH, xpath).click()

        except TimeoutException:
            print('No cookies found')

    def look_for_search_bar(self,
                            xpath: str):
        '''
        Looks for the search bar given the xpat
        Parameters
        ----------
        xpath: str
            The xpath of the search bar
        Returns
        -------
        Optional[webdriver.element]
        '''
        try:
            time.sleep(1)
            search_bar = (
                WebDriverWait(self.driver, 5)
                .until(EC.presence_of_element_located(
                    (By.XPATH, xpath)
                    )
                    )
            )
            search_bar.click()
            return search_bar
        except TimeoutException:
            print('No search bar found')
            return None

    def send_keys_to_search_bar(self,
                                text: str,
                                xpath: str) -> None:
        '''
        Write something on a search bar
        Parameters
        ----------
        text: str
            The text we want to pass to the search bar
        xpath: str
            xpath of the search bar
        '''
        search_bar = self.look_for_search_bar(xpath)
        if search_bar:
            search_bar.send_keys(text)
        else:
            raise Exception('No search bar found')
    def find_container(self, xpath: str) -> None:
        '''
        Finds the container of items in a website
        '''
        return self.driver.find_element(By.XPATH, xpath)


class ScraperZoopla(Scraper):
    '''
    Scraper that works only for the zoopla website
    It will extract information about the price, n_bedrooms, n_bathrooms,
    and sqft of the properties in a certain location
    Parameters
    ----------
    location: str
        The location to look properties in
    Attributes
    ----------
    prop_dict: dict
        Contains price, bedrooms, bathrooms, and sqft of each property
    '''
    def __init__(self, location: str):
        super().__init__('https://www.zoopla.co.uk')
        self.prop_dict = {
            'ID': [],
            'Price': [],
            'Bedrooms': [],
            'Bathrooms': [],
            'Friendly_ID': [],
            }
        self.image_dict = {
            'ID': [],
            'Property ID': [],
            'Image Link': [],
        }
        df = pd.read_sql('properties_2', self.engine)
        self.friendly_id_scraped = list(df['Friendly_ID'])
        self.location = location

    def go_to_location(self):
        self.accept_cookies(xpath='//button[@id="save"]',
                            iframe='gdpr-consent-notice')
        self.send_keys_to_search_bar(
            text=self.location,
            xpath='//input[@id="header-location"]')
        time.sleep(1)
        list_locations = self.driver.find_element(By.XPATH, '//ul[@data-testid="autosuggest-list"]')
        time.sleep(1)
        list_locations.find_element(By.XPATH, './li').click()
        time.sleep(1)
        self.driver.find_element(By.XPATH, '//button[@data-testid="search-button"]').click()
        # container = self.find_container(xpath='//div[@class="css-1anhqz4-ListingsContainer earci3d2"]')
        # container.find_elements(By.XPATH)

    def find_container(self, xpath: str='//div[@class="css-1anhqz4-ListingsContainer e1awou0d2"]') -> None:
        return super().find_container(xpath)

    def get_links(self, container, href_xpath='.//a[@data-testid="listing-details-link"]') -> list:
        '''
        Gets the links of the properties in a certain location
        Parameters
        ----------
        container: WebDriverElement
            The container of the properties
        Returns
        -------
        list
            The links of the properties
        '''
        list_elements = container.find_elements(By.XPATH, './div')
        href_list = []
        for el in list_elements:
            href = el.find_element(By.XPATH, href_xpath).get_attribute('href')
            href_list.append(href)
        
        return href_list

    def get_info_in_link(self, href_list: list) -> None:
        '''
        Gets the information of the properties in a link
        '''

        for href in tqdm(href_list[:10]):
            friendly_id = href.split('/')[-2]
            if friendly_id in self.friendly_id_scraped:
                print('Already scraped')
                continue
            else:
                self.driver.get(href)
                time.sleep(1)
                try:
                    price = self.driver.find_element(By.XPATH, '//span[@data-testid="price"]').text
                except NoSuchElementException:
                    price = 'N/A'
                try:
                    n_bedrooms = self.driver.find_element(By.XPATH, '//span[@data-testid="beds-label"]').text
                except NoSuchElementException:
                    n_bedrooms = 'N/A'
                try:
                    n_bathrooms = self.driver.find_element(By.XPATH, '//span[@data-testid="baths-label"]').text
                except NoSuchElementException:
                    n_bathrooms = 'N/A'
                


                self.prop_dict['ID'].append(uuid.uuid4())
                self.prop_dict['Price'].append(price)
                self.prop_dict['Bedrooms'].append(n_bedrooms)
                self.prop_dict['Bathrooms'].append(n_bathrooms)
                self.prop_dict['Friendly_ID'].append(friendly_id)

                try: 
                    image_list = []
                    ol = self.driver.find_element(By.XPATH, '//ol[@class="css-1teujbt-SlideContainer e7zaxia6"]')
                    image_1 = ol.find_element(By.XPATH, './li[@data-testid="gallery-image" and @aria-hidden="false"]')
                    image_list.append(image_1.find_element(By.XPATH, './/img').get_attribute('src'))
                    next_button = self.driver.find_element(By.XPATH, '//button[@class="css-13rp99h-Button-ButtonWithIcon-Button-ButtonNext e7zaxia1"]')
                    next_button.click()
                    time.sleep(1)
                    ol = self.driver.find_element(By.XPATH, '//ol[@class="css-1teujbt-SlideContainer e7zaxia6"]')
                    image_2 = ol.find_element(By.XPATH, './li[@data-testid="gallery-image" and @aria-hidden="false"]')
                    image_list.append(image_2.find_element(By.XPATH, './/img').get_attribute('src'))

                except NoSuchElementException:
                    image_list = []

                with tempfile.TemporaryDirectory() as tmpdirname:
                    for i in range(len(image_list)):
                        time.sleep(2)
                        new_id = uuid.uuid4()
                        urllib.request.urlretrieve(image_list[i], tmpdirname + f'/{new_id}.jpg')
                        self.client.upload_file(tmpdirname + f'/{new_id}.jpg', 'march-bucket-test', f'{new_id}.jpg')
                        self.image_dict['ID'].append(new_id)
                        self.image_dict['Property ID'].append(self.prop_dict['ID'][-1])
                        self.image_dict['Image Link'].append(f'https://march-bucket-test.s3.amazonaws.com/{new_id}.jpg')


if __name__ == '__main__':
    bot = ScraperZoopla('London')
    bot.go_to_location()
    time.sleep(3)
    container = bot.find_container()
    links = bot.get_links(container)
    bot.get_info_in_link(links)