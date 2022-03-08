import unittest
import scraper
from selenium.webdriver.common.by import By
import time


class TestScraper(unittest.TestCase):
    def setUp(self):
        self.bot = scraper.ScraperZoopla('London')
    
    def test_accept_cookies(self):
        self.bot.accept_cookies(xpath='//button[@id="save"]',
                                iframe='gdpr-consent-notice')
        self.bot.driver.find_element(By.XPATH, '//div[@class="css-1onqvz2-LogoWrapper e1r69hd79"]')

    def test_scrape_properties(self):
        self.bot.scrape_properties()
        time.sleep(5)
        actual_value = self.bot.driver.current_url
        expected_value = "https://www.zoopla.co.uk/for-sale/property/london/?view_type=list&q=London&results_sort=newest_listings&search_source=home"
        self.assertEqual(actual_value, expected_value)    


if __name__ == '__main__':
    unittest.main(verbosity=0)