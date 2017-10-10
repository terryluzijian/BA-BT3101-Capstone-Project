import json
import os
import re
import requests
from lxml import html
from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions
from selenium.webdriver.common.by import By
from urllib.parse import urljoin


class UniversityRankUpdater(object):

    WORLD_RANKING_HOMEPAGE = 'https://www.timeshighereducation.com/world-university-rankings'
    WORLD_RANKING_HOMEPAGE_ANCHOR_XPATH = '//a[text()[normalize-space(.)] = \'World University Rankings\']/@href'
    YEAR_REGEX = r'(20[01]{1}[0-9]{1})'

    RANKING_PAGE_TABLE_ID = 'datatable-1_wrapper'
    UNI_HREF_XPATH = '//a[attribute::*[contains(., \'ranking-institution-title\')]]/@href'
    UNI_TITLE_XPATH = '//a[attribute::*[contains(., \'ranking-institution-title\')]]/text()'
    BUTTON_XPATH = lambda x: '//li[@class[contains(., \'paginate_button\')]]/a[text() = \'%d\']' % x

    TIMEOUT = 10
    UNI_LST_MAXIMUM_LEN = 100

    FILE_NAME = 'UNIVERSITY_LINK_PRE.json'

    def __init__(self, uni_lst_maximum_len=None):
        self.UNI_LST_MAXIMUM_LEN = uni_lst_maximum_len if uni_lst_maximum_len is not None else self.UNI_LST_MAXIMUM_LEN
        init_request = requests.get(self.WORLD_RANKING_HOMEPAGE)
        ranking_hp_tree = html.fromstring(init_request.content)
        ranking_link = ranking_hp_tree.xpath(self.WORLD_RANKING_HOMEPAGE_ANCHOR_XPATH)
        ranking_link = list(filter(lambda x: re.search(self.YEAR_REGEX, x), ranking_link))
        if len(ranking_link) >= 2:
            ranking_link = sorted(ranking_link, key=lambda x: re.findall(self.YEAR_REGEX, x), reverse=True)
        try:
            self.next_url = urljoin(init_request.url, ranking_link[0])
        except IndexError:
            pass
        self.driver = webdriver.PhantomJS()
        self.driver.set_window_size(1400, 1000)
        self.driver.get(self.next_url)
        try:
            element_present = expected_conditions.presence_of_element_located((By.ID, self.RANKING_PAGE_TABLE_ID))
            WebDriverWait(self.driver, self.TIMEOUT).until(element_present)
        except TimeoutException:
            print('Timed out waiting for page to load')

    def fetch_university(self):
        _beginning_round = 1
        _next_url_title = []
        _next_url_uni = []
        while len(_next_url_uni) < self.UNI_LST_MAXIMUM_LEN:
            rank = html.fromstring(self.driver.page_source)
            _next_url_title.extend(rank.xpath(self.UNI_TITLE_XPATH))
            _next_url_uni.extend(rank.xpath(self.UNI_HREF_XPATH))
            button = self.driver.find_element_by_xpath(self.BUTTON_XPATH(_beginning_round + 1))
            self.driver.execute_script("arguments[0].click();", button)
            _beginning_round += 1

        with open('UNI_LINK_LIST_PRE.json', 'w') as outfile:
            json.dump(dict(zip(_next_url_title, _next_url_uni)), outfile, indent=2)

    def get_json_list(self):
        parent_path = os.path.abspath(os.path.join(os.path.dirname(__file__),".."))
        file_name = os.path.isfile(parent_path + '/data/%s' % self.FILE_NAME)
        if not os.path.isfile(file_name):
            self.fetch_university()
        return json.loads(open(file_name).read())
