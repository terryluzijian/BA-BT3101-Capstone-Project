import os
import pandas as pd
import scrapy
from crawler.xpath_generic_extractor import GenericExtractor
from crawler.items import DepartmentItem
from scrapy import Request


class DepartmentParser(scrapy.Spider):

    # This class is intended to read department faculty website and extract
    # link from given xpath, including some exception handling

    name = 'department'
    FACULTY_DATA = 'DEPARTMENT_FACULTY_WITH_XPATH.csv'

    def __init__(self, *args, **kwargs):
        # Get faculty link for each university
        super(DepartmentParser, self).__init__(*args, **kwargs)
        parent_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
        file_path = parent_path + '/data/%s' % self.FACULTY_DATA
        self.faculty_data = pd.read_csv(file_path).dropna(subset=['XPATH'])
        self.generic_extractor = GenericExtractor()

    def start_requests(self):
        # Iterate and yield request respectively
        for index, value in self.faculty_data.iterrows():
            school = value['Name']
            school_url = value['URL']
            url_xpath = value['XPATH']
            remark = value['Remark']
            sub_request = Request(school_url, self.parse)
            sub_request.meta['school'] = school
            sub_request.meta['url_xpath'] = url_xpath
            sub_request.meta['remark'] = remark
            yield sub_request

    def parse(self, response):
        # Fetch meta data
        school = response.meta['school']
        url_xpath = response.meta['url_xpath']
        remark = response.meta['remark']

        # If link has been extracted and no xpath is presented
        if url_xpath == '-':
            url_list = remark.split('; ')
            for dept_url in url_list:
                sub_request = Request(dept_url, self.parse_link)
                sub_request.meta['school'] = school
                yield sub_request
            return

        # Parse the corresponding links and yield items
        url_xpath_ex_href = url_xpath.replace('/@href', '')
        link_element = self.generic_extractor.generic_get_anchor_and_text(response, url_xpath_ex_href, url_xpath)
        department_item = DepartmentItem()
        for href, text in link_element.items():
            department_item['url'] = response.follow(href.split()[0]).url
            department_item['school_name'] = school
            department_item['title'] = text
            yield department_item

        # If there is pagination
        if remark == 'Pagination':
            if response.url[-1] == 'A':
                for index in range(1, 26):
                    sub_request = Request(response.url[:-1] + '%c' % (65 + index), self.parse)
                    sub_request.meta['school'] = school
                    sub_request.meta['url_xpath'] = url_xpath
                    sub_request.meta['remark'] = ''
                    yield sub_request

    def parse_link(self, response):
        # Fetch meta data
        school = response.meta['school']

        # Yield item
        department_item = DepartmentItem()
        department_item['url'] = response.url
        department_item['school_name'] = school
        department_item['title'] = response.xpath('//title/text()').extract_first()
        yield department_item
