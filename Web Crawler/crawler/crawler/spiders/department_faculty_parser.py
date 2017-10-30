import os
import pandas as pd
import re
import scrapy
from crawler.xpath_generic_extractor import generic_get_anchor_and_text
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
        self.faculty_data = pd.read_csv(file_path, encoding='latin-1').dropna(subset=['XPATH'])

    def start_requests(self):
        # Iterate and yield request respectively
        for index, value in self.faculty_data.iterrows():
            school = value['Name']
            school_url = value['URL']
            url_xpath = value['XPATH']
            remark = value['Remark']
            department_or_faculty = value['Department or Faculty']
            sub_request = Request(school_url, self.parse)
            sub_request.meta['school'] = school
            sub_request.meta['url_xpath'] = url_xpath
            sub_request.meta['remark'] = remark
            sub_request.meta['department_or_faculty'] = department_or_faculty
            yield sub_request

    def parse(self, response):
        # Fetch meta data
        school = response.meta['school']
        url_xpath = response.meta['url_xpath']
        remark = response.meta['remark']
        department_or_faculty = response.meta['department_or_faculty']

        # If link has been extracted and no xpath is presented
        if url_xpath == '-':
            url_list = remark.split('; ')
            for dept_url in url_list:
                sub_request = Request(dept_url, self.parse_link)
                sub_request.meta['school'] = school
                sub_request.meta['department_or_faculty'] = department_or_faculty
                yield sub_request
            return

        # Parse the corresponding links and yield items
        url_xpath_ex_href = url_xpath.replace('/@href', '')
        link_element = generic_get_anchor_and_text(response, url_xpath_ex_href, url_xpath)
        department_item = DepartmentItem()
        for text, href in link_element.items():
            real_url = response.follow(href.split()[0]).url

            # If the title is not available, accordingly visit the page to get the tile
            # Yield the item otherwise
            if re.sub(r'\([1-9]+\)', '', text) != '':
                department_item['url'] = real_url
                department_item['school_name'] = school
                department_item['title'] = text
                department_item['department_or_faculty'] = department_or_faculty
                yield department_item
            else:
                sub_request = Request(real_url, self.parse_link)
                sub_request.meta['school'] = school
                sub_request.meta['department_or_faculty'] = department_or_faculty
                yield sub_request

        # If there is pagination
        if remark == 'Pagination':
            if response.url[-1] == 'A':
                for index in range(1, 26):
                    sub_request = Request(response.url[:-1] + '%c' % (65 + index), self.parse)
                    sub_request.meta['school'] = school
                    sub_request.meta['url_xpath'] = url_xpath
                    sub_request.meta['remark'] = ''
                    sub_request.meta['department_or_faculty'] = department_or_faculty
                    yield sub_request

    def parse_link(self, response):
        # Fetch meta data
        school = response.meta['school']
        self.logger.info('Visiting %s (%s) for extra information' % (response.url, school))

        # Yield item
        department_item = DepartmentItem()
        department_item['url'] = response.url
        department_item['school_name'] = school
        department_item['title'] = response.xpath('//title/text()').extract_first()
        department_item['department_or_faculty'] = response.meta['department_or_faculty']
        yield department_item
