import json
import os
import re
import sys
import scrapy
from crawler.items import DirectoryItem
from datetime import datetime
from lxml import html
from random import shuffle
from scrapy import Request
from scrapy.linkextractors import IGNORED_EXTENSIONS
from scrapy.utils.url import url_has_any_extension, parse_url
from tld import get_tld
from urllib.parse import urljoin


class UniversityDirectoryLocator(scrapy.Spider):

    name = 'directory'
    SCHOOL_HOMEPAGE_DATA = 'UNIVERSITY_LINK.json'
    IGNORED_REGEX = [r'[^a-zA-Z]?(%s)[^a-zA-Z]?' % s for s in \
                     ('news', 'library', 'privacy', 'policies', 'events')]

    custom_settings = {
        'DEPTH_LIMIT': 1,
        'DEPTH_PRIORITY': 1,
        'DEPTH_STATS_VERBOSE': True
    }
    MAX_DEPTH = custom_settings['DEPTH_LIMIT']

    def __init__(self, *args, **kwargs):
        super(UniversityDirectoryLocator, self).__init__(*args, **kwargs)

        # Get University links and add their top domains to the allowed domains for crawler
        # Shuffle university name for later iteration
        self.university_link = json.load(open(self.return_file_path()))
        self.allowed_domains = list(map(lambda x: get_tld(x['Homepage']), list(self.university_link.values())))
        self.shuffled_name = list(self.university_link.keys())
        shuffle(self.shuffled_name)

        # Get file extensions to deny
        self.denied_extension = list(map(lambda x: '.%s' % x, IGNORED_EXTENSIONS))

    def start_requests(self):
        # Iterate through shuffled list of university names
        for university_name in self.shuffled_name:
            school_url = self.university_link[university_name]['Homepage']

            # Yield request with some meta data
            request = Request(school_url, self.parse)
            request.meta['current_depth'] = 0
            request.meta['initial_url'] = ''
            request.meta['school_name'] = university_name
            request.meta['href_dict_combined'] = {}
            request.meta['url_to_visit'] = school_url
            yield request

    def parse(self, response):
        # Fetch meta data
        depth = response.meta['current_depth']
        uni_name = response.meta['school_name']
        all_href_scanned = response.meta['href_dict_combined']
        initial_url = response.meta['initial_url']
        url_to_visit = response.meta['url_to_visit']

        # Log current url visited
        if re.search(r'render\.html\?url=(.+)', response.url):
            url_redirected = re.search(r'render\.html\?url=(.+)', response.url).group(1)
        else:
            url_redirected = response.url
        self.log_info('Visiting [%s] at depth %d from [%s]' % (url_redirected, depth, initial_url))
        if url_to_visit != url_redirected:
            self.log_info('Redirected from %s to %s' % (url_redirected, url_to_visit))
        url_to_parse = parse_url(url_redirected)
        if len(url_to_parse.path.split('/')) > self.MAX_DEPTH + 1:
            return

        # Get basic information
        page_title = response.xpath('//title/text()').extract_first()

        # Search for anchors
        anchors = response.xpath('//a[descendant-or-self::*[@href]]').extract()
        anchors_select = list(map(lambda x: html.fromstring(x), anchors))

        # Get href and textual content
        href = list(map(lambda x: x.xpath('//*//@href')[0], anchors_select))
        text = list(map(lambda x: x.xpath('//*[not(self::style) and not(self::script)]//text()'), anchors_select))
        text = list(map(lambda x: ' '.join(' '.join(x).split()), text))

        # Filter href list
        href_joined = list(map(lambda x: urljoin(url_redirected, x), href))
        href_text_dict = dict(zip(href_joined, text))
        href_text_dict_current_integrate = {**all_href_scanned, **href_text_dict}
        href_text_dict = {key: item for key, item in href_text_dict.items() \
                          if key not in all_href_scanned.keys()}

        directory_item = DirectoryItem()
        for href, text in href_text_dict.items():
            directory_item['url'] = url_redirected
            directory_item['depth'] = depth
            directory_item['school_name'] = uni_name
            directory_item['page_title'] = page_title
            directory_item['initial_url'] = initial_url
            directory_item['href'] = href
            directory_item['text'] = text
            yield directory_item

        # Yield reqeust accordingly within the specified extension and regex
        for href_key in href_text_dict:
            if not url_has_any_extension(href_key, self.denied_extension):
                if self.iterate_through_regex(href_key):
                    url_parsed = parse_url(href_key)
                    if url_parsed.query == '':
                        request = Request(href_key, self.parse)
                        request.meta['current_depth'] = depth + 1
                        request.meta['initial_url'] = url_redirected
                        request.meta['school_name'] = uni_name
                        request.meta['href_dict_combined'] = href_text_dict_current_integrate
                        request.meta['url_to_visit'] = href_key
                        yield request

    def iterate_through_regex(self, target_string):
        for regex in self.IGNORED_REGEX:
            if re.search(regex, target_string):
                return False
        return True

    def log_info(self, log_string):
        # Get current timestamp
        current_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # Imitate scrapy logging particulars
        scrapy_specific = '[%s.logger] INFO:' % self.__name__()
        final_string_line = '%s %s %s' % (current_timestamp, scrapy_specific, log_string)
        sys.stdout.write(final_string_line)
        sys.stdout.write('\n')
        return final_string_line

    def return_file_path(self, data_file_name=SCHOOL_HOMEPAGE_DATA):
        parent_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
        file_path = parent_path + '/data/%s' % data_file_name
        return file_path

    def __name__(self):
        return 'UniversityDirectoryLocator'
