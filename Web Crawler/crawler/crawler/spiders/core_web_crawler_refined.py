import os
import pandas
import re
import scrapy
from crawler.items import DirectoryProfilePageItem
from crawler.similarity_navigator import SimilarityNavigator
from crawler.xpath_generic_extractor import check_word_filter, MENU_TEXT_FILTER, get_title_h1_h2, get_main_content, \
                                            get_main_content_text, get_main_content_excluding_menu, \
                                            get_header, get_menu, get_main_content_unique
from random import shuffle
from scrapy import Request
from scrapy.http import Response
from scrapy_splash import SplashRequest
from scrapy.utils.url import parse_url
from tld import get_tld


class UniversityWebCrawler(scrapy.Spider):

    # Core crawler that takes in university faculty/department page and searches for corresponding sub-branch
    # pages identifiable as department/staff directory for personal profile extraction purpose

    # Initiate name for calling and department links for subsequent shuffling and yielding of request
    name = 'university_testing'

    # Get department link from the file name stored as a class attribute using pandas
    # Shuffle the link for a random start to avoid over-frequent crawling
    DEPARTMENT_DATE_FILE_NAME = 'DEPARTMENT_FACULTY.csv'
    data_file_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..')) + '/data/'
    department_data = pandas.read_csv(data_file_path + DEPARTMENT_DATE_FILE_NAME)
    department_data_index = department_data.index

    # Get top domain by iterating through the department links to avoid going beyond the allowed domain
    allowed_domains_pre = set()
    department_url = department_data['url']
    for url in department_url:
        allowed_domains_pre.add(get_tld(url))
    allowed_domains = list(allowed_domains_pre)

    # Import and initiate similarity navigator and override Scrapy custom settings
    SIMILARITY_NAVIGATOR = SimilarityNavigator()
    custom_settings = {
        # Take a depth-first search algorithm by setting a negative priority or positive otherwise
        'DEPTH_LIMIT': 4,
        'DEPTH_PRIORITY': -1,
        'DEPTH_STATS_VERBOSE': True,

        'CONCURRENT_REQUESTS': 128,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 2,
        'DOWNLOAD_DELAY': 1,
    }
    DEPTH_LIMIT_DEPARTMENT = 3

    def __init__(self, *args, **kwargs):
        # Initiate spider object and shuffling urls to start crawling
        super(UniversityWebCrawler, self).__init__(*args, **kwargs)

        # TODO: Log basic information

        self.shuffled_index = list(UniversityWebCrawler.department_data_index)
        shuffle(self.shuffled_index)

    def start_requests(self):
        # Use normal request by default for department homepage for faster loading speed
        # Use splash request by default otherwise for possible AJAX-inclusive pages with rendering
        for individual_index in self.shuffled_index:
            # Get data from the specific row
            individual_data = UniversityWebCrawler.department_data.loc[individual_index]
            university_name = individual_data['school_name']
            url_link = individual_data['url']
            link_title = individual_data['title']

            # Yield request corresponding with preliminary meta data including university name and page title
            request = Request(url_link, callback=self.parse_menu)
            request.meta['Original Start'] = url_link
            request.meta['University Name'] = university_name
            request.meta['Link'] = url_link  # Link read as input
            request.meta['Title'] = link_title  # Title read as input
            request.meta['Title Hierarchy'] = link_title
            request.meta['Previous Link'] = ''  # Previous link
            request.meta['depth'] = 0
            yield request

    def parse_menu(self, response):
        # Parse menu content of the current page to locate department or people components
        # Stop crawling if encountered 404 error and report
        if self.report_basic_information(response, response.meta)['404']:
            return

        # Start core content analysis including menu and navigation content parsing
        current_response = response
        if not response.meta.get('From parse_department', False):
            target_content = UniversityWebCrawler.SIMILARITY_NAVIGATOR.get_target_content(response)
        else:
            target_content = UniversityWebCrawler.SIMILARITY_NAVIGATOR.get_target_content(response,
                                                                                          parse_only_people=True)
        self.logger.info('Requesting %s at depth %s with content %s at parse_menu' % (response.url,
                                                                                      response.meta['depth'],
                                                                                      str(target_content)))

        # Iterate through each target content and get relevant attribute to store in meta data and
        # pass to next level of parsing
        for target in target_content:
            # Target content follows (title, link, similarity, tag)
            target_title = target[0]
            target_link = target[1]
            target_tag = target[2]

            # Yield request to new meta data with normal request for department tagged link
            # and splash request for link tagged with people
            target_meta = {
                # Link-related data
                'Link': target_link,
                'Title': target_title,
                'Past Response': [current_response],
                'depth': response.meta['depth'] + 1,

                # Some basic record-based data
                'University Name': response.meta['University Name'],
                'Original Start': response.meta['Original Start'],
                'Previous Link': response.url
            }
            if target_tag == 'PEOPLE':
                target_request = Request(target_link, self.parse_people, meta=target_meta)
                target_meta['Original Start'] = response.url
                yield target_request
            elif target_tag == 'DEPARTMENT':
                target_request = Request(target_link, self.parse_department, meta=target_meta)
                yield target_request

    def parse_department(self, response):
        # Parse main content of the current page recursively
        basics = self.report_basic_information(response, response.meta)
        if basics['404']:
            return

        # If the redirected is presented with an actually shorter path (e.g. /xxx/xxx/xxx -> /yyy)
        if basics['Redirected']:
            redirect_parsed = self.get_netloc_and_path_level(response.url)
            previous_parsed = self.get_netloc_and_path_level(response.meta['Previous Link'])
            if redirect_parsed[2] < previous_parsed[2]:  # 2: index of len(path):
                response.meta['depth'] = 0
                response.meta['Previous Link'] = response.url
                response.meta['From parse_department'] = True
                yield Request(response.url, self.parse_menu, meta=response.meta)
                return

        # Start core content analysis including main content parsing
        current_response = response
        current_response_parsed_dict = {
            name: element
            for name, element in zip(['Title', 'Link', 'Netloc', 'Path', 'Path Level'],
                                     [response.meta['Title'], response.url] +
                                     list(self.get_netloc_and_path_level(current_response.url)))
        }
        main_content = get_main_content_unique(response, response.meta['Past Response'])
        main_content_parsed = [[text, link] + list(self.get_netloc_and_path_level(link))
                               for text, link in main_content.items()]
        self.logger.info('Requesting %s at depth %s at parse_department' % (response.url,
                                                                            response.meta['depth']))

        # Compare page-level data including domain name and path level
        for content_item in main_content_parsed:
            # Each item follows (title, link, netloc, path element, path level)
            content_title = content_item[0]
            content_link = content_item[1]
            content_netloc = content_item[2]
            content_path_pair = (content_item[3], content_item[4])

            content_meta = {
                # Link-related data
                'Link': content_link,
                'Title': content_title,
                'depth': response.meta['depth'] + 1,

                # Some basic record-based data
                'University Name': response.meta['University Name'],
                'Original Start': response.meta['Original Start'],
                'Previous Link': response.url
            }
            if (current_response_parsed_dict['Netloc'] != content_netloc) | \
               (content_path_pair[4] < current_response_parsed_dict['Path Level']):
                # If encountered a href with lower path level or different netloc (e.g. a sub-domain)
                # Yield back to page menu parsing at parse_menu
                # Set depth back to 0
                content_meta['depth'] = 0
                content_meta['From parse_department'] = True
                target_request = Request(content_link, self.parse_menu, meta=content_meta)
                yield target_request
            else:
                # Recursively yield request at parse_department level otherwise until reaching certain depth
                content_meta['Past Response'] = response.meta['Past Response'] + [current_response]
                target_request = Request(content_link, self.parse_department, meta=content_meta)
                yield target_request

    def parse_people(self, response):
        pass

    def parse(self, response):
        pass

    def report_basic_information(self, response, response_meta):
        # Report redirecting information
        is_redirected = response.url != response_meta['Link']
        is_404 = response.status == 404
        if is_redirected:
            self.logger.info('Redirecting from %s to %s for %s, %s' % (response_meta['Link'], response.url,
                                                                       response_meta['Title'],
                                                                       response_meta['University Name']))

        # Report 404 Error
        if is_404:
            self.logger.error('404 Visiting Error for link %s (%s, %s)' % (response.url, response_meta['Title'],
                                                                           response_meta['University Name']))

        # Return back 404 Error
        return {
            '404': is_404,
            'Redirected': is_redirected
        }

    @staticmethod
    def get_netloc_and_path_level(target_url):
        parsed = parse_url(target_url)
        path = list(filter(lambda x: len(x) > 0, parsed.path.split('/')))
        return parsed.netloc, path, len(path)
