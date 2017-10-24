import logging
import os
import pandas
import scrapy
from crawler.similarity_navigator import SimilarityNavigator
from crawler.xpath_generic_extractor import get_main_content, get_main_content_excluding_menu, get_header, get_menu
from random import shuffle
from scrapy import Request
from scrapy.http import Response
from scrapy_splash import SplashRequest
from scrapy.utils.log import configure_logging
from tld import get_tld


class UniversityWebCrawler(scrapy.Spider):

    # Core crawler that takes in university faculty/department page and searches for corresponding sub-branch
    # pages identifiable as department/staff directory for personal profile extraction purpose

    # Initiate name for calling and department links for subsequent shuffling and yielding of request
    name = 'university'

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
        'DEPTH_LIMIT': 3,
        # Take a depth-first search algorithm by setting a negative priority
        'DEPTH_PRIORITY': -1,
        'DEPTH_STATS_VERBOSE': True
    }

    def __init__(self, *args, **kwargs):
        # Initiate spider object and shuffling urls to start crawling
        super(UniversityWebCrawler, self).__init__(*args, **kwargs)

        # Log basic information
        configure_logging(install_root_handler=False)
        logging.basicConfig(
            filename='university_spider_info.log',
            filemode='a',
            format='%(levelname)s: %(message)s',
            level=logging.DEBUG
        )
        logging.basicConfig(
            filename='university_spider_error.log',
            filemode='a',
            format='%(levelname)s: %(message)s',
            level=logging.ERROR
        )

        self.shuffled_index = list(UniversityWebCrawler.department_data_index)
        shuffle(self.shuffled_index)

    def start_requests(self):
        # Use normal request by default for department homepage for faster rendering speed
        # Use splash request by default otherwise for possible AJAX-inclusive pages
        for individual_index in self.department_data_index[560:561]:
            individual_data = UniversityWebCrawler.department_data.loc[individual_index]
            university_name = individual_data['school_name']
            url_link = individual_data['url']
            link_title = individual_data['title']

            # Yield request corresponding with preliminary meta data including university name and page title
            request = Request(url_link, callback=self.parse_page_menu)
            request.meta['University Name'] = university_name
            request.meta['Url Link Passed'] = url_link
            request.meta['Link Title Passed'] = link_title
            request.meta['Previous Link'] = ''
            request.meta['depth'] = 0
            yield request

    def parse_page_menu(self, response):
        # Parse the zero-depth page content and update the department link csv file with the correct
        # page title name, assumed to reflect the real content of the specific page
        # Fetch meta data
        university_name = response.meta['University Name']
        url_link_passed = response.meta['Url Link Passed']
        link_title_passed = response.meta['Link Title Passed']
        visiting_depth = response.meta['depth']
        previous_link = response.meta['Previous Link']

        # Report 404 Error
        if response.status == 404:
            self.logger.error('404 Visiting Error for link %s (%s, %s)' % (response.url, 
                                                                           link_title_passed, 
                                                                           university_name))
            return
        
        # Report redirecting information
        if response.url != url_link_passed:
            self.logger.info('Redirecting from %s to %s for %s, %s' % (url_link_passed, response.url,
                                                                       link_title_passed, university_name))

        # Start core content analysis including menu and navigation content parsing
        target_content = UniversityWebCrawler.SIMILARITY_NAVIGATOR.get_target_content(response)

        self.logger.info("Visiting %s at depth %s with target content %s" % (response.url,
                                                                             visiting_depth,
                                                                             str(target_content)))

        # Get header content for next-level benchmarking
        header_content = get_header(response)

        # Iterate through each target content and get relevant attribute to store in meta data and
        # pass to next level of parsing
        for target in target_content:
            target_content_dict = {
                'Title': target[0], 'Target Url': target[1], 'Tag': target[3]
            }
            next_level_meta = {
                'University Name': university_name, 'depth': visiting_depth + 1,
                'Url Link Passed': response.urljoin(target_content_dict['Target Url']),
                'Link Title Passed': ' - '.join([link_title_passed, target_content_dict['Title']]),

                'Header Content': header_content,
                # 'Menu Content': get_menu(response),
                'Main Content': get_main_content(response),

                'Specific Tag': target_content_dict['Tag'],
                'Previous Link': response.url
            }
            # Use splash-request by default
            request = SplashRequest(response.urljoin(target_content_dict['Target Url']),
                                    self.parse, meta=next_level_meta)
            yield request

    def parse(self, response, header_similarity=0.7, entity_threshold=3):
        # Main parse function to parse pages with depth greater than zero
        # Fetch meta data
        pre_meta = response.meta
        splash_meta = pre_meta['splash']

        self.logger.info('Visiting %s at depth %d' % (splash_meta['args']['url'], pre_meta['depth']))

        # Compare header data to the previous page to determine page similarity
        header_content = get_header(response)
        header_content_joined = ' '.join(header_content.keys())
        pre_header_content_joined = ' '.join(pre_meta['Header Content'].keys())
        if UniversityWebCrawler.SIMILARITY_NAVIGATOR.get_similarity(first_string=header_content_joined,
                                                                    second_string=pre_header_content_joined)[0] \
                < header_similarity:

            self.logger.info('Falling back to normal request for %s' % splash_meta['args']['url'])

            # Fallback to previous parse with normal request
            request = Request(splash_meta['args']['url'], callback=self.parse_page_menu)
            request.meta['University Name'] = pre_meta['University Name']
            request.meta['Url Link Passed'] = splash_meta['args']['url']
            request.meta['Link Title Passed'] = ' - '.join(pre_meta['Link Title Passed'].split(' - ')[:-1])

            pre_meta['depth'] -= 1
            request.meta['depth'] = pre_meta['depth']
            request.meta['Previous Link'] = pre_meta['Previous Link']
            yield request
            return

        # Get previous menu content and check redundancy
        main_content = get_main_content_excluding_menu(response)
        previous_content_link = set(pre_meta['Main Content'].values())
        current_menu = {text: link for text, link in get_menu(response).items() if link not in previous_content_link}
        main_content.update(current_menu)

        # Identify named entity to determine the type of information of the current page
        entity_list_org = [(key, value) for key, value in main_content.items()
                           if (len(UniversityWebCrawler.SIMILARITY_NAVIGATOR.model_en(key).ents) != 0) &
                           ('ORG' in [element.label_ for element
                                      in UniversityWebCrawler.SIMILARITY_NAVIGATOR.model_en(key).ents])]
        entity_list_person = [(key, value) for key, value in main_content.items()
                              if (len(UniversityWebCrawler.SIMILARITY_NAVIGATOR.model_en(key).ents) != 0) &
                              ('PERSON' in [element.label_ for element
                                            in UniversityWebCrawler.SIMILARITY_NAVIGATOR.model_en(key).ents])]

        # Stop yielding if current page does not meet the criteria
        if pre_meta['Specific Tag'] == 'DEPARTMENT':
            if len(entity_list_org) <= entity_threshold:
                return
        if pre_meta['Specific Tag'] == 'PEOPLE':
            if len(entity_list_person) <= entity_threshold:
                return

        # Yield following splash request
        for page_title, page_link in main_content.items():
            next_level_meta = {
                'University Name': pre_meta['University Name'],
                'depth': pre_meta['depth'] + 1,
                'Url Link Passed': Response(splash_meta['args']['url']).urljoin(page_link),
                'Link Title Passed': ' - '.join([pre_meta['Link Title Passed'], page_title]),

                'Header Content': header_content,
                'Main Content': main_content,

                'Specific Tag': pre_meta['Specific Tag'],
                'Previous Link': splash_meta['args']['url'],

                'Entity List': entity_list_person if pre_meta['Specific Tag'] == 'PEOPLE' else entity_list_org
            }
            request = SplashRequest(Response(splash_meta['args']['url']).urljoin(page_link),
                                    self.parse, meta=next_level_meta)
            yield request
