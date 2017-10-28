import os
import pandas
import re
import scrapy
from crawler.items import DirectoryProfilePageItem
from crawler.similarity_navigator import SimilarityNavigator
from crawler.xpath_generic_extractor import check_word_filter, MENU_TEXT_FILTER, get_title_h1_h2, get_main_content, \
                                            get_main_content_text, get_main_content_excluding_menu, \
                                            get_header, get_menu
from random import shuffle
from scrapy import Request
from scrapy.http import Response
from scrapy_splash import SplashRequest
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
        # Take a depth-first search algorithm by setting a negative priority or positive otherwise
        'DEPTH_LIMIT': 4,
        'DEPTH_PRIORITY': -1,
        'DEPTH_STATS_VERBOSE': True,

        'CONCURRENT_REQUESTS': 32,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 2,
        'DOWNLOAD_DELAY': 1,
    }

    def __init__(self, *args, **kwargs):
        # Initiate spider object and shuffling urls to start crawling
        super(UniversityWebCrawler, self).__init__(*args, **kwargs)

        # TODO: Log basic information

        self.shuffled_index = list(UniversityWebCrawler.department_data_index)
        shuffle(self.shuffled_index)

    def start_requests(self):
        # Use normal request by default for department homepage for faster rendering speed
        # Use splash request by default otherwise for possible AJAX-inclusive pages
        for individual_index in self.shuffled_index:
            individual_data = UniversityWebCrawler.department_data.loc[individual_index]
            university_name = individual_data['school_name']
            url_link = individual_data['url']
            link_title = individual_data['title']

            # Yield request corresponding with preliminary meta data including university name and page title
            request = Request(url_link, callback=self.parse_page_menu)
            request.meta['Start from Url'] = url_link
            request.meta['University Name'] = university_name
            request.meta['Url Link Passed'] = url_link  # Link read as input
            request.meta['Link Title Passed'] = link_title  # Title read as input
            request.meta['Previous Link'] = ''  # Previous link
            request.meta['depth'] = 0
            yield request

    def parse_page_menu(self, response,
                        top_content=5):
        # Parse the zero-depth page content and update the department link csv file with the correct
        # page title name, assumed to reflect the real content of the specific page
        # Fetch meta data
        university_name = response.meta['University Name']
        url_link_passed = response.meta['Url Link Passed']
        link_title_passed = response.meta['Link Title Passed']
        visiting_depth = response.meta['depth']

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
        target_content = UniversityWebCrawler.SIMILARITY_NAVIGATOR.get_target_content(response)[:top_content]
        fall_back = response.meta.get('Fall Back', False)
        if fall_back:
            # Only visit those links with high possibility of being personal page
            target_content = UniversityWebCrawler.SIMILARITY_NAVIGATOR.get_target_content(response,
                                                                                          threshold=0.8)[:top_content]
            target_content = list(filter(lambda tag: tag[3] != 'DEPARTMENT', target_content))

        self.logger.info("Normally requesting %s at depth %s with target content %s" % (response.url,
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
                'University Name': university_name,
                'depth': visiting_depth + 1,
                'Url Link Passed': response.urljoin(target_content_dict['Target Url']),
                'Link Title Passed': ' - '.join([link_title_passed, target_content_dict['Title']]),

                'Header Content': header_content,  # Header content of current page
                'Main Content': get_main_content(response),  # Main content of current page
                # 'Menu Content': get_menu(response),

                'Specific Tag': target_content_dict['Tag'],  # People page or department page
                'Previous Link': response.url,
                'Fall Back': fall_back,  # Indicator of fall back to normal request

                'dept depth': visiting_depth if target_content_dict['Tag'] != 'DEPARTMENT' else visiting_depth + 1,
                'people depth': visiting_depth if target_content_dict['Tag'] != 'PEOPLE' else visiting_depth + 1,

                'Start from Url': response.meta['Start from Url'] if not fall_back else url_link_passed
            }
            # Use splash-request by default
            request = SplashRequest(response.urljoin(target_content_dict['Target Url']),
                                    self.parse, meta=next_level_meta)
            yield request

    def parse(self, response,
              header_similarity=0.7,
              dept_threshold=3, department_max_step=2,
              print_limit=20):
        # Main parse function to parse pages with depth greater than zero
        def parse_entity(target_string):

            def normalize_string_space(string_before):
                return ' '.join(string_before.split())

            parsed = UniversityWebCrawler.SIMILARITY_NAVIGATOR.model_en(target_string).ents
            entity_dict = {normalize_string_space(entity.string): entity.label_ for entity in parsed
                           if entity.label_ in ['PERSON', 'ORG']}
            return entity_dict

        def valid_person_name(name,
                              filter_word=('library', 'street', 'view', 'profile', 'more'),
                              punctuation='!#$%&\'()*+/:;<=>?@[\\]^_`{|}~'):
            # Check whether a person's name is valid
            name_lower = name.lower()
            for mark in punctuation:
                if mark in name_lower:
                    return False
            return not check_word_filter(name_lower, filter_word)

        # Fetch meta data
        pre_meta = response.meta
        splash_meta = pre_meta['splash']

        self.logger.info('Visiting %s at depth %d from %s (dept: %d, people: %d)' % (splash_meta['args']['url'],
                                                                                     pre_meta['depth'],
                                                                                     pre_meta['Previous Link'],
                                                                                     pre_meta['dept depth'],
                                                                                     pre_meta['people depth']))

        # Compare header data to the previous page to determine page similarity
        header_content = get_header(response)
        header_content_joined = ' '.join(header_content.keys())
        pre_header_content_joined = ' '.join(pre_meta['Header Content'].keys())

        if (UniversityWebCrawler.SIMILARITY_NAVIGATOR.get_similarity(first_string=header_content_joined,
                                                                     second_string=pre_header_content_joined)[0]
                < header_similarity) | ((pre_meta['Specific Tag'] == 'DEPARTMENT') &
                                        (pre_meta['dept depth'] >= department_max_step)):

            # Stop yielding if there are too few department link from the previous metadata
            if pre_meta['Specific Tag'] == 'DEPARTMENT':
                if pre_meta.get('Entity List', False):
                    if len(pre_meta['Entity List']['DEPT']) <= dept_threshold:
                        return

            # Allow only one-time fall back
            if not pre_meta.get('Fall Back', False):

                # Fallback to previous parse with normal request
                request = Request(splash_meta['args']['url'], callback=self.parse_page_menu)
                request.meta['University Name'] = pre_meta['University Name']
                pre_meta['depth'] = 0
                request.meta['depth'] = pre_meta['depth']
                request.meta['Url Link Passed'] = splash_meta['args']['url']
                request.meta['Link Title Passed'] = ' - '.join(pre_meta['Link Title Passed'].split(' - ')[:-1])

                # Set a new meta value to indicate fall back, which is assumed to be directed to the
                # department page and therefore not necessary to check target content with
                # department tag anymore
                request.meta['Previous Link'] = pre_meta['Previous Link']
                request.meta['Fall Back'] = True
                request.meta['Start from Url'] = pre_meta['Start from Url']
                yield request

        # Get previous menu content and check redundancy
        previous_main_content = set(pre_meta['Main Content'].values())
        current_main_content = get_main_content_excluding_menu(response)
        current_menu = {text: link for text, link in get_menu(response).items() if link not in previous_main_content}

        # Get the non-repetitive main content of the current page by updating the dict with the current menu content
        current_main_content.update(current_menu)

        # Identify named entity to determine the type of information of the current page
        entity_parsed = [(parse_entity(key), value) for key, value in combine_duplicate_in_dict(current_main_content)
                         if len(parse_entity(key)) != 0]
        entity_list_org = [(' '.join(key_dict.keys()), value) for key_dict, value in entity_parsed
                           if 'ORG' in key_dict.values()]
        entity_list_person = [(' '.join(filter(lambda x: valid_person_name(x), key_dict.keys())), value)
                              for key_dict, value in entity_parsed if 'PERSON' in key_dict.values()]

        # Detect personal profile page through title, h1 and h2
        h1_h2_title = get_title_h1_h2(response)
        for tag in h1_h2_title:
            parsed_list = list(map(lambda target_string: parse_entity(target_string), h1_h2_title[tag]))
            h1_h2_title[tag] = [entity
                                for parsed_dict in parsed_list
                                for name, entity in parsed_dict.items() if entity == 'PERSON']

        # Locate personal profile page
        if ('PERSON' in h1_h2_title['title']) and (
           ('PERSON' in h1_h2_title['h1'] or 'PERSON' in h1_h2_title['h2'])):
            directory_item = DirectoryProfilePageItem()
            directory_item['profile_url'] = splash_meta['args']['url']
            directory_item['university_name'] = pre_meta['University Name']
            directory_item['page_title'] = response.xpath('//title/text()[normalize-space(.)]').extract_first()
            directory_item['aggregated_title'] = pre_meta['Link Title Passed']
            directory_item['starting_url'] = pre_meta['Start from Url']
            # directory_item['response_body'] = response.body
            directory_item['main_content'] = pre_meta['Main Content']
            directory_item['header_content'] = pre_meta['Header Content']
            directory_item['depth'] = pre_meta['depth']
            directory_item['previous_link'] = pre_meta['Previous Link']
            self.logger.info('Found 1 possible item at %s: %s' % (splash_meta['args']['url'],
                                                                  directory_item['page_title']))
            yield directory_item
            return

        # Yield following splash request
        for page_title, page_link in current_main_content.items():

            if check_word_filter(page_link, MENU_TEXT_FILTER):
                continue

            next_level_meta = {
                'University Name': pre_meta['University Name'],
                'depth': max(pre_meta['dept depth'], pre_meta['people depth']) + 1,
                'Url Link Passed': Response(splash_meta['args']['url']).urljoin(page_link),
                'Link Title Passed': ' - '.join([pre_meta['Link Title Passed'], page_title]),

                'Header Content': header_content,
                'Main Content': current_main_content,

                'Specific Tag': pre_meta['Specific Tag'],
                'Previous Link': splash_meta['args']['url'],

                'Entity List': {'PEOPLE': entity_list_person,
                                'DEPT': entity_list_org},

                'dept depth': pre_meta['depth'] if pre_meta['Specific Tag'] != 'DEPARTMENT' else pre_meta['depth'] + 1,
                'people depth': pre_meta['depth'] if pre_meta['Specific Tag'] != 'PEOPLE' else pre_meta['depth'] + 1,

                'Start from Url': response.meta['Start from Url']
            }

            request = SplashRequest(Response(splash_meta['args']['url']).urljoin(page_link),
                                    self.parse, meta=next_level_meta)
            yield request


def combine_duplicate_in_dict(some_dict):
    # Return a dict item without duplicate text key
    def substitute_dup_component(target_string):
        return re.sub(r'\([1-9]+\)', '', target_string)

    return [(substitute_dup_component(key), value) for key, value in some_dict.items()]
