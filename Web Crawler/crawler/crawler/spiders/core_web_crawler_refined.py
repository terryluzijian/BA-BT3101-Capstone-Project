import os
import pandas
import re
import scrapy
from crawler.similarity_navigator import SimilarityNavigator
from crawler.xpath_generic_extractor import check_word_filter, get_title_h1_h2_h3, get_main_content, \
                                            get_main_content_text, get_main_content_excluding_menu, \
                                            get_header, get_menu, get_main_content_unique, generic_get_unique_content
from difflib import SequenceMatcher
from lxml import html
from random import shuffle
from scrapy import Request
from scrapy.linkextractors import LinkExtractor, IGNORED_EXTENSIONS
from scrapy.utils.url import parse_url, url_has_any_extension
from tld import get_tld


class UniversityWebCrawlerRefined(scrapy.Spider):

    # Core crawler that takes in university faculty/department page and searches for corresponding sub-branch
    # pages identifiable as department/staff directory for personal profile extraction purpose

    # Initiate name for calling and department links for subsequent shuffling and yielding of request
    name = 'refined'
    custom_settings = {
        # Override custom settings preset by scrapy
        # Take a depth-first search algorithm by setting a negative priority or positive otherwise
        'DEPTH_LIMIT': 4,
        'DEPTH_PRIORITY': -3,
        'DEPTH_STATS_VERBOSE': True,

        # Limit the concurrent request per domain and moderate the server load
        'CONCURRENT_REQUESTS': 256,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 8,
        'DOWNLOAD_DELAY': 2,
    }

    # Set it to be true for debugging
    PRINT_VERBOSE = True

    # Set it to be true for crawling single specified department
    TESTING = True

    # Set it to be true to enable broad crawling
    GENERIC = False

    # Get department link from the file name stored as a class attribute using pandas
    # Shuffle the link for a random start to avoid over-frequent crawling
    # Get prioritized department list specified by user
    DEPARTMENT_DATA_FILE_NAME = 'DEPARTMENT_FACULTY.csv'
    DEPARTMENT_DATA_PRIORITIZED = 'DEPARTMENT_FACULTY_PRIORITIZED.csv'
    data_file_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..')) + '/data/'
    department_data = pandas.read_csv(data_file_path + DEPARTMENT_DATA_FILE_NAME)
    department_data_index = department_data.index
    department_data_prioritized = pandas.read_csv(data_file_path + DEPARTMENT_DATA_PRIORITIZED)

    # Get top domain by iterating through the department links to avoid going beyond the allowed domain
    # Do minor change in accordance with custom variables
    allowed_domains_pre = set()
    if GENERIC:
        department_url = department_data['url']
    else:
        department_url = department_data_prioritized['url']
    for url in department_url:
        allowed_domains_pre.add(get_tld(url))
    allowed_domains = list(allowed_domains_pre)

    # Import and initiate similarity navigator
    SIMILARITY_NAVIGATOR = SimilarityNavigator()

    # Filter word for extracting link and identifying entity
    LINK_FILTER_KEYWORD_STRING_WISE = frozenset(['publication', 'publications', 'paper', 'search', 'news', 'event',
                                                 'events', 'calendar', 'map', 'student', 'lab'])
    LINK_FILTER_KEYWORD_CHAR_WISE = frozenset(['login', 'logout', 'publication', 'news', 'wiki', 'lab', 'resource',
                                               'event', 'calendar', 'map', 'article', 'blog', 'student'])
    ENTITY_FILTER_KEYWORD = frozenset(['library', 'university', 'department', 'faculty',
                                       'college', 'staff', 'student', 'lab'])

    # Create a class attribute to store corresponding patterns that match a profile page under certain
    # department site
    # Only crawling links that match certain patterns after reaching the threshold
    POSSIBLE_PROFILE_PAGE = {}
    PROFILE_THRESHOLD = 10

    # Testing params
    test_link = 'https://chemistry.uchicago.edu'
    test_tag = 'department'
    test_title = 'Department of Geography'
    allowed_domains.append(get_tld(test_link))

    def __init__(self, *args, **kwargs):
        # Initiate spider object and shuffling urls to start crawling
        super(UniversityWebCrawlerRefined, self).__init__(*args, **kwargs)

        # TODO: Log basic information

        # Initiate iframe link extractor
        self.iframe_extractor = LinkExtractor(allow=self.allowed_domains, tags=['iframe'], attrs=['src'],
                                              unique=False, canonicalize=False)

        # Get the index of the data frame for shuffling, available only for broad crawling
        self.shuffled_index = list(UniversityWebCrawlerRefined.department_data_index)
        if UniversityWebCrawlerRefined.GENERIC:
            shuffle(self.shuffled_index)

        # Get file extensions to deny and read the profile threshold from class attribute
        self.denied_extension = list(map(lambda x: '.%s' % x, IGNORED_EXTENSIONS))
        self.profile_threshold = UniversityWebCrawlerRefined.PROFILE_THRESHOLD

    def start_requests(self):
        # Use normal request by default for department homepage for faster loading speed
        # Use splash request by default otherwise for possible AJAX-inclusive pages with rendering
        # For testing:
        if UniversityWebCrawlerRefined.TESTING:
            test_link = UniversityWebCrawlerRefined.test_link
            request = Request(test_link, callback=self.parse_menu, errback=self.errback_report)
            request.meta['depth'] = 0
            request.meta['Link'] = test_link
            request.meta['Title'] = UniversityWebCrawlerRefined.test_title
            request.meta['University Name'] = 'Test School'
            request.meta['Original Start'] = (test_link, UniversityWebCrawlerRefined.test_title)
            request.meta['Previous Link'] = ''
            request.meta['Is Department'] = UniversityWebCrawlerRefined.test_tag
            yield request

        else:
            for individual_index in self.shuffled_index:
                # Get data from the specific row
                individual_data = UniversityWebCrawlerRefined.department_data.loc[individual_index]
                university_name = individual_data['school_name']
                url_link = individual_data['url']
                link_title = individual_data['title']
                department_or_faculty = individual_data['department_or_faculty']

                # Yield request corresponding with preliminary meta data including university name and page title
                request = Request(url_link, callback=self.parse_menu, errback=self.errback_report)

                # Link-related data
                request.meta['Link'] = url_link  # Link read as input
                request.meta['Title'] = link_title  # Title read as input
                request.meta['depth'] = 0

                # Some basic record-based data
                request.meta['University Name'] = university_name
                request.meta['Original Start'] = (url_link, link_title)
                request.meta['Previous Link'] = ''
                request.meta['Is Department'] = department_or_faculty
                yield request

    def parse_menu(self, response):
        # Parse menu content of the current page to locate department or people components
        # Stop crawling if encountered 404 error and report
        if self.report_basic_information(response, response.meta)['404']:
            return

        # Start core content analysis including menu and navigation content parsing
        current_response = response
        if (not response.meta.get('From parse_department', False)) & (not response.meta.get('Fall Back', False)) &\
           (not response.meta['Is Department'] == 'department'):
            # Get general target content if the response does not have the above meta data
            target_content = UniversityWebCrawlerRefined.SIMILARITY_NAVIGATOR.get_target_content(response)
        else:
            # Parse only faculty-related content otherwise with a higher threshold
            target_content = UniversityWebCrawlerRefined.SIMILARITY_NAVIGATOR.get_target_content(response,
                                                                                                 parse_only_people=True,
                                                                                                 threshold=0.85)

        if UniversityWebCrawlerRefined.PRINT_VERBOSE:
            # Print current callback-level information
            self.logger.info('Requesting %s (%s) at depth %s with content %s at parse_menu from %s'
                             % (response.url, response.meta['Is Department'], response.meta['depth'], target_content,
                                response.meta['Previous Link']))

        # Iterate through each target content and get relevant attribute to store in meta data and
        # pass to next level of parsing
        for target in target_content:
            # Target content follows (title, link, similarity, tag)
            target_title = target[0]
            target_link = target[1]
            target_tag = target[3]

            # Yield request to new meta data with normal request for department tagged link
            # and splash request for link tagged with people
            target_meta = {
                # Link-related data
                'Link': target_link,
                'Title': target_title,
                'depth': response.meta['depth'] + 1,
                'Past Response': [current_response],

                # Some basic record-based data
                'University Name': response.meta['University Name'],
                'Original Start': response.meta['Original Start'],
                'Previous Link': response.url,
                'Fall Back': response.meta.get('Fall Back', False),
                'Is Department': response.meta['Is Department'],
            }

            # Yield request
            if target_tag == 'PEOPLE':
                # If the response is originated from parse_department, change the title
                if response.meta.get('From parse_department', False):
                    title = response.xpath('//title/text()').extract_first()
                else:
                    title = None

                target_meta['Original Start'] = (response.url, title if title is not None else response.meta['Title'])
                if target_meta['Original Start'] not in UniversityWebCrawlerRefined.POSSIBLE_PROFILE_PAGE.keys():
                    # Start recording the profile page dictionary by setting original start (key pair) as key
                    UniversityWebCrawlerRefined.POSSIBLE_PROFILE_PAGE[target_meta['Original Start']] = {}
                    UniversityWebCrawlerRefined.POSSIBLE_PROFILE_PAGE[target_meta['Original Start']]['Total'] = 0
                    UniversityWebCrawlerRefined.POSSIBLE_PROFILE_PAGE[target_meta['Original Start']]['Pattern'] = []

                target_request = Request(target_link,
                                         callback=self.parse_people, meta=target_meta, errback=self.errback_report)
                yield target_request
            elif target_tag == 'DEPARTMENT':
                target_request = Request(target_link,
                                         callback=self.parse_department, meta=target_meta, errback=self.errback_report)
                yield target_request

    def parse_department(self, response):
        # Parse main content of the current page recursively
        basics = self.report_basic_information(response, response.meta)
        if basics['404']:
            return

        # Parse current url
        current_parsed = self.get_netloc_and_path_level(response.url)

        # If the redirected is presented with an actually shorter path (e.g. /xxx/xxx/xxx -> /yyy)
        if basics['Redirected']:
            if self.link_contain_keyword(response.url):
                # Stop fall back if the url contains keywords to be filtered
                return

            # Compare with previous url with its parsed data
            previous_parsed = self.get_netloc_and_path_level(response.meta['Previous Link'])
            if (current_parsed[2] < previous_parsed[2]) | (current_parsed[0] != previous_parsed[0]):
                # If the link is actually redirected and might lead to a new sub-domain or new path
                if UniversityWebCrawlerRefined.PRINT_VERBOSE:
                    # Print redirection information
                    self.logger.info('Going back to parse_menu from parse_department for %s at depth %s' %
                                     (response.url, response.meta['depth']))

                # Re-initiate metadata
                response.meta['depth'] = -1
                response.meta['Previous Link'] = response.url
                response.meta['From parse_department'] = True
                response.meta['Is Department'] = 'department'
                target_request = Request(response.url, self.parse_menu, meta=response.meta, errback=self.errback_report)

                # IMPORTANT: tell the crawler to pass the link again and do not filter it
                target_request.dont_filter = True
                yield target_request
                return

        if UniversityWebCrawlerRefined.PRINT_VERBOSE:
            # Print current callback-level information
            self.logger.info('Requesting %s at depth %s at parse_people from %s' %
                             (response.url, response.meta['depth'], response.meta['Previous Link']))

        # Start core content analysis including main content parsing
        current_response = response
        current_depth = response.meta.copy()['depth']
        current_response_parsed_dict = {
            name: element
            for name, element in zip(['Title', 'Link', 'Netloc', 'Path', 'Path Level'],
                                     [response.meta['Title'], response.url] + list(current_parsed))
        }

        # Get current unique content (fallback to get_general when nothing returned)
        main_content = get_main_content_unique(response, response.meta['Past Response'])
        main_content_parsed = [[text, link] + list(self.get_netloc_and_path_level(link))
                               for text, link in main_content.items()]

        # Compare page-level data including domain name and path level
        for content_item in main_content_parsed:
            # Each item follows (title, link, netloc, path element, path level)
            content_title = content_item[0]
            content_link = content_item[1]
            content_netloc = content_item[2]
            content_path_pair = (content_item[3], content_item[4])

            # Filter link here similar to the redirection occasion, with denying on file extension as well
            if url_has_any_extension(content_link, self.denied_extension) | self.link_contain_keyword(content_link):
                continue

            # Update meta data
            content_meta = {
                # Link-related data
                'Link': content_link,
                'Title': content_title,
                'depth': current_depth + 1,

                # Some basic record-based data
                'University Name': response.meta['University Name'],
                'Original Start': response.meta['Original Start'],
                'Previous Link': response.url,
                'Is Department': response.meta['Is Department'],
            }

            # Yield request
            if (current_response_parsed_dict['Netloc'] != content_netloc) | \
               (content_path_pair[1] < current_response_parsed_dict['Path Level']):
                # If encountered a href with lower path level or different netloc (e.g. a sub-domain)
                # Yield back to page menu parsing at parse_menu
                # Set depth back to 0 (-1 for current level)
                response.meta['depth'] = -1
                content_meta['depth'] = response.meta['depth']
                content_meta['From parse_department'] = True
                content_meta['Is Department'] = 'department'
                target_request = Request(content_link, self.parse_menu, meta=content_meta, errback=self.errback_report)
                yield target_request
            else:
                # Recursively yield request at parse_department level otherwise until reaching certain depth
                content_meta['Past Response'] = response.meta['Past Response'] + [current_response]
                response.meta['depth'] = current_depth
                content_meta['depth'] = response.meta['depth']
                target_request = Request(content_link, self.parse_department, meta=content_meta,
                                         errback=self.errback_report)
                yield target_request

    def parse_people(self, response):
        # Parse main content of the current page recursively
        basics = self.report_basic_information(response, response.meta)
        if basics['404']:
            # Stop parsing when encountering 404 error
            return

        # If redirection contains keyword to filter, stop parsing
        if basics['Redirected']:
            if self.link_contain_keyword(response.url):
                return

        # If current page is an iframe (e.g. NUS Department of Japanese Studies)
        for iframe_link in self.iframe_extractor.extract_links(response):
            # Get iframe link and reset depth, with callback at the same level (parse_people)
            response.meta['depth'] -= 1
            new_meta = response.meta.copy()
            new_meta['iframe'] = True
            new_meta['depth'] = response.meta['depth']
            yield Request(response.urljoin(iframe_link.url), self.parse_people, meta=new_meta,
                          errback=self.errback_report)

        # Core part of parse_people with call on processing named entity for the response
        current_response = response
        current_depth = response.meta.copy()['depth']

        # Get the dictionary assumed to have been created at parse_menu level
        profile_dict = UniversityWebCrawlerRefined.POSSIBLE_PROFILE_PAGE[response.meta['Original Start']]
        is_personal = self.process_possible_named_entity(response)

        if current_depth > 1:
            # Only process when reaching depth deeper than 1
            # Compare the title with the previous, extract using xpath
            current = response.xpath('//title/text()').extract_first()
            previous = response.meta['Past Response'][-1].xpath('//title/text()').extract_first()

            # Get unique title for current page subsequently and handle None object
            current = '' if current is None else current
            previous = response.meta['Title'] if previous is None else previous
            match = SequenceMatcher(None, current, previous).find_longest_match(0, len(current), 0, len(previous))
            current_unique = re.sub(pattern=r'[^ \.A-Za-z\-]', repl=' ',
                                    string=current.replace(current[match.a: match.a + match.size], ''))

            # Combine all the conditions:
            # If the original link title has person name
            # If the current page contains certain person element (publication, research interest and biography)
            # If the current unique title has person name
            if (len(self.parse_entity(response.meta['Title'], including_org=False)) >= 1) | is_personal |\
               (len(self.parse_entity(current_unique, including_org=False)) >= 1):
                # Proceed with updating profile dictionary
                profile_dict['Total'] += 1

                # Generalize personal profile pattern here
                if profile_dict['Total'] < self.profile_threshold:
                    profile_dict['Pattern'].append(response.url)

                    if UniversityWebCrawlerRefined.PRINT_VERBOSE:
                        # Log information
                        self.logger.info('FOUND 1 ITEM at %s (depth: %s, item: %s, start: %s)' %
                                         (response.url, current_depth,
                                          profile_dict['Total'], response.meta['Original Start']))

                    # Process the profile item
                    self.process_profile_item(response, current_unique)
                else:
                    # Compile patterns when reaching the threshold
                    if profile_dict['Total'] == self.profile_threshold:
                        profile_dict['Compiled'] = self.compile_pattern(profile_dict['Pattern'], response)

                    # If the number of items exceeds threshold, only yield item when the link matches the pattern
                    if self.match_pattern(profile_dict['Compiled'], response.url):
                        if UniversityWebCrawlerRefined.PRINT_VERBOSE:
                            # Log information
                            self.logger.info('FOUND 1 ITEM at %s (depth: %s, item: %s, start: %s)' %
                                             (response.url, current_depth, profile_dict['Total'],
                                              response.meta['Original Start']))

                        # Process the profile item
                        self.process_profile_item(response, current_unique)
                return

        # If path differs, go back to parse_menu (assumed to happen at depth <= 1 and hence skip the conditioning above)
        current_path = self.get_netloc_and_path_level(response.url)[1]
        previous_path = self.get_netloc_and_path_level(response.meta['Past Response'][-1].url)[1]
        if self.is_direct_to_different_path(current_path, previous_path):
            if not response.meta.get('Fall Back', False):
                # Allow only one-time fall-back
                response.meta['depth'] = -1
                new_meta = response.meta.copy()
                new_meta['Fall Back'] = True
                new_meta['depth'] = response.meta['depth']
                yield Request(response.url, self.parse_menu, meta=new_meta, dont_filter=True,
                              errback=self.errback_report)
                return

        if UniversityWebCrawlerRefined.PRINT_VERBOSE:
            # Print current callback-level information
            self.logger.info('Requesting %s at depth %s at parse_people from %s' %
                             (response.url, response.meta['depth'], response.meta['Previous Link']))

        # Parse unique main content of current page
        # Recursively yield request at current call back
        # Get current response metadata
        # Get unique content of current response (fallback to general when empty)
        main_content = get_main_content_unique(response, response.meta['Past Response'])
        if response.meta.get('XML', False):
            to_parse = html.fromstring(response.body)
            link_href = to_parse.xpath('//a[@href]/@href')
            main_content = {key: value for key, value in zip(['Link %d' % index for index in range(len(link_href))],
                                                             link_href)}

        # If main content still contains zero components, it is probably a AJAX-enabled page
        # Simulate a XML document call by heuristics
        if len(main_content) == 0:
            response.meta['depth'] -= 1
            new_meta = response.meta.copy()
            new_meta['depth'] = response.meta['depth']
            new_meta['XML'] = True
            yield Request(response.urljoin(response.url.replace('html', 'xml')), self.parse_people, meta=new_meta,
                          errback=self.errback_report)

        # Iterate through each component
        for content_text, content_href in main_content.items():
            if url_has_any_extension(content_href, self.denied_extension) | self.link_contain_keyword(content_href):
                # Filter link first
                continue

            # Similar to the item yielding above, check whether link matches the pattern
            if profile_dict['Total'] >= self.profile_threshold:
                if 'Compiled' in profile_dict.keys():
                    if not self.match_pattern(profile_dict['Compiled'], content_href):
                        continue

            # Update content metadata
            content_meta = {
                # Link-related data
                'Link': content_href,
                'Title': content_text,
                'depth': current_depth + 1,
                'Past Response': response.meta['Past Response'] + [current_response],

                # Some basic record-based data
                'University Name': response.meta['University Name'],
                'Original Start': response.meta['Original Start'],
                'Previous Link': response.url,
                'Is Department': response.meta['Is Department'],
            }

            # If the text contains person name, it is highly possible it directs to a
            # personal profile page
            content_request = Request(content_href, self.parse_people, meta=content_meta, errback=self.errback_report)
            yield content_request

    def process_profile_item(self, response, unique_title, len_lim=10):
        # Key function to parse profile items

        # ----------------------------------NAME COMPONENT CODE----------------------------------
        # Parse title first to find person's name
        name = 'Unknown'
        if len(self.parse_entity(response.meta['Title'])) >= 1:
            # If entity is identified in the metadata title
            name = response.meta['Title']
        elif len(self.parse_entity(unique_title)) >= 1:
            # If entity is identified in the current unique title
            name = unique_title
        else:
            # Identify named entity in h1, h2 and h3 text
            h1_h2_h3 = [element for key, value in get_title_h1_h2_h3(response).items()
                        for element in value if key in ['h1', 'h2', 'h3']]
            h1_h2_h3_parsed = list(filter(lambda y: len(y) > 0, map(lambda x: self.parse_entity(x), h1_h2_h3)))
            h1_h2_h3_entity = [name for name_list in list(map(lambda x: list(x.keys()), h1_h2_h3_parsed))
                               for name in name_list]

            # Select first occurrence
            name = name if len(h1_h2_h3_entity) <= 0 else h1_h2_h3_entity[0]

        # ----------------------------------POSITION/APPOINTMENT COMPONENT CODE----------------------------------
        # Parse and mine main text, find professor first
        position = 'Non-Professor'
        main_text = generic_get_unique_content(response, response.meta['Past Response'][-1], get_text=True)

        # For the simplest case, find position title in name
        if response.meta['Title'].lower().startswith('prof'):
            position = 'Professor'
        elif response.meta['Title'].lower().startswith('assistant') | response.meta['Title'].lower().startswith('asst'):
            position = 'Assistant Professor'
        elif response.meta['Title'].lower().startswith('associate') | response.meta['Title'].lower().startswith('asso'):
            position = 'Associate Professor'
        else:
            # Find in longer main text otherwise, start from shorter length and expand subsequently
            assoc_prof_re = r'(associate|(assoc\.?)) prof[\.]?[ ]?'
            assist_prof_re = r'(assistant|(asst\.?)) prof[\.]?[ ]?'
            prof_re = r'prof\.|professor'
            if len(list(filter(lambda x: re.search(assoc_prof_re,
                                                   x.lower()) and len(x.split(' ')) <= len_lim, main_text))) >= 1:
                position = 'Associate Professor'
            if len(list(filter(lambda x: re.search(assist_prof_re,
                                                   x.lower()) and len(x.split(' ')) <= len_lim, main_text))) >= 1:
                position = 'Assistant Professor'
            if len(list(filter(lambda x: 'professor' in x.lower() and len(x.split(' ')) <= 1, main_text))) >= 1:
                position = 'Professor'
            else:
                not_assist = list(filter(lambda x: not re.search(assist_prof_re, x.lower()), main_text))
                not_assoc = list(filter(lambda x: not re.search(assoc_prof_re, x.lower()), main_text))
                remain = set([element for element in (not_assoc + not_assoc)
                              if element in not_assist and element in not_assoc])
                if len(list(filter(lambda x: re.search(prof_re,
                                                       x.lower()) and len(x.split(' ')) <= len_lim, remain))) >= 1:
                    position = 'Professor'

        if position != 'Non-Professor':
            print('%s (%s)' % (name, position))

    def match_pattern(self, pattern_dict, response_url):
        # Help function to tell whether certain link matches the pattern made
        def path_contain_path(url_parsed, path_list):
            # Run through every path, inclusive or not of the url path to be parsed
            path_to_search = '/'.join(url_parsed[1][:-1])
            for path in path_list:
                if path in path_to_search:
                    return True
            return False

        parsed = self.get_netloc_and_path_level(response_url)
        if parsed[0] not in pattern_dict['Netloc']:
            return False
        if not path_contain_path(parsed, pattern_dict['Path']):
            return False
        return True

    def compile_pattern(self, pattern_list, response, top_n=2):
        # Add the parent link also as it may also implies viable link
        pattern_list_parsed = list(map(lambda link: self.get_netloc_and_path_level(link),
                                       pattern_list + [response.meta['Previous Link']]))

        # Only get the top n netloc for simplicity and accuracy
        netloc_list = [element[0] for element in pattern_list_parsed]
        netloc_sort = sorted(set(netloc_list), key=lambda x: netloc_list.count(x), reverse=True)
        compiled = {
            'Netloc': list(netloc_sort)[:top_n],
            'Path': set(filter(lambda x: x != '', ['/'.join(element[1][:-1]) for element in pattern_list_parsed]))
        }

        # Log pattern information
        self.logger.info('Found pattern %s for %s' % (list(compiled.values()), response.meta['Original Start']))
        return compiled

    def errback_report(self, failure):
        # Log all failures
        try:
            self.logger.info('Minor error occurs at %s / %s' % (failure.request, failure.value.response))
        except AttributeError:
            self.logger.info('Minor error occurs, which might be Twisted Connection Error')

    def report_basic_information(self, response, response_meta):
        # Report redirecting information
        is_redirected = (response.url != response_meta['Link'])
        is_404 = (response.status == 404)
        if is_redirected:
            if UniversityWebCrawlerRefined.PRINT_VERBOSE:
                self.logger.info('Redirecting from %s to %s for %s, %s' % (response_meta['Link'], response.url,
                                                                           response_meta['Title'],
                                                                           response_meta['University Name']))

        # Report 404 Error
        if is_404:
            if UniversityWebCrawlerRefined.PRINT_VERBOSE:
                self.logger.info('404 Visiting Error for link %s (%s, %s)' % (response.url, response_meta['Title'],
                                                                              response_meta['University Name']))

        # Return back 404 Error
        return {
            '404': is_404,
            'Redirected': is_redirected
        }

    def parse(self, response):
        # Compulsory override but skipped for this class
        pass

    @staticmethod
    def link_contain_keyword(link):
        # Get all word token in link
        tokens = re.findall(r'[A-Za-z]+', link)
        for token in tokens:
            if token.lower() in UniversityWebCrawlerRefined.LINK_FILTER_KEYWORD_STRING_WISE:
                return True
        for word in UniversityWebCrawlerRefined.LINK_FILTER_KEYWORD_CHAR_WISE:
            if word in link.lower():
                return True
        return False

    @staticmethod
    def process_possible_named_entity(response):
        # Text-wise comparison
        text_content = generic_get_unique_content(response, response.meta['Past Response'], get_text=True)
        have_publication = list(filter(lambda x: ('publicati' in x.lower()) & (len(x.split(' ')) <= 5), text_content))
        have_research_interest = list(filter(lambda x: ('interest' in x.lower()) & (len(x.split(' ')) <= 5),
                                             text_content))
        have_bio = list(filter(lambda x: 'biograp' in x.lower() and len(x.split(' ')) <= 5, text_content))
        return (len(have_publication) > 0) | (len(have_research_interest) > 0) | (len(have_bio) > 0)

    @staticmethod
    def is_direct_to_different_path(current_path, previous_path):
        path_max_iter = min([len(current_path), len(previous_path)])
        for iteration in range(path_max_iter):
            if current_path[iteration] != current_path[iteration]:
                return True

    @staticmethod
    def get_netloc_and_path_level(target_url):
        parsed = parse_url(target_url)
        path = list(filter(lambda x: len(x) > 0, parsed.path.split('/')))
        return parsed.netloc, path, len(path)

    @staticmethod
    def parse_entity(target_string, including_org=True):

        def normalize_string_space(string_before):
            return ' '.join(string_before.split())

        def contain_filter_word(test_string, word_list):
            for word in word_list:
                if word in test_string.lower():
                    return True
            return False

        parsed = UniversityWebCrawlerRefined.SIMILARITY_NAVIGATOR.model_en(target_string).ents

        # Include ORG here as spacy is not capable of identifying all kinds of names
        if including_org:
            entity_dict = {normalize_string_space(entity.string): entity.label_ for entity in parsed
                           if entity.label_ in ['PERSON', 'ORG']}
        else:
            entity_dict = {normalize_string_space(entity.string): entity.label_ for entity in parsed
                           if entity.label_ in ['PERSON']}
            entity_dict = {key: value for key, value in entity_dict.items()
                           if not contain_filter_word(key, UniversityWebCrawlerRefined.ENTITY_FILTER_KEYWORD)}
        return entity_dict
