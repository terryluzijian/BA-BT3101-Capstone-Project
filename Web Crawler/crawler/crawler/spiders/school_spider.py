import re
import scrapy
from crawler.items import SchoolWebPageItem
from datetime import datetime
from scrapy.loader import ItemLoader
from tld import get_tld


class SchoolWebsiteContentSpider(scrapy.Spider):

    name = 'school'
    allowed_domains = []

    # Customized filtering params
    FILTERED_TAG_NAME = ('script', 'style', 'title')
    FILTERED_WORD_NAME = ('library', 'wiki', 'login', 'new', 'lib.')
    FILTERED_FILE_SUFFIX = ('js', 'css', 'img', 'png')

    # Constant
    STRING_LENGTH_LIMIT = 4
    EMAIL_UPPER_BOUND = 3
    EMAIL_LOWER_BOUND = 1

    # Specific word list
    PROFILE_SPECIFIC_WORD = {
        'PERSONAL': ('biography', 'contact', 'email', 'phone number', 'tel', 'office'),
        'POST_FULL': ('professor', 'lecturer', 'associate professor', 'assistant professor', 'doctor'),
        'POST_ABBR': ('prof', 'a/p', 'assoc prof', 'dr'),
        'RESEARCH': ('research interests', 'specialized areas', 'research areas'
                     'publications', 'publication', 'selected publications')
    }
    RANK = sorted({
        'RESEARCH': 1,
        'EMAIL': 2,
        'POST_ABBR': 3,
        'POST_FULL': 4,
        'PERSONAL': 5,
    }.items(), key=lambda x: x[1])

    # Fixed XPath
    TITLE_XPATH = '//title/text()'
    TEXT_XPATH = '//*[not(%s)]/text()[normalize-space(.)][string-length() >= %d]' % \
                 ('|'.join(list(map(lambda x: 'self::%s' % x, FILTERED_TAG_NAME))), STRING_LENGTH_LIMIT)
    LINK_XPATH = '//*/attribute::src|//*/attribute::href'
    PDF_XPATH = '//*/attribute::src[contains(., \'.pdf\')]|//*/attribute::href[contains(., \'.pdf\')]'
    EMAIL_XPATH = '//*/attribute::*[contains(., \'mail\')]'

    # Regex
    EMAIL_REGEX = r'([a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+)'
    DATE_REGEX = r'(20[0-9]{2}).{0,1}([1][0-2]|[0]{0,1}[1-9])(.([1-3][0-9]|[0]{0,1}[1-9])){0,1}'

    # School URL list here
    SCHOOL_WEBSITE_DICT = {
        'National University of Singapore': 'http://www.nus.edu.sg/',
        'Harvard University': 'https://www.harvard.edu/'
    }

    def __init__(self, *args, **kwargs):
        super(SchoolWebsiteContentSpider, self).__init__(*args, **kwargs)
        self.start_urls = list(self.SCHOOL_WEBSITE_DICT.values())

        # Only go through urls that are within the domain (e.g. Facebook page etc. should not be included)
        website_list = SchoolWebsiteListProcessor(self.start_urls)
        self.allowed_domains = website_list.get_top_domain()

        # Inversely look up for the school name and pass it as a value later in the parse callback
        self.inverse_lookup = {website: name for name, website in zip(self.SCHOOL_WEBSITE_DICT.keys(),
                                                                      website_list.get_top_domain())}

    def determine_validity(self, url, response):
        # if the url is date-specific, it is less likely to be a profile page
        if re.search(self.DATE_REGEX, url):
            return False

        # if the url is neither a html document nor a pdf file (CV)
        if (url.split('.')[-1] in self.FILTERED_FILE_SUFFIX) | ('<html>' not in str(response.body)):
            return False

        # if the url contains words in the filtered word list
        for word in self.FILTERED_WORD_NAME:
            if word in url:
                return False

        return True

    def determine_personal_profile_component(self, **kwargs):
        # Help function to locate certain keywords
        def iterate_and_find(text_content_list, word_list, abbreviated=False):
            for element in word_list:
                if not abbreviated:
                    if len(list(filter(lambda x: x == element, text_content_list))) > 0:
                        return True
                else:
                    if len(list(filter(lambda x: x in element, text_content_list))) > 0:
                        return True
            return False

        # Help function for logic operation
        def compare_logic_pair(cond_dict, rank):
            init_cond = False
            for item in rank:
                init_cond = init_cond | cond_dict[item[0]]
                if init_cond:
                    return init_cond

        # Parse kwargs
        try:
            email_list = kwargs['email']
            # Get the lowercase for later comparison
            text_list = list(map(lambda x: x.lower(), kwargs['text']))
        except KeyError:
            return False

        # Enumerate the conditions
        condition_dict = {}
        email_condition = (len(email_list) >= self.EMAIL_LOWER_BOUND) & (len(email_list) <= self.EMAIL_UPPER_BOUND)
        condition_dict['EMAIL'] = email_condition
        for key, values in self.PROFILE_SPECIFIC_WORD.items():
            condition_dict[key] = iterate_and_find(text_content_list=text_list,
                                                   word_list=self.PROFILE_SPECIFIC_WORD[key],
                                                   abbreviated=True if 'abbr' in key.lower() else False)
        return compare_logic_pair(condition_dict, self.RANK)

    def fetch_email_list(self, response):
        # Get email address from html body
        email_match = re.findall(self.EMAIL_REGEX, str(response.body))
        email_list = set(email_match)

        # Get email address from attribute value
        email_xpath_extract = ' '.join(response.xpath('//*/attribute::*[contains(., \'mail\')]').extract())
        email_xpath_match = re.findall(self.EMAIL_REGEX, email_xpath_extract)

        # Strip email found in the xpath
        for email in email_xpath_match:
            email_list.add(email)
            email_xpath_extract = email_xpath_extract.replace(email, '')

        # Find match again if the email address is split in the attribute value
        email_xpath_match_again = re.findall(r'\'([a-zA-Z0-9_.+-]+)\'', email_xpath_extract)
        if len(email_xpath_match_again) >= 2:
            if re.match(r'[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+', email_xpath_match_again[-1]):
                email_list.add('%s@%s' % (email_xpath_match_again[-1], email_xpath_match_again[-2]))
        return email_list

    def fetch_text_content(self, response):
        try:
            text_content = response.xpath(self.TEXT_XPATH).extract()
            text_content = list(map(lambda y: ' '.join(y), list(map(lambda x: x.split(), text_content))))
        except IndexError:
            text_content = 'Nan'
        return text_content

    def parse(self, response):
        # Determine whether the url is valid by calling the instance method determine_validity
        is_valid = self.determine_validity(response.url, response)
        if not is_valid:
            return

        # Uncomment for log info debugging
        # self.logger.info('Visiting %s' % response.url)

        # Get basic information of the page
        # Get school name and page title
        top_domain = get_tld(response.url)
        try:
            school_name = self.inverse_lookup[top_domain]
        except KeyError:
            school_name = 'Unknown School'
        page_title = response.xpath(self.TITLE_XPATH).extract_first()

        # Get page text content
        text_content = self.fetch_text_content(response)
        aggregated_text_content = ' <SEPARATOR> '.join(text_content)

        # Get email list
        email_list = self.fetch_email_list(response)

        # Create an item
        if self.determine_personal_profile_component(email=email_list, text=text_content):
            email_dict = dict(zip(list(map(lambda x: 'Email %d' % x, range(len(email_list)))), email_list))

            # Get pdf link for possible personal CV
            following_pdf_link = response.xpath(self.TEXT_XPATH).extract()
            pdf_dict = dict(zip(list(map(lambda x: 'PDF %d' % x, range(len(following_pdf_link)))), following_pdf_link))

            page_loader = ItemLoader(item=SchoolWebPageItem(), response=response)
            page_loader.add_value('school_name', school_name)
            page_loader.add_value('page_title', page_title)
            page_loader.add_value('page_link', response.url)
            page_loader.add_value('page_text_content', aggregated_text_content)
            page_loader.add_value('crawled_email_list', email_dict)
            page_loader.add_value('crawled_pdf_link', pdf_dict)
            page_loader.add_value('last_updated', str(datetime.now()))
            yield page_loader.load_item()

        # Yield next pages through anchor or source attribute
        for next_page in response.xpath(self.LINK_XPATH):
            yield response.follow(next_page, callback=self.parse)


class SchoolWebsiteListProcessor(object):

    def __init__(self, url_list):
        assert(type(url_list) is list)
        self.url_list = url_list

        # Add corresponding top domains
        self.url_top_domain = set()
        for url in url_list:
            self.url_top_domain.add(get_tld(url))

    def get_url_list(self):
        return self.url_list

    def get_top_domain(self):
        return self.url_top_domain
