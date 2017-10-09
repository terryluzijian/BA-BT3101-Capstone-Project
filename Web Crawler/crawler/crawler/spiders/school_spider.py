import re
import scrapy
from crawler.items import SchoolWebPageItem
from datetime import datetime
from scrapy.exceptions import NotSupported
from scrapy.loader import ItemLoader
from tld import get_tld


class SchoolWebsiteContentSpider(scrapy.Spider):

    name = 'school'
    allowed_domains = []

    # Customized filtering params
    FILTERED_TAG_NAME = ['script', 'style', 'title']
    FILTERED_WORD_NAME = ['library', 'wiki', 'login', 'new', 'lib.']

    # Fixed XPath
    TITLE_XPATH = '//title/text()'
    STRING_LENGTH_LIMIT = 4
    TEXT_XPATH = '//*[not(%s)]/text()[normalize-space(.)][string-length() >= %d]' % \
                 ('|'.join(list(map(lambda x: 'self::%s' % x, FILTERED_TAG_NAME))), STRING_LENGTH_LIMIT)
    LINK_XPATH = '//*/attribute::src|//*/attribute::href'
    PDF_XPATH = '//*/attribute::src[contains(., \'.pdf\')]|//*/attribute::href[contains(., \'.pdf\')]'

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
        if '<html>' not in response.body:
            return False

        # if the url contains words in the filtered word list
        for word in self.FILTERED_WORD_NAME:
            if word in url:
                return False

        return True

    def parse(self, response):
        # Determine whether the url is valid by calling the instance method determine_validity
        is_valid = self.determine_validity(response.url, response)
        if not is_valid:
            return

        # Uncomment for log info debugging
        # self.logger.info('Visiting %s' % response.url)

        # Get basic information of the page
        # Get school name
        top_domain = get_tld(response.url)
        try:
            school_name = self.inverse_lookup[top_domain]
        except KeyError:
            school_name = 'Unknown School'

        # Get page title
        try:
            page_title = response.xpath(self.TITLE_XPATH).extract_first()
        except NotSupported:
            return

        # Get page text content
        try:
            text_content = response.xpath(self.TEXT_XPATH).extract()
            text_content = list(map(lambda y: ' '.join(y), list(map(lambda x: x.split(), text_content))))
        except IndexError:
            text_content = 'Nan'
        aggregated_text_content = ' <SEPARATOR> '.join(text_content)

        # Get email list
        email_match = re.findall(self.EMAIL_REGEX, str(response.body))
        email_list = list(set(email_match))
        # Create an item
        if (len(email_list) >= 1) & (len(email_list) <= 3):
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

        # Adding corresponding top domains
        self.url_top_domain = set()
        for url in url_list:
            self.url_top_domain.add(get_tld(url))

    def get_url_list(self):
        return self.url_list

    def get_top_domain(self):
        return self.url_top_domain
