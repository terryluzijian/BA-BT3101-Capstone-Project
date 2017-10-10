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
    FILTERED_TAG_NAME = ('script', 'style', 'title')
    FILTERED_WORD_NAME = ('library', 'wiki', 'login', 'new', 'faq', 'article', 'blog', 'file', 'search',
                          'contact', 'map', 'calendar', 'form', 'conference', 'help', 'register', 'apply',
                          'program', 'internship', 'video', 'book', 'scholarship', 'paper', 'slide', 'welcome',
                          'gallery', 'citation', 'title', 'event', 'plugin', 'admission', 'student',
                          'image', 'author', 'feed', 'product', 'career', 'lecture', 'about', 'tour', 'time',
                          'module', 'workshop', 'present', 'demo', 'log', 'database', 'term')
    FILTERED_PORTAL_NAME = ('lib', 'library', 'new', 'event')
    FILTERED_FILE_SUFFIX = ('js', 'css', 'img', 'png', 'mp4', 'jpg', 'jpeg', 'gif', 'avi', 'flv', 'doc', 'docx'
                            'ico', 'pdf')
    FILTERED_URL_FORMAT = ('-', '_', '&', '=', '%')

    # Constant
    STRING_LENGTH_LIMIT = 4
    LINK_PARAMS_LENGTH_MAX = 4

    # Fixed XPath
    TITLE_XPATH = '//title/text()'
    TEXT_XPATH = '//*[not(%s)]/text()[normalize-space(.)][string-length() >= %d]' % \
                 ('|'.join(list(map(lambda x: 'self::%s' % x, FILTERED_TAG_NAME))), STRING_LENGTH_LIMIT)
    LINK_XPATH = '//*/attribute::src|//*/attribute::href'
    PDF_XPATH = '//*/attribute::src[contains(., \'.pdf\')]|//*/attribute::href[contains(., \'.pdf\')]'
    EMAIL_XPATH = '//*/attribute::*[contains(., \'mail\')]'

    # Regex
    EMAIL_REGEX = r'([a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+)'
    DATE_REGEX = r'(20[0-9]{2}).?([1][0-2]|[0]?[1-9])(.([1-3][0-9]|[0]?[1-9]))?'

    # School URL list here
    SCHOOL_WEBSITE_DICT = {
        'National University of Singapore': 'http://www.nus.edu.sg/',
        # 'Harvard University': 'https://www.harvard.edu/'
    }

    def __init__(self, *args, **kwargs):
        super(SchoolWebsiteContentSpider, self).__init__(*args, **kwargs)
        self.start_urls = list(self.SCHOOL_WEBSITE_DICT.values())

        # Only go through urls that are within the domain (e.g. Facebook page etc. should not be included)
        website_list = SchoolWebsiteListProcessor(self.start_urls)
        self.allowed_domains = website_list.get_top_domain()

        # Inversely look up for the school name and pass it as a value later in the parse callback
        self.domain_lookup = dict(zip(self.SCHOOL_WEBSITE_DICT.keys(), website_list.get_top_domain()))
        self.inverse_lookup = {website: name for name, website in self.domain_lookup.items()}

    def determine_portal(self, url):
        try:
            link = re.findall(r'http[s]?://([^/]+)', url)[0].lower()
            for word in self.FILTERED_PORTAL_NAME:
                if word in link:
                    return False
            return True
        except IndexError:
            return False

    def determine_validity(self, url):
        # if the url is fundamentally invalid
        if not url.startswith('http'):
            return False
        if url[-1] == '/':
            while url[-1] == '/':
                url = url[:-1]
        url = url.lower()

        # Check whether the url leads to undesired portal
        if not self.determine_portal(url):
            return

        # if the url is date-specific, it is less likely to be a profile page
        if re.search(self.DATE_REGEX, url):
            return False

        # if the url is neither a html document nor a pdf file (CV)
        url_suffix = url.split('.')[-1]
        if url_suffix in self.FILTERED_FILE_SUFFIX:
            return False
        suffix_list = list(map(lambda x: '.%s' % x, self.FILTERED_FILE_SUFFIX))
        link_end = url.split('/')[-1]
        for suffix in suffix_list:
            if suffix in link_end:
                return False

        # if the url contains words in the filtered word list
        for word in self.FILTERED_WORD_NAME:
            if word in url:
                return False

        # If the url is clearly with a news or article title (- or %20)
        for form in self.FILTERED_URL_FORMAT:
            if len(link_end.split(form)) >= self.LINK_PARAMS_LENGTH_MAX:
                return False

        return True

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
        return list(filter(lambda x: get_tld(response.url).split('.')[0] in x.split('@')[-1], email_list))

    def fetch_text_content(self, response):
        try:
            text_content = response.xpath(self.TEXT_XPATH).extract()
            text_content = list(map(lambda y: ' '.join(y), list(map(lambda x: x.split(), text_content))))
        except IndexError:
            text_content = 'Nan'
        return text_content

    def parse(self, response):
        # Determine whether the url is valid by calling the instance method determine_validity
        is_valid = self.determine_validity(response.url)
        if not is_valid:
            return
        # Uncomment for log info debugging
        # self.logger.info('Visiting %s' % response.url)
        print(response.url)

        # Get basic information of the page
        # Get school name and page title
        top_domain = get_tld(response.url)
        try:
            school_name = self.inverse_lookup[top_domain]
        except KeyError:
            school_name = 'Unknown School'
        try:
            page_title = response.xpath(self.TITLE_XPATH).extract_first()
            # Get page text content
            text_content = self.fetch_text_content(response)
            aggregated_text_content = ' <SEPARATOR> '.join(text_content)
        except NotSupported:
            return

        # Get email list
        email_list = self.fetch_email_list(response)

        # Create an item
        email_dict = dict(zip(list(map(lambda x: 'Email %d' % x, range(len(email_list)))), email_list))

        # Get pdf link for possible personal CV
        following_pdf_link = response.xpath(self.PDF_XPATH).extract()
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
