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

    # Customized params
    filtered_tag_name = ['script', 'style', 'title']
    filtered_word_name = ['library', 'wiki', 'login', 'new', 'lib.']
    title_xpath = '//title/text()'
    string_length_limit = 4
    text_xpath = '//*[not(%s)]/text()[normalize-space(.)][string-length() >= %d]' % \
                 ('|'.join(list(map(lambda x: 'self::%s' % x, filtered_tag_name))), string_length_limit)
    email_regex = r'[\w\.-]+@[\w\.-]+'
    date_regex = r'(20[0-9]{2}).{0,1}([1][0-2]|[0]{0,1}[1-9])(.([1-3][0-9]|[0]{0,1}[1-9])){0,1}'

    # School URL list here
    school_website_dict = {
        'National University of Singapore': 'http://www.nus.edu.sg/'
    }

    def __init__(self, *args, **kwargs):
        super(SchoolWebsiteContentSpider, self).__init__(*args, **kwargs)
        self.start_urls = list(self.school_website_dict.values())
        website_list = SchoolWebsiteListProcessor(self.start_urls)
        self.allowed_domains = website_list.get_top_domain()
        self.inverse_lookup = {website: name for name, website in zip(self.school_website_dict.keys(),
                                                                      website_list.get_top_domain())}

    def determine_validity(self, url):
        if re.search(self.date_regex, url):
            return False
        for word in self.filtered_word_name:
            if word in url:
                return False
        return True

    def parse(self, response):
        # Determine whether the url is valid
        if not self.determine_validity(response.url):
            return
        # self.logger.info('Visiting %s' % response.url)
        # Get school name
        top_domain = get_tld(response.url)
        try:
            school_name = self.inverse_lookup[top_domain]
        except KeyError:
            school_name = 'Unknown School'
        # Get page title
        try:
            page_title = response.xpath(self.title_xpath).extract_first()
        except NotSupported:
            return
        # Get page text content
        try:
            text_content = response.xpath(self.text_xpath).extract()
            text_content = list(map(lambda y: ' '.join(y), list(map(lambda x: x.split(), text_content))))
        except IndexError:
            text_content = 'Nan'
        aggregated_text_content = ' <SEPARATOR> '.join(text_content)
        # Get email list
        email_match = re.findall(self.email_regex, str(response.body))
        email_list = list(set(email_match))
        # Create an item
        if (len(email_list) >= 1) & (len(email_list) <= 3):
            page_loader = ItemLoader(item=SchoolWebPageItem(), response=response)
            page_loader.add_value('school_name', school_name)
            page_loader.add_value('page_title', page_title)
            page_loader.add_value('page_link', response.url)
            page_loader.add_value('page_text_content', aggregated_text_content)
            page_loader.add_value('crawled_email_list',
                                  dict(zip(list(map(lambda x: 'Email %d' % x, range(len(email_list)))), email_list)))
            page_loader.add_value('last_updated', str(datetime.now()))
            yield page_loader.load_item()
        for next_page in response.xpath('//a/@href'):
            yield response.follow(next_page, callback=self.parse)


class SchoolWebsiteListProcessor(object):

    def __init__(self, url_list):
        assert(type(url_list) is list)
        self.url_list = url_list
        self.url_top_domain = set()
        for url in url_list:
            self.url_top_domain.add(get_tld(url))

    def get_url_list(self):
        return self.url_list

    def get_top_domain(self):
        return self.url_top_domain
