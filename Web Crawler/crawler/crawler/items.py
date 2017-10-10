# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class SchoolWebPageItem(scrapy.Item):
    school_name = scrapy.Field()
    page_title = scrapy.Field()
    page_link = scrapy.Field()
    page_text_content = scrapy.Field()
    crawled_email_list = scrapy.Field()
    crawled_pdf_link = scrapy.Field()
    last_updated = scrapy.Field(serializer=str)

    def __repr__(self):
        return repr({'Page Link': self['page_link'],
                     'Page Title': self['page_title']})
