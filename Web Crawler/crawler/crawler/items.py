# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

from scrapy.item import Item, Field


class DirectoryItem(Item):
    url = Field()
    depth = Field()
    school_name = Field()
    page_title = Field()
    initial_url = Field()
    href = Field()
    text = Field()

class DepeartmentItem(Item):
    url = Field()
    school_name = Field()
    title = Field()
