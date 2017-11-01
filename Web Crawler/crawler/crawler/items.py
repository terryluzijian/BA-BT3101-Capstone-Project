# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

from scrapy.item import Item, Field
from scrapy.loader.processors import TakeFirst


class DirectoryItem(Item):
    url = Field()
    depth = Field()
    school_name = Field()
    page_title = Field()
    initial_url = Field()
    href = Field()
    text = Field()


class DepartmentItem(Item):
    url = Field()
    school_name = Field()
    title = Field()
    department_or_faculty = Field()


class ProfilePageItem(Item):
    name = Field(output_processor=TakeFirst())
    department = Field(output_processor=TakeFirst())
    university = Field(output_processor=TakeFirst())
    profile_link = Field(output_processor=TakeFirst())
    position = Field(output_processor=TakeFirst())
    phd_year = Field(output_processor=TakeFirst())
    phd_school = Field(output_processor=TakeFirst())
    promotion_year = Field(output_processor=TakeFirst())
    text_raw = Field(output_processor=TakeFirst())
    tag = Field(output_processor=TakeFirst())
