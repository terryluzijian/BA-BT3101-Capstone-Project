# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

import codecs
import json
from scrapy.exceptions import DropItem


class DropWebsiteWithDuplicateEmailPipeline(object):

    maximum_frequency = 1

    def __init__(self):
        self.email_address_combination_frequency_dict = {}

    def process_item(self, item, spider):
        item = {key: value[0] for key, value in item.items()}
        email_combination = tuple(item['crawled_email_list'].values())
        if email_combination in self.email_address_combination_frequency_dict.keys():
            self.email_address_combination_frequency_dict[email_combination] += 1
            if self.email_address_combination_frequency_dict[email_combination] >= self.maximum_frequency:
                raise DropItem('Drop page %s due to duplicate email combination' % item['page_link'])
            else:
                print(item['page_link'], item['crawled_email_list'])
                return item
        else:
            print(item['page_link'], item['crawled_email_list'])
            self.email_address_combination_frequency_dict[email_combination] = 1
            return item
