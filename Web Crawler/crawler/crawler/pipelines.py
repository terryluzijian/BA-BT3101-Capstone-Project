# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html


from scrapy.exceptions import DropItem
from scrapy.core.scraper import logger


class FilterWebsitePipeline(object):

    WORD_MAXIMUM_FREQUENCY = 5
    EMAIL_UPPER_BOUND = 3
    EMAIL_LOWER_BOUND = 1

    # Specific word list
    PROFILE_SPECIFIC_WORD = {
        'PERSONAL': ('biography', 'contact', 'email', 'phone number', 'tel', 'office'),
        'POST_FULL': ('professor', 'lecturer', 'associate professor', 'assistant professor', 'doctor'),
        'POST_ABBR': ('prof ', 'a/p ', 'assoc prof ', 'dr '),
        'RESEARCH': ('research interest', 'specialized area', 'research area', 'areas of interest',
                     'publication', 'selected publications')
    }

    def __init__(self):
        self.email_address_combination_frequency_dict = {}

    def determine_personal_profile_component(self, **kwargs):
        # Help function to locate certain keywords
        def iterate_and_find(text_content_list, word_list):
            for element in word_list:
                length_captured = len(list(filter(lambda x: element in x, text_content_list)))
                if (length_captured > 0) & (length_captured < self.WORD_MAXIMUM_FREQUENCY):
                    return True
            return False

        # Parse kwargs
        try:
            email_list = kwargs['email']
            # Get the lowercase for later comparison
            text_list = list(map(lambda x: x.lower(), kwargs['text']))
        except KeyError:
            return False

        # Enumerate the conditions
        # email_condition_upper = len(email_list) <= self.EMAIL_UPPER_BOUND
        condition_dict = {}
        email_condition_lower = len(email_list) >= self.EMAIL_LOWER_BOUND
        condition_dict['EMAIL'] = email_condition_lower
        for key, values in self.PROFILE_SPECIFIC_WORD.items():
            condition_dict[key] = iterate_and_find(text_content_list=text_list,
                                                   word_list=self.PROFILE_SPECIFIC_WORD[key])

        # Combine the conditions
        personal_contact_cond = condition_dict['EMAIL'] & condition_dict['PERSONAL']
        personal_post_cond = condition_dict['POST_FULL'] & condition_dict['POST_ABBR']
        return condition_dict['RESEARCH'] | (personal_post_cond & personal_contact_cond)

    def process_item(self, item, spider):
        item = {key: value[0] for key, value in item.items()}
        # Process link
        if item['page_link'].count('/') <= 3:
            raise DropItem('Drop page %s as it is a homepage' % item['page_link'])
        # Process content
        if not self.determine_personal_profile_component(email=item['crawled_email_list'],
                                                         text=item['page_text_content'].split(' <SEPARATOR> ')):
            raise DropItem('Drop page %s as it is not personal profile specific' % item['page_link'])
        # Process email list
        email_combination = tuple(item['crawled_email_list'].values())
        if len(email_combination) > self.EMAIL_UPPER_BOUND:
            raise DropItem('Drop page %s as it contains too much email addresses' % item['page_link'])
        if email_combination in self.email_address_combination_frequency_dict.keys():
            self.email_address_combination_frequency_dict[email_combination] += 1
        else:
            self.email_address_combination_frequency_dict[email_combination] = 1
        return item
