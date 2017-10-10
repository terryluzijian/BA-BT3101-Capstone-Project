import json
import os
import scrapy
from urllib.parse import urljoin


class UniversityHomepageCrawler(scrapy.Spider):

    name = 'university_homepage_spider'
    PREVIOUS_URL = 'https://www.timeshighereducation.com/world-university-rankings/'
    FILE_NAME = 'UNIVERSITY_LINK_PRE.json'
    NEW_FILE_NAME = 'UNIVERSITY_LINK.json'
    HOMEPAGE_XPATH = '//*[text() =\'Official website\']/parent::*/@href'

    def __init__(self, *args, **kwargs):
        super(UniversityHomepageCrawler, self).__init__(*args, **kwargs)
        self.new_file_path = self.return_file_path().replace(self.FILE_NAME, self.NEW_FILE_NAME)
        self.file_exist = os.path.isfile(self.new_file_path)
        if not self.file_exist:
            with open(self.new_file_path, mode='w') as f:
                json.dump({}, f, indent=2)

    def start_requests(self):
        university_pre_link_json = json.load(open(self.return_file_path()))
        for university_name in university_pre_link_json.keys():
            university_sub_dict = university_pre_link_json[university_name]
            university_pre_url = university_sub_dict['Url']
            university_rank = university_sub_dict['Rank']
            university_request = scrapy.Request(url=urljoin(self.PREVIOUS_URL, university_pre_url),
                                                callback=self.parse)
            university_request.meta['rank'] = university_rank
            university_request.meta['name'] = university_name
            if not self.file_exist:
                yield university_request

    def parse(self, response):
        homepage_url = response.xpath(self.HOMEPAGE_XPATH).extract_first()
        university_name = response.meta['name']
        university_rank = response.meta['rank']
        self.logger.info('Getting homepage for %s with rank %s' % (university_name, university_rank))
        entry = {university_name: {'Homepage': homepage_url, 'Rank': university_rank}}
        feeds = json.load(open(self.new_file_path))
        feeds.update(entry)
        with open(self.new_file_path, mode='w') as f:
            json.dump(feeds, f, indent=2)

    def return_file_path(self):
        parent_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
        file_path = parent_path + '/data/%s' % self.FILE_NAME
        return file_path
