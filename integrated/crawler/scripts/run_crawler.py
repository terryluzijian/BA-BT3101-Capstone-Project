import scrapy.crawler
from scrapy.utils.project import get_project_settings
from scrapy.utils.url import is_url


class ProfileCrawlerProcess(scrapy.crawler.CrawlerProcess):

    USE_CASE = frozenset(['GENERIC_BROAD', 'GENERIC_PAR', 'PRIORITIZE_BROAD', 'PRIORITIZE_PAR', 'TEST'])
    CRAWLER_NAME = 'core'
    PAGE_LIMIT = 50

    def __init__(self, crawl_type):
        assert type(crawl_type) is str and crawl_type in ProfileCrawlerProcess.USE_CASE
        super(ProfileCrawlerProcess, self).__init__(settings=get_project_settings())
        self.crawler_type = crawl_type
        if crawl_type in ['GENERIC_PAR', 'PRIORITIZE_PAR']:
            self.settings.update({'CLOSESPIDER_PAGECOUNT': ProfileCrawlerProcess.PAGE_LIMIT})

    def start_crawl(self, *args):
        if self.crawler_type == 'GENERIC_PAR':
            assert len(args) == 1 and is_url(args[0])
            self.crawl(ProfileCrawlerProcess.CRAWLER_NAME, particular_url=args[0])
        elif self.crawler_type == 'PRIORITIZE_PAR':
            assert len(args) == 2 and type(args[0]) is list and type(args[1]) is str
            self.crawl(ProfileCrawlerProcess.CRAWLER_NAME, start_university=args[0],
                       start_department=args[1], **{'PRIORITIZED': True})
        else:
            if self.crawler_type == 'GENERIC_BROAD':
                self.crawl(ProfileCrawlerProcess.CRAWLER_NAME, **{'GENERIC': True})
            elif self.crawler_type == 'TEST':
                self.crawl(ProfileCrawlerProcess.CRAWLER_NAME, **{'TESTING': True})
            elif self.crawler_type == 'PRIORITIZE_BROAD':
                self.crawl(ProfileCrawlerProcess.CRAWLER_NAME, **{'PRIORITIZED': True})
        self.start()


def run_crawler(crawler_type, *args):
    process = ProfileCrawlerProcess(crawler_type)
    if crawler_type in ['GENERIC_BROAD', 'PRIORITIZE_BROAD', 'TEST']:
        process.start_crawl()
    elif crawler_type == 'PRIORITIZE_PAR':
        assert len(args) == 2
        process.start_crawl(args[0], args[1])
    elif crawler_type == 'GENERIC_PAR':
        assert len(args) == 1
        process.start_crawl(args[0])

# Example:
# if __name__ == '__main__':
#    run_crawler('GENERIC_BROAD')
#    run_crawler('PRIORITIZE_BROAD')
#    run_crawler('TEST')
#    run_crawler('PRIORITIZE_PAR', ['Queen Mary University of London', 'University of British Columbia'], 'Geography')
#    run_crawler('GENERIC_PAR', 'https://economics.stanford.edu/')
#

run_crawler('PRIORITIZE_BROAD')
