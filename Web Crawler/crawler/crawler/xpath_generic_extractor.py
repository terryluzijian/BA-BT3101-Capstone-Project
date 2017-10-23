from lxml import html


class GenericExtractor(object):

    '''

    The following variables specifies nodes to select by XPath

    Characteristics of school websites lie in that for homepage, or navigation page, key
    (or more important, detailed) information can be assumed to be solely retrievable
    through clicking on anchored links embedded in menu of the current page.

    '''

    # User ancestor axis to filter out certain tags optionally with certain attributes
    HEADER_TAGGED = 'not(ancestor::header)'
    HEADER_IMPLIED = 'not(ancestor::*[contains(@*, "head")])'
    FOOTER_TAGGED = 'not(ancestor::footer)'
    FOOTER_IMPLIED = 'not(ancestor::*[contains(@*, "bottom")]) and not(ancestor::*[contains(@*, "foot")])'
    OTHERS_IMPLIED = 'not(ancestor::*[contains(@*, "banner")]) and not(ancestor::*[contains(@*, "crumb")])' + \
                     ' and not(ancestor::*[contains(@*, "quick")])'
    COND_COMBINE = ' and '.join(frozenset([HEADER_IMPLIED, HEADER_TAGGED, FOOTER_TAGGED,
                                           FOOTER_IMPLIED, OTHERS_IMPLIED]))

    MAIN_CONTENT_XPATH = '//a[%s]' % COND_COMBINE
    MAIN_CONTENT_HREF_XPATH = '%s/@href' % MAIN_CONTENT_XPATH

    # Get menu-level link
    MENU = 'ancestor::nav and not(ancestor::*[contains(@*, "off-canvas")])'
    MENU_IMPLIED = 'ancestor::*[contains(@*, "menu")]'
    MENU_EMBEDDED = ' and '.join([FOOTER_IMPLIED, FOOTER_TAGGED, OTHERS_IMPLIED, '(%s)' % MENU_IMPLIED])
    MENU_COND_COMBINE = '(%s) or (%s)' % (MENU, MENU_EMBEDDED)
    MENU_XPATH = '//a[%s]' % MENU_COND_COMBINE
    MENU_HREF_XPATH = '%s/@href' % MENU_XPATH

    # Get menu-excluded main-content xpath
    MENU_EXCLUDED = ' and '.join(list(map(lambda cond: 'not(%s)' % cond, MENU_COND_COMBINE.split(' or '))))

    MAIN_CONTENT_NO_MENU_XPATH = MAIN_CONTENT_XPATH + '[%s]' % MENU_EXCLUDED
    MAIN_CONTENT_NO_MENU_HREF_XPATH = '%s/@href' % MAIN_CONTENT_NO_MENU_XPATH

    TEXT_XPATH = '//text()[normalize-space(.)]'

    def generic_get_anchor_and_text(self, response, content_xpath, href_xpath):
        content = response.xpath(content_xpath).extract()
        content_text = map(lambda html_string: ' '.join(html.fromstring(html_string).xpath(self.TEXT_XPATH)), content)
        content_text = list(map(lambda text: ' '.join(text.split()), list(content_text)))
        href = response.xpath(href_xpath).extract()
        return {link: text for link, text in zip(href, content_text)}

    def get_main_content(self, response):
        return self.generic_get_anchor_and_text(response=response,
                                                content_xpath=self.MAIN_CONTENT_XPATH,
                                                href_xpath=self.MAIN_CONTENT_HREF_XPATH)

    def get_main_content_excluding_menu(self, response):
        return self.generic_get_anchor_and_text(response=response,
                                                content_xpath=self.MAIN_CONTENT_NO_MENU_XPATH,
                                                href_xpath=self.MAIN_CONTENT_NO_MENU_HREF_XPATH)

    def get_menu(self, response):
        return self.generic_get_anchor_and_text(response=response,
                                                content_xpath=self.MENU_XPATH,
                                                href_xpath=self.MENU_HREF_XPATH)
