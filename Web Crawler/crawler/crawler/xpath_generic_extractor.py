import re
from lxml import html
from scrapy.linkextractors import IGNORED_EXTENSIONS
from scrapy.utils.url import parse_url

# The following variables specifies nodes to select by XPath

# Characteristics of school websites lie in that for homepage, or navigation page, key
# (or more important, detailed) information can be assumed to be solely retrievable
# through clicking on anchored links embedded in menu of the current page

# User ancestor axis to filter out certain tags optionally with certain attributes
HEADER_TAGGED = 'not(ancestor::header)'
HEADER_IMPLIED = 'not(ancestor::*[contains(@*, "head")]) and not(ancestor::*[contains(@*, "Head")])'
FOOTER_TAGGED = 'not(ancestor::footer)'
FOOTER_IMPLIED = 'not(ancestor::*[contains(@*, "bottom")]) and not(ancestor::*[contains(@*, "foot")]) and ' + \
                 'not(ancestor::*[contains(@*, "Bottom")]) and not(ancestor::*[contains(@*, "Foot")])'
OTHERS_IMPLIED = 'not(ancestor::*[contains(@*, "banner")]) and not(ancestor::*[contains(@*, "crumb")]) and ' + \
                 'not(ancestor::*[contains(@*, "quick")]) and not(ancestor::*[contains(@*, "Banner")]) and ' + \
                 'not(ancestor::*[contains(@*, "Crumb")]) and not(ancestor::*[contains(@*, "Quick")])'
COND_COMBINE = ' and '.join(frozenset([HEADER_IMPLIED, HEADER_TAGGED, FOOTER_TAGGED,
                                       FOOTER_IMPLIED, OTHERS_IMPLIED]))

MAIN_CONTENT_XPATH = '//a[%s][@href]' % COND_COMBINE
MAIN_CONTENT_HREF_XPATH = '%s/@href' % MAIN_CONTENT_XPATH

# Get strictly header content for future filtering
IS_HEADER = '(ancestor::header or ancestor::*[contains(@*, "header")] or ancestor::*[contains(@*, "Header")])'
IS_HEADER_COND = 'and'.join(frozenset([IS_HEADER, FOOTER_TAGGED, FOOTER_IMPLIED, OTHERS_IMPLIED]))
HEADER_XPATH = '//a[%s][@href]' % IS_HEADER_COND
HEADER_HREF_XPATH = '%s/@href' % HEADER_XPATH

# Get menu-level link
EXCEPTIONS = 'ancestor::*[contains(@*, "sticky")]'
MENU = '(ancestor::nav or ' + \
       '(ancestor::*[contains(@*, "nav")] or ancestor::*[contains(@*, "Nav")])) ' + \
       'and not(ancestor::*[contains(@*, "off-canvas")])'
MENU_IMPLIED = 'ancestor::*[contains(@*, "menu")] or ancestor::*[contains(@*, "Menu")]'
MENU_EMBEDDED = '(%s) or (%s)' % (EXCEPTIONS,
                                  ' and '.join([FOOTER_IMPLIED,
                                                FOOTER_TAGGED,
                                                OTHERS_IMPLIED,
                                                '(%s)' % MENU_IMPLIED]))
MENU_COND_COMBINE = '(%s) or (%s)' % (MENU, MENU_EMBEDDED)
MENU_XPATH = '//a[%s][@href]' % MENU_COND_COMBINE
MENU_HREF_XPATH = '%s/@href' % MENU_XPATH

# Get menu-excluded main-content xpath
MENU_EXCLUDED = ' and '.join(list(map(lambda cond: 'not(%s)' % cond, MENU_COND_COMBINE.split(' or '))))

MAIN_CONTENT_NO_MENU_XPATH = MAIN_CONTENT_XPATH + '[%s]' % MENU_EXCLUDED
MAIN_CONTENT_NO_MENU_HREF_XPATH = '%s/@href' % MAIN_CONTENT_NO_MENU_XPATH

TEXT_XPATH = '//*[not(self::script) and not(self::style)]/text()[normalize-space(.)]'
TEXT_MENU_XPATH = 'ancestor::*[self::li or self::div][count(a)=1]/a[not(descendant::script) ' + \
                  'and not(descendant::style)]/text()[normalize-space(.)]'

# Menu specific element to pass
MENU_TEXT_FILTER = frozenset(['calendar', 'curriculum', 'event', 'news', 'resource', 'student'])
MENU_TOKEN_FILTER = frozenset(['hide', 'main', 'menu', 'more', 'show', 'skip'])
FILE_EXTENSION = IGNORED_EXTENSIONS + ['htm', 'html']


def generic_get_anchor_and_text(response, content_xpath, href_xpath):
    content = response.xpath(content_xpath).extract()
    content_text = map(lambda html_string: ' '.join(html.fromstring(html_string).xpath(TEXT_XPATH)), content)
    content_text = list(map(lambda text: ' '.join(text.split()), list(content_text)))
    href = response.xpath(href_xpath).extract()
    text_freq_dict = {}

    # Avoid redundancy
    for index in range(len(content_text)):
        text_ele = content_text[index]
        if text_ele in text_freq_dict.keys():
            content_text[index] += '(%d)' % text_freq_dict[text_ele]
            text_freq_dict[text_ele] += 1
        else:
            text_freq_dict[text_ele] = 1

    # Zip text and href
    return {text: link for text, link in zip(content_text, href)}


def get_main_content(response):
    return generic_get_anchor_and_text(response=response,
                                       content_xpath=MAIN_CONTENT_XPATH,
                                       href_xpath=MAIN_CONTENT_HREF_XPATH)


def get_main_content_excluding_menu(response):
    return generic_get_anchor_and_text(response=response,
                                       content_xpath=MAIN_CONTENT_NO_MENU_XPATH,
                                       href_xpath=MAIN_CONTENT_NO_MENU_HREF_XPATH)


def get_header(response):
    return generic_get_anchor_and_text(response=response,
                                       content_xpath=HEADER_XPATH,
                                       href_xpath=HEADER_HREF_XPATH)


def get_general(response):
    return generic_get_anchor_and_text(response=response,
                                       content_xpath='//a[@href]',
                                       href_xpath='//a[@href]/@href')


def get_menu(response):
    # Core method for school content crawling
    # Check whether certain keyword should be filtered
    def check_word_filter(target_string, filter_set):
        target_string_lower = target_string.lower()
        for word in filter_set:
            if word in target_string_lower:
                return True
        return False

    # Get menu through a more specific method as it is a special case where we
    # want to understand the hierarchy as well
    original_dict = generic_get_anchor_and_text(response, MENU_XPATH, MENU_HREF_XPATH)
    original_dict = {link: text for text, link in original_dict.items()}

    # Select all menu component and the hierarchical text (e.g. About -> Programme -> Undergraduate)
    content_selector = response.xpath(MENU_XPATH)
    content = map(lambda selector: selector.xpath(TEXT_MENU_XPATH).extract(), content_selector)

    # Clean the textual data, which is already tokenized, and join them
    content = map(lambda text_list: list(map(lambda text: ' '.join(text.split()), text_list)), list(content))
    content_text = list(map(lambda text_list: ' '.join([element for element in text_list
                                                        if not check_word_filter(element, MENU_TOKEN_FILTER)]),
                            list(content)))

    # Get the href and zip together
    href = response.xpath(MENU_HREF_XPATH).extract()
    href_temp_list = [[text, link] for text, link in zip(content_text, href) if not check_word_filter(text,
                                                                                                      MENU_TEXT_FILTER)]
    href_dict = {}
    for tuple_pair in href_temp_list:

        # Check whether hierarchical processing works
        if len(tuple_pair[0]) == 0:
            ori_name = original_dict[tuple_pair[1]]
            ori_name_strip = re.sub(r'\([1-9]+\)', '', ori_name)
            # If the original name is empty
            if len(ori_name_strip) != 0:
                href_dict[ori_name] = tuple_pair[1]
            else:
                # Parse the href and create a name
                href_parse = parse_url(tuple_pair[1])

                # Clean the href to only remain meaningful text
                href_clean = re.sub(r'[!"#$%&\'()*+,-./:;<=>?@[\\\]^_`{|}~]+', ' ',
                                    string=' '.join([href_parse.path, href_parse.params, href_parse.query]))
                href_in_text = ' '.join(set(filter(lambda token: (token != '') and (token not in FILE_EXTENSION),
                                                   href_clean.split(' '))))

                # Set the dict value accordingly
                href_dict[href_in_text] = tuple_pair[1]
        else:
            href_dict[tuple_pair[0]] = tuple_pair[1]

    return href_dict
