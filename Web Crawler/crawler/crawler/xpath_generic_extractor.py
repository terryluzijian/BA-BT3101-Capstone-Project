import re
from scrapy.http import Response
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
                 'not(ancestor::*[contains(@*, "Banner")]) and ' + \
                 'not(ancestor::*[contains(@*, "Crumb")]) and ' + \
                 'not(ancestor::*[contains(@*, "off-canvas")]) and not(ancestor::*[contains(@*, "global")]) and ' + \
                 'not(ancestor::*[contains(@*, "Global")])'
COND_COMBINE = ' and '.join(frozenset([HEADER_IMPLIED, HEADER_TAGGED, FOOTER_TAGGED,
                                       FOOTER_IMPLIED, OTHERS_IMPLIED]))

MAIN_CONTENT_XPATH = '//a[%s][@href[not(contains(., "#"))]]' % COND_COMBINE
MAIN_CONTENT_HREF_XPATH = '%s/@href[not(contains(., "#"))]' % MAIN_CONTENT_XPATH

# Get strictly header content for future filtering
IS_HEADER = '(ancestor::header or ancestor::*[contains(@*, "header")] or ancestor::*[contains(@*, "Header")])'
IS_HEADER_COND = 'and'.join(frozenset([IS_HEADER, FOOTER_TAGGED, FOOTER_IMPLIED, OTHERS_IMPLIED]))
HEADER_XPATH = '//a[%s][@href[not(contains(., "#"))]]' % IS_HEADER_COND
HEADER_HREF_XPATH = '%s/@href[not(contains(., "#"))]' % HEADER_XPATH

# Get menu-level link
EXCEPTIONS = 'ancestor::*[contains(@*, "sticky")] or ancestor::*[contains(@*, "no-bottom")]'
MENU = ' and '.join(['(ancestor::nav or (ancestor::*[contains(@*, "nav")] or ancestor::*[contains(@*, "Nav")])) ' +
                     'and not(ancestor::*[contains(@*, "off-canvas")])',
                     FOOTER_TAGGED, FOOTER_IMPLIED, OTHERS_IMPLIED])
MENU_IMPLIED = 'ancestor::*[contains(@*, "menu")] or ancestor::*[contains(@*, "Menu")]'
MENU_EMBEDDED = '(%s) or (%s)' % (EXCEPTIONS,
                                  ' and '.join([FOOTER_IMPLIED,
                                                FOOTER_TAGGED,
                                                OTHERS_IMPLIED,
                                                '(%s)' % MENU_IMPLIED]))
MENU_COND_COMBINE = '(%s) or (%s)' % (MENU, MENU_EMBEDDED)
MENU_XPATH = '//a[%s][@href[not(contains(., "#"))]]' % MENU_COND_COMBINE
MENU_HREF_XPATH = '%s/@href[not(contains(., "#"))]' % MENU_XPATH

# Get menu-excluded main-content xpath
# TODO: refine menu excluded condition in the future due to lack of robustness
# TODO: double-check any function related to this variable
MENU_EXCLUDED = ' and '.join(list(map(lambda cond: 'not(%s)' % cond, MENU_COND_COMBINE.split(' or '))))

MAIN_CONTENT_NO_MENU_XPATH = MAIN_CONTENT_XPATH + '[%s]' % MENU_EXCLUDED
MAIN_CONTENT_NO_MENU_HREF_XPATH = '%s/@href[not(contains(., "#"))]' % MAIN_CONTENT_NO_MENU_XPATH

TEXT_XPATH = 'descendant-or-self::*[not(self::script) and not(self::style)]/text()[normalize-space(.)]'
# TEXT_MENU_XPATH = 'ancestor::*[self::li or self::div][count(a)=1]/a[not(descendant::script) ' + \
# 'and not(descendant::style)]//text()[normalize-space(.)]'
# NEW_TEXT_XPATH = '%s | preceding-sibling::*/text()[normalize-space(.)] | %s' % (TEXT_MENU_XPATH, TEXT_XPATH)

# h1, h2, h3, title and text
TITLE_XPATH = '//title//text() | //*[contains(@class, "title")]//text()'
H1_XPATH = '//h1//text()'
H2_XPATH = '//h2//text()'
H3_XPATH = '//h3//text()'
MAIN_CONTENT_TEXT_XPATH_RAW = '//*[not(self::script) and not(self::style) and not(self::a)]' \
                              + '[text()[normalize-space(.)]]//text()'
MAIN_CONTENT_TEXT_XPATH = '//*[not(self::script) and not(self::style)][%s][%s][text()[normalize-space(.)]]//text()' \
                          % (MENU_EXCLUDED, COND_COMBINE)

# Menu specific element to pass
MENU_TEXT_FILTER = frozenset(['calendar', 'curriculum', 'event', 'news', 'resource', 'student', 'log'])
MENU_TOKEN_FILTER = frozenset(['hide', 'main', 'menu', 'more', 'show', 'skip', 'back', 'top', 'to'])
FILE_EXTENSION = IGNORED_EXTENSIONS + ['htm', 'html']


# Generic functions

def generic_get_anchor_and_text(response, content_xpath, href_xpath):
    # Get content text list, normalize and concatenate
    content = response.xpath(content_xpath)
    content_text = map(lambda html_string: ' '.join(html_string.xpath(TEXT_XPATH).extract()), content)
    content_text = list(map(lambda text: ' '.join(text.split()), list(content_text)))
    href = list(map(lambda each_string: normalize_string(each_string), response.xpath(href_xpath).extract()))
    text_freq_dict = {}

    # Avoid redundancy
    for index in range(len(content_text)):
        text_ele = content_text[index]
        if text_ele in text_freq_dict.keys():
            if text_freq_dict[text_ele] == 1:
                prev_index = content_text.index(text_ele)
                content_text[prev_index] += '(1)'
            text_freq_dict[text_ele] += 1
            content_text[index] += '(%d)' % text_freq_dict[text_ele]
        else:
            text_freq_dict[text_ele] = 1

    # Zip text and href
    text_link_dict = {text: response.urljoin(link) for text, link in zip(content_text, href)}
    return text_link_dict


def generic_get_unique_content(response, past_response, extract_func=None, get_text=False):
    # Get unique content by comparing with previous response
    # Deal with list or a single Response object
    def normalize_link(link_url):
        # TreatURL with slash at the end as the same as those without a slash
        if link_url[-1] == '/':
            return link_url[:-1]
        else:
            return link_url

    if isinstance(past_response, Response):
        general_past_content = get_general(past_response)
    elif type(past_response) is list:
        # Changed to get_general
        general_past_content = {key: value
                                for element in past_response
                                for key, value in get_general(element).items()}
    else:
        general_past_content = {}

    if not get_text:
        # Return unique content
        assert extract_func is not None
        response_content = extract_func(response)
        if len(response_content) <= 0:
            response_content = get_general(response)
        link_values = list(map(lambda link: normalize_link(link), list(general_past_content.values())))
        content = {text: link for text, link in response_content.items()
                   if (normalize_link(link) not in link_values) & (text not in general_past_content.keys())}
        if (len(content) == 0) & (extract_func != get_general):
            return generic_get_unique_content(response, past_response, get_general)
        else:
            return content
    else:
        text_content = response.xpath(MAIN_CONTENT_TEXT_XPATH_RAW).extract()
        text_content_normalized = list(filter(lambda y: len(y) >= 3, map(lambda x: ' '.join(x.split()), text_content)))
        content_keys = set(map(lambda x: re.sub(r'\([1-9]+\)', '', x), list(general_past_content.keys())))
        return [text_element for text_element in text_content_normalized
                if re.sub(r'\([1-9]+\)', '', text_element) not in content_keys]


# Specific get functions

def get_main_content(response):
    # Main content can include secondary menu
    return generic_get_anchor_and_text(response=response,
                                       content_xpath=MAIN_CONTENT_XPATH,
                                       href_xpath=MAIN_CONTENT_HREF_XPATH)


def get_main_content_unique(response, past_response):
    # Changed to get_general
    return generic_get_unique_content(response, past_response, get_general)


def get_main_content_excluding_menu(response):
    # Rewrite main content with menu content as main content can include secondary menu
    # Delete secondary menu here
    main_content = get_main_content(response)
    menu_content = get_menu(response)
    return {text: link for text, link in main_content.items() if (text not in menu_content.keys())
            | (link not in menu_content.values())}


def get_main_content_text(response):
    # Get main content text
    text_content = response.xpath(MAIN_CONTENT_TEXT_XPATH).extract()
    menu_content = get_menu(response)

    # Double-check there is no menu component
    text_content = [text for text in text_content if text not in menu_content.keys()]
    return list(filter(lambda y: len(y) > 3, map(lambda x: ' '.join(x.split()), text_content)))


def get_header(response):
    # Get header content
    return generic_get_anchor_and_text(response=response,
                                       content_xpath=HEADER_XPATH,
                                       href_xpath=HEADER_HREF_XPATH)


def get_general(response):
    # Get all anchor object for current response
    return generic_get_anchor_and_text(response=response,
                                       content_xpath='//a[@href[not(contains(., "#"))]]',
                                       href_xpath='//a[@href]/@href[not(contains(., "#"))]')


def get_title_h1_h2_h3(response):
    # Get H1, H2 and title-tagged text data
    def get_text(response_fetch, text_xpath, punctuation='!"#$%&\'()*+,-./:;<=>?@[\\]^_`{|}~'):
        # Help function to capitalize words properly
        def capitalize_string(target_string):
            if re.search(r'[A-Z][A-Z]+', target_string):
                return target_string[:1].upper() + target_string[1:].lower()
            else:
                return target_string

        # Get normalized text
        title_content = response_fetch.xpath(text_xpath).extract()
        content_normalized = list(map(lambda text: normalize_string(text, ' '), title_content))
        final_content = [list(map(lambda y: capitalize_string(y),
                                  filter(lambda x: x not in punctuation, element.split())))
                         for element in content_normalized]

        return [' '.join(text_element) for text_element in final_content]

    return {'h1': get_text(response, H1_XPATH),
            'h2': get_text(response, H2_XPATH),
            'h3': get_text(response, H3_XPATH),
            'title': get_text(response, TITLE_XPATH)}


def get_menu(response):
    # Core method for school content crawling
    # Get menu through a more specific method as it is a special case where we
    # want to understand the hierarchy as well
    original_dict = generic_get_anchor_and_text(response, MENU_XPATH, MENU_HREF_XPATH)
    original_dict = {link: text for text, link in original_dict.items()}

    # Select all menu component and the hierarchical text (e.g. About -> Programme -> Undergraduate)
    content_selector = response.xpath(MENU_XPATH)
    content = map(lambda selector: selector.xpath(TEXT_XPATH).extract(), content_selector)

    # Clean the textual data, which is already tokenized, and join them
    content = map(lambda text_list: list(map(lambda text: ' '.join(text.split()), text_list)), list(content))
    content_text = list(map(lambda text_list: ' '.join([element for element in text_list
                                                        if not check_word_filter(element, MENU_TOKEN_FILTER)]),
                            list(content)))

    # Get the href and zip together
    href = list(map(lambda each_string: normalize_string(each_string), response.xpath(MENU_HREF_XPATH).extract()))
    href_temp_list = [[text, response.urljoin(link)] for text, link in zip(content_text, href)
                      if not(check_word_filter(text, MENU_TEXT_FILTER))]
    href_dict = {}
    for tuple_pair in href_temp_list:

        # Check whether hierarchical processing works
        if len(tuple_pair[0]) == 0:
            try:
                ori_name = original_dict[tuple_pair[1]]
                ori_name_strip = re.sub(r'\([1-9]+\)', '', ori_name)
            except KeyError:
                ori_name_strip = ''
                ori_name = '#pass'
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


# Help functions

def normalize_string(target_string, separator=''):
    # Normalize string with
    return separator.join(target_string.split())


def check_word_filter(target_string, filter_set):
    # Check whether certain keyword should be filtered
    target_string_lower = target_string.lower()
    for word in filter_set:
        if word in target_string_lower:
            return True
    return False


def get_main_and_menu(one_response):
    # Help function to get main content with menu (including all navigation, header and secondary menu)
    content = get_main_content(one_response)
    content.update(get_menu(one_response))
    if len(content) <= 0:
        content = get_general(one_response)
    return content
