from lxml import html

# The following variables specifies nodes to select by XPath

# Characteristics of school websites lie in that for homepage, or navigation page, key
# (or more important, detailed) information can be assumed to be solely retrievable
# through clicking on anchored links embedded in menu of the current page

# User ancestor axis to filter out certain tags optionally with certain attributes
HEADER_TAGGED = 'not(ancestor::header)'
HEADER_IMPLIED = 'not(ancestor::*[contains(@*, "head")])'
FOOTER_TAGGED = 'not(ancestor::footer)'
FOOTER_IMPLIED = 'not(ancestor::*[contains(@*, "bottom")]) and not(ancestor::*[contains(@*, "foot")])'
OTHERS_IMPLIED = 'not(ancestor::*[contains(@*, "banner")]) and not(ancestor::*[contains(@*, "crumb")])' + \
                 ' and not(ancestor::*[contains(@*, "quick")])'
COND_COMBINE = ' and '.join(frozenset([HEADER_IMPLIED, HEADER_TAGGED, FOOTER_TAGGED,
                                       FOOTER_IMPLIED, OTHERS_IMPLIED]))

MAIN_CONTENT_XPATH = '//a[%s][@href]' % COND_COMBINE
MAIN_CONTENT_HREF_XPATH = '%s/@href' % MAIN_CONTENT_XPATH

# Get strictly header content for future filtering
IS_HEADER = '(ancestor::header or ancestor::*[contains(@*, "header")])'
IS_HEADER_COND = 'and'.join(frozenset([IS_HEADER, FOOTER_TAGGED, FOOTER_IMPLIED, OTHERS_IMPLIED]))
HEADER_XPATH = '//a[%s][@href]' % IS_HEADER_COND
HEADER_HREF_XPATH = '%s/@href' % HEADER_XPATH

# Get menu-level link
MENU = '(ancestor::nav or ancestor::*[contains(@*, "nav")]) and not(ancestor::*[contains(@*, "off-canvas")])'
MENU_IMPLIED = 'ancestor::*[contains(@*, "menu")]'
MENU_EMBEDDED = ' and '.join([FOOTER_IMPLIED, FOOTER_TAGGED, OTHERS_IMPLIED, '(%s)' % MENU_IMPLIED])
MENU_COND_COMBINE = '(%s) or (%s)' % (MENU, MENU_EMBEDDED)
MENU_XPATH = '//a[%s][@href]' % MENU_COND_COMBINE
MENU_HREF_XPATH = '%s/@href' % MENU_XPATH

# Get menu-excluded main-content xpath
MENU_EXCLUDED = ' and '.join(list(map(lambda cond: 'not(%s)' % cond, MENU_COND_COMBINE.split(' or '))))

MAIN_CONTENT_NO_MENU_XPATH = MAIN_CONTENT_XPATH + '[%s]' % MENU_EXCLUDED
MAIN_CONTENT_NO_MENU_HREF_XPATH = '%s/@href' % MAIN_CONTENT_NO_MENU_XPATH

TEXT_XPATH = '//*[not(self::script) and not(self::style)]/text()[normalize-space(.)]'
TEXT_MENU_XPATH = 'ancestor::*[count(a)=1]/a[not(descendant::script) ' + \
                  'and not(descendant::style)]/text()[normalize-space(.)]'

# Menu specific element to pass
MENU_TEXT_FILTER = frozenset(['resource', 'event', 'news', 'calendar'])


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


def get_menu(response):
    # Get menu through a more specific method as it is a special case where we
    # want to understand the hierarchy as well
    original_dict = generic_get_anchor_and_text(response, MENU_XPATH, MENU_HREF_XPATH)
    original_dict = {link: text for text, link in original_dict.items()}

    # Select all menu component and the hierarchical text (e.g. About -> Programme -> Undergraduate)
    content_selector = response.xpath(MENU_XPATH)
    content = map(lambda selector: selector.xpath(TEXT_MENU_XPATH).extract(), content_selector)

    # Clean the textual data, which is already tokenized, and join them
    content = map(lambda text_list: list(map(lambda text: ' '.join(text.split()), text_list)), list(content))
    content_text = list(map(lambda text_list: ' '.join(text_list), list(content)))

    # Get the href and zip together
    href = response.xpath(MENU_HREF_XPATH).extract()
    href_temp_list = [[text, link] for text, link in zip(content_text, href) if text.lower() not in MENU_TEXT_FILTER]
    href_dict = {}
    for tuple_pair in href_temp_list:

        # Check whether hierarchical processing works
        if len(tuple_pair[0]) == 0:
            href_dict[original_dict[tuple_pair[1]]] = tuple_pair[1]
        else:
            href_dict[tuple_pair[0]] = tuple_pair[1]

    return href_dict
