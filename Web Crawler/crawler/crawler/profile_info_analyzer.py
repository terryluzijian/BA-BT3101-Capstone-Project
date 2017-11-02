from queue import Queue

def search_string_regex(target_string, regex, len_lim=20, is_filter=False, ignore_case=True):
	if is_filter:
		return re.search(regex, target_string.lower() if ignore_case else target_string)

	if len(target_string.split(' ')) > len_lim:
		return False
	else:
		return re.search(regex, target_string.lower() if ignore_case else target_string)

def search_sublist_regex(target_list, regex, len_lim=20, is_filter=False, ignore_case=True):
	for sub_string in target_list:
		if search_string_regex(sub_string, regex, len_lim, is_filter, ignore_case):
			return True
	return False

phd_year = 'Unknown'
phd_school = 'Unknown'
promote_year = 'Unknown'

CLASS_FILTER_XPATH = '[not(self::nav) and (self::p or self::li or self::ol or self::dl or self::tr or self::span)]'
GROUP_TEXT_XPATH = '//*[descendant-or-self::*[text()]]' + CLASS_FILTER_XPATH
tag_regex = re.compile(r'\<[^>]*\>')
text_list_divided = list(map(lambda x: tag_regex.split(x), response.xpath(GROUP_TEXT_XPATH).extract()))
text_list_divided_filter = list(map(lambda sub_list: list(filter(lambda strings: len(strings.split()) > 0, sub_list)), text_list_divided))
text_list_divided_normalize_map = list(map(lambda sub_list: list(map(lambda strings: ' '.join(strings.split()), sub_list)), text_list_divided_filter))
text_list_pre_final = list(filter(lambda sub_list: len(sub_list) > 0, text_list_divided_normalize_map))

div_text = list(map(lambda html_str: html.fromstring(html_str).xpath('text()'), response.xpath('//div[text()]').extract()))
div_text_filter = list(map(lambda sub_list: list(filter(lambda strings: len(strings.split()) > 0, sub_list)), div_text))
div_normalize_map = list(map(lambda sub_list: list(map(lambda strings: ' '.join(strings.split()), sub_list)), div_text_filter))
div_final = list(filter(lambda sub_list: len(sub_list) > 0, div_normalize_map))
text_list_pre_final.extend(div_final)

phd_regex = r'(ph\.?[ ]?d\.?[ ]?)|(d\.?[ ]?phil\.?[ ]?)'
prof_regex = r'professor'
university_detect_regex = r'(?:university|institute|college|[A-Z][A-Z]{2,4})'
university_get_regex = r'(?:[ ]?([A-Za-z ]*(?:University|Institute|College})[A-Za-z ]*)[ ]?|[A-Z][A-Z]{2,4})'

year_regex = r'(?:19|20)\d{2}'
year_regex_html = r'\/(?:19|20)\d{2}\/'
publication_regex = r'[A-Z][a-z]+(?:, |,| )[A-Z](?:\. |\.| ).*(?:19|20)\d{2}'

phd_string = list(filter(lambda sub_list: search_sublist_regex(sub_list, phd_regex), text_list_pre_final))
phd_school = list(filter(lambda sub_list: search_sublist_regex(sub_list, university_detect_regex), phd_string))
phd_year = list(filter(lambda sub_list: search_sublist_regex(sub_list, year_regex) and not search_sublist_regex(sub_list, year_regex_html), phd_school))

prof_string = list(filter(lambda sub_list: search_sublist_regex(sub_list, prof_regex) and not search_sublist_regex(sub_list, publication_regex, ignore_case=False), text_list_pre_final))
prof_year = list(filter(lambda sub_list: search_sublist_regex(sub_list, year_regex) and not search_sublist_regex(sub_list, year_regex_html), prof_string))

