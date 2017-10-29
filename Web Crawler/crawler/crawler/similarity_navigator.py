import re
import spacy
import sys
from difflib import SequenceMatcher
from .xpath_generic_extractor import get_menu, get_general


class SimilarityNavigator(object):

    # This class is intended to help navigation through menu content of a web page

    # Assumptions made here include that, regardless of the nature of the school page
    # being a department page or a generic faculty page, we could get the next-level
    # information by solely navigating through menu

    # The target words include (by descending importance) people, faculty staff, department,
    # academic units, research etc. WE solely get either department content or staff
    # directories

    DEPARTMENT_TARGET = frozenset(['area of study', 'department', 'department of', 'school', 'school of',
                                   'academic unit', 'school & department', 'major and minor', 'major',
                                   'faculty & department', 'academics', 'departments and programs'])
    PEOPLE_TARGET = frozenset(['academic staff', 'faculty', 'faculty staff', 'faculty people',
                               'faculty directory', 'our people', 'people', 'staff',
                               'staff directory', 'teaching staff', 'emeritus'])
    SEQUENTIAL_THRESHOLD = 0.8

    def __init__(self):
        self.model_en = spacy.load('en_core_web_md')

    def get_content(self, response, target, target_class, extract_func, ratio=0.8, top=3):

        def normalize_word(target_string):
            if target_string.lower() not in self.model_en.vocab:
                string_replace = re.sub(r'[A-Z][A-Z]+', '', target_string)
                content_normalized = filter(lambda x: re.search(r'[A-Za-z]+', x), string_replace.split())
                return ' '.join(list(map(lambda x: str(x), content_normalized)))
            else:
                return target_string

        # Use the extraction inherited from crawler.xpath_generic_extractor
        menu = [[normalize_word(key), value] for key, value in extract_func(response).items()]
        menu_with_similarity = []

        # Iterate through each item pair and obtain the similarity with the target keyword
        for item_pair in menu:
            menu_with_similarity.append(item_pair + self.get_similarity(first_string=item_pair[0],
                                                                        second_string=target,
                                                                        ratio=ratio) + [target_class])

        # Return the sorted list and by default the top three content
        return sorted(menu_with_similarity, key=lambda x: x[2], reverse=True)[:top]

    def get_similarity(self, first_string, second_string, ratio=0.8):
        # Get the similarities between two strings and weigh by the given ratio
        # Get the semantic similarity between two string
        first_string_nlp = self.model_en(first_string.lower())
        second_string_nlp = self.model_en(second_string.lower())

        # Get the sequential similarity between two strings, i.e. how many characters are shared
        s = SequenceMatcher(None, first_string.lower(), second_string.lower())
        if s.ratio() >= self.SEQUENTIAL_THRESHOLD:
            # If the sequence similarity is extremely high, stop before calculating semantic similarity
            return [s.ratio()]

        # Output a weighted similarity metric by the given ratio
        return [first_string_nlp.similarity(second_string_nlp) * ratio + s.ratio() * (1 - ratio)]

    def get_target_content(self, response,
                           parse_only_people=False, parse_only_department=False,
                           fall_back_to_general=True, extract_func=get_menu,
                           ratio=0.8, top_from_each=3, threshold=0.7):
        # Iterate through the target lists and get the most similar contents
        combined_list = {
            'DEPARTMENT': list(self.DEPARTMENT_TARGET),
            'PEOPLE': list(self.PEOPLE_TARGET)
        }
        if parse_only_department:
            del combined_list['PEOPLE']
        if parse_only_people:
            del combined_list['DEPARTMENT']

        target_list = []
        for target_class, target_words in combined_list.items():
            for target_word in target_words:
                target_list += self.get_content(response=response,
                                                target=target_word, target_class=target_class,
                                                extract_func=extract_func,
                                                ratio=ratio,
                                                top=top_from_each)

        # Output the sorted target list within the specified threshold
        sorted_list = sorted(target_list, key=lambda x: x[2], reverse=True)
        result_list = []
        link_set = set()
        for element_pair in sorted_list:

            # Remove redundant link element
            if (element_pair[1] not in link_set) & (element_pair[2] >= threshold):
                result_list.append(element_pair)
            link_set.add(element_pair[1])

        # Fallback to general href crawling and try again
        if fall_back_to_general:
            if (len(result_list) == 0) and (extract_func != get_general):
                sys.stdout.write('Returning empty result for response %s and falling back to general crawl' % response)
                sys.stdout.write('\n')
                return self.get_target_content(response,
                                               parse_only_people=parse_only_people,
                                               parse_only_department=parse_only_department,
                                               fall_back_to_general=False, extract_func=get_general,
                                               ratio=ratio, top_from_each=top_from_each, threshold=threshold)

        return result_list
