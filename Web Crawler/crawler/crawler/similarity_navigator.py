import spacy
from crawler.xpath_generic_extractor import get_menu
from difflib import SequenceMatcher


class SimilarityNavigator(object):

    # This class is intended to help navigation through menu content of a web page

    # Assumptions made here include that, regardless of the nature of the school page
    # being a department page or a generic faculty page, we could get the next-level
    # information by solely navigating through menu

    # The target words include (by descending importance) people, faculty staff, department,
    # academic units, research etc. WE solely get either department content or staff
    # directories

    DEPARTMENT_TARGET = frozenset(['department', 'school', 'area of study'])
    PEOPLE_TARGET = frozenset(['people', 'faculty', 'staff directory'])

    def __init__(self):
        self.model_en = spacy.load('en')

    def get_content(self, response, target, extract_func, ratio=0.8, top=3):
        # Use the extraction inherited from crawler.xpath_generic_extractor
        menu = [[key, value] for key, value in extract_func(response).items()]
        menu_with_similarity = []

        # Iterate through each item pair and obtain the similarity with the target keyword
        for item_pair in menu:
            menu_with_similarity.append(item_pair + self.get_similarity(first_string=item_pair[0],
                                                                        second_string=target,
                                                                        ratio=ratio))

        # Return the sorted list and by default the top three content
        return sorted(menu_with_similarity, key=lambda x: x[2], reverse=True)[:top]

    def get_similarity(self, first_string, second_string, ratio=0.8):
        # Get the similarities between two strings and weigh by the given ratio
        # Get the semantic similarity between two string
        first_string_nlp = self.model_en(first_string.lower())
        second_string_nlp = self.model_en(second_string.lower())

        # Get the sequential similarity between two strings, i.e. how many characters are shared
        s = SequenceMatcher(None, first_string.lower(), second_string.lower())

        # Output a weighted similarity metric by the given ratio
        return [first_string_nlp.similarity(second_string_nlp) * ratio + s.ratio() * (1 - ratio)]

    def get_target_content(self, response, ratio=0.8, top_from_each=2, threshold=0.7):
        # Iterate through the target lists and get the most similar contents
        combined_list = list(self.DEPARTMENT_TARGET) + list(self.PEOPLE_TARGET)
        target_list = []
        for target_word in combined_list:
            target_list += self.get_content(response=response, target=target_word, extract_func=get_menu,
                                            ratio=ratio,
                                            top=top_from_each)

        # Output the sorted target list within the specified threshold
        sorted_list = sorted(target_list, key=lambda x: x[2], reverse=True)
        return [element_pair for element_pair in sorted_list if element_pair[2] >= threshold]
