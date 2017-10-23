def get_the_closest(target, get_func, nlp_ratio=0.7):
    ...:
    ...:

    def get_similarity(string1, string2, nlp_ratio):
        ...: string1_nlp = nlp(string1)

    ...: string2_nlp = nlp(string2)
    ...: s = SequenceMatcher(None, string1, string2)
    ...:
    return [string1_nlp.similarity(string2_nlp) * nlp_ratio + s.ratio() * (1 - nlp_ratio)]
    ...:
    ...: menu = [[b, a] for a, b in get_func(response).items()]
    ...: menu_with_similarity = []
    ...:
    for item in menu:
        ...: menu_with_similarity.append(item + get_similarity(item[0].lower(), target, nlp_ratio=nlp_ratio))
    ...:
    return sorted(menu_with_similarity, key=lambda x: x[2], reverse=True)