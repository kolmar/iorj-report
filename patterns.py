import re

from collections import namedtuple

ArticleMatch = namedtuple('ArticleMatch', ['issue', 'id', 'language'])

_pattern = re.compile('''(?P<lang>/en)?/(?P<issue>\d{4}-\d*-\d+)(?:\.html|/(?P<id>\d+)[^\d]*)?''')


def match_path(url_path):
    match = _pattern.fullmatch(url_path)

    if match is not None:
        issue = match.group('issue')
        id = match.group('id')
        language = 'en' if match.group('lang') else 'ru'

        return ArticleMatch(issue, id, language)
