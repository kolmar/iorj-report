import itertools
import os.path
import csv
import urllib.parse
from collections import namedtuple

import bs4
import requests
import patterns
import oauth


# 1)     Количество просмотров, уникальных посетителей, скачиваний текстов отдельно для русской и английской версии сайта (в целом, без разбора отдельных страниц)
#
# просмотры, посетители рус:
# filters=ym:pv:URLDomain=='iorj.hse.ru' AND ym:pv:URLPath!*'/en/*'&metrics=ym:pv:pageviews,ym:pv:users&sort=-ym:pv:pageviews
# 63017.0, 14213.0
#
# просмотры посетители англ:
# filters=ym:pv:URLDomain=='iorj.hse.ru' AND ym:pv:URLPath=*'/en/*'&metrics=ym:pv:pageviews,ym:pv:users&sort=-ym:pv:pageviews
# 14979.0, 4616.0
#
# скачиваний статей/журналов англ:
# страница загрузки - хост - iorj.hse.ru; загрузка файла - хост - iorj.hse.ru; страница загрузки - путь - /en/*
# 3005, 1508 (скачиваний, уников)
#
# скачиваний статей/журналов англ:
# страница загрузки - хост - iorj.hse.ru; загрузка файла - хост - iorj.hse.ru; страница загрузки - путь - !/en/*
# вручную учесть закачки не статей и текстов
# 8322, 3865

dirpath = '/Users/kkolmar/common/iorj/report2017/'
archive_dir = os.path.join(dirpath, 'archive')
issues_dir = os.path.join(archive_dir, 'issues')

# Формирование файла rawdownloads.txt:
# страница загрузки - хост - iorj.hse.ru; загрузка файла - хост - iorj.hse.ru
# группировка по страница загрузки - путь
raw_downloads_path = os.path.join(dirpath, 'rawdownloads.txt')

# Данные приложения IORJReport:
# ID: 12f41230e56a42a78d25afefbfdf93b5
# Callback URL: https://oauth.yandex.ru/verification_code


Article = namedtuple('Article', ['id', 'title', 'authors', 'url', 'issue', 'language'])

Issue = namedtuple('Issue', ['number', 'language', 'views', 'visitors', 'article_data'])

Download = namedtuple('Download', ['id', 'count'])

ArticleData = namedtuple('ArticleData', ['id', 'language', 'title', 'authors', 'url', 'issue', 'views', 'visitors', 'downloads'])


def query_dict():
    return {'ids': '32635220',
            'accuracy': 'full',
            'limit': '50',
            'oauth_token': oauth.token,
            'date1': '2016-11-13',
            'date2': '2017-11-14',
            'filters': "ym:pv:URLDomain=='iorj.hse.ru'",
            'metrics': 'ym:pv:pageviews,ym:pv:users',
            'sort': '-ym:pv:pageviews'}


def make_url(query_dict):
    scheme = 'https'
    netloc = 'api-metrika.yandex.ru'
    path = '/stat/v1/data.json'

    return urllib.parse.urlunsplit((scheme, netloc, path, urllib.parse.urlencode(query_dict), ''))


def process_request(query, grouping_names):
    if isinstance(grouping_names, str):
        grouping_names = [grouping_names]

    result = requests.get(make_url(query)).json()

    def dimensions_data(data):
        return [data['dimensions'][0][name] for name in grouping_names]

    def metrics_data(data):
        return [int(item) for item in data['metrics']]

    return [tuple(dimensions_data(data) + metrics_data(data)) for data in result['data']]


def views_by_country():
    query = query_dict()
    query['dimensions'] = 'ym:pv:regionCountry'
    query['limit'] = 300

    return process_request(query, ['name', 'iso_name'])


def views_by_city():
    query = query_dict()
    query['dimensions'] = 'ym:pv:regionCity'
    # query['filters'] += " AND ym:pv:regionCountryIsoName=.('RU', 'UA', 'KZ', 'BY')"
    query['filters'] += " AND ym:pv:regionCountryIsoName=='RU'"

    return process_request(query, 'name')


def language_suffix(language):
    return '-en' if language == 'en' else ''


def views_of_issues(issues):
    issue_articles = {issue: {} for issue in issues}

    query = query_dict()
    query['dimensions'] = 'ym:pv:URLPath'
    query['limit'] = 5000

    result = process_request(query, 'name')

    for page in result:
        match = patterns.match_path(page[0])
        if match and match.issue in issue_articles:
            key = match.id + language_suffix(match.language) if match.id else match.language
            issue_articles[match.issue][key] = page[1:]

    return issue_articles


def get_id_from_url(url):
    return os.path.splitext(os.path.basename(urllib.parse.urlsplit(url).path))[0]


def parse_issue_file(issue_name):
    def parse_item(item):
        authors = [author.text.strip() for author in item.find_all('i')]
        title_node = item.find('span', class_='article_title')
        title = title_node.text.strip()
        if not title:
            title = item.find('p', class_='text').text.strip()
        url = title_node.parent.get('href')
        language = 'en' if '/en/' in url else 'ru'
        id = get_id_from_url(url)
        return Article(id, title, authors, url, issue_name, language)

    with open(os.path.join(issues_dir, issue_name), 'r') as f:
        soup = bs4.BeautifulSoup(f.read(), "html5lib")
        soup = soup.find('table', class_='issue_type2_maintable').find_all('div', class_='link')

        return [parse_item(item) for item in soup]


def parse_raw_downloads(filepath) -> {str: Download}:
    def grouper(iterable, n, fillvalue=None):
        args = [iter(iterable)] * n
        return itertools.zip_longest(fillvalue=fillvalue, *args)

    articles = {}

    with open(filepath, 'r') as f:
        for url, _, _, _, n in grouper(f, 5, ""):
            n = int(n)
            if n < 3: n = -1

            path = urllib.parse.urlsplit(url.strip()).path
            match = patterns.match_path(path)
            if match and match.id:
                articles[match.id + language_suffix(match.language)] = Download(match.id, int(n))

    return articles


def gather_issue_data(issues_numbers, raw_downloads_path):
    downloads = parse_raw_downloads(raw_downloads_path)
    issue_views = views_of_issues(issues_numbers)

    issues = []
    for issue in issues_numbers:
        for language in ['ru', 'en']:
            issue_file = issue + language_suffix(language)

            articles = []
            for article in parse_issue_file(issue_file):
                assert(article.language == language)

                article_key = article.id + language_suffix(article.language)
                download_count = downloads[article_key].count if article_key in downloads else -1
                views, visitors = issue_views[issue].get(article_key, (0, 0))
                articles.append(ArticleData(
                    downloads=download_count,
                    views=views,
                    visitors=visitors,
                    **article._asdict()))
            articles.sort(key=lambda article: article.views, reverse=True)

            views, visitors = issue_views[issue][language]
            issues.append(Issue(issue, language, views, visitors, articles))

    return issues


def write_csv(filename, rows):
    with open(os.path.join(dirpath, filename + '.csv'), 'w', encoding='cp1251') as file:
        writer = csv.writer(file, delimiter=';')
        writer.writerows(rows)


def write_user_reports():
    write_csv('views_by_country', [[name] + metrics for name, _, *metrics in views_by_country()])
    write_csv('views_by_city', views_by_city())


def write_issue_report(issues):
    article_data = []
    for number, language, views, visitors, articles in gather_issue_data(issues, raw_downloads_path):
        year, _, no = number.split('-')
        issue_string = year + ' #' + no

        article_data.append((issue_string, language, None, None, views, visitors, None))

        for id, _, title, authors, _, issue, views, visitors, downloads in articles:
            article_data.append((
                issue_string,
                language,
                title,
                ', '.join(authors),
                views,
                visitors,
                '-' if downloads < 0 else str(downloads)))

    write_csv('article_data', article_data)


def main():
    write_user_reports()

    interesting_issues = ['2016-11-4', '2017-12-1', '2017-12-2', '2017-12-3']
    write_issue_report(interesting_issues)


if __name__ == '__main__':
    main()