import os.path
import csv
import urllib.parse
from collections import namedtuple, defaultdict
from itertools import zip_longest

import bs4
import requests
import patterns
import oauth
import util

# Parameters for this script

report_dirname = '/Users/kkolmar/common/iorj/report2022/'
data_start_date = '2021-12-01'
data_end_date = '2022-12-01'

interesting_issues = ['2021-16-4', '2022-17-1', '2022-17-2', '2022-17-3']
ignore_issues = {('2021-16-3', 'en')}

dirpath = report_dirname
archive_dir = os.path.join(dirpath, 'archive')
issues_dir = os.path.join(archive_dir, 'issues')
util.ensure_directory(issues_dir)

# Формирование файла rawdownloads.txt:
# https://metrika.yandex.ru/list
# Отчеты - Стандартные отчеты - Содержание - Загрузки файлов
# страница загрузки - хост - iorj.hse.ru; загрузка файла - хост - iorj.hse.ru
# группировка по страница загрузки - путь
raw_downloads_path = os.path.join(dirpath, 'rawdownloads.txt')

# Данные приложения IORJReport:
# ID: 12f41230e56a42a78d25afefbfdf93b5
# Пароль: 98b2e0efb06a4cb08baedafeb82d93e1
# Callback URL: https://oauth.yandex.ru/verification_code

# Получение токена:
# https://oauth.yandex.ru/authorize?response_type=token&client_id=12f41230e56a42a78d25afefbfdf93b5


Article = namedtuple('Article', ['id', 'title', 'authors', 'url', 'issue', 'language'])

Issue = namedtuple('Issue', ['number', 'language', 'views', 'visitors', 'article_data'])

Download = namedtuple('Download', ['id', 'count'])

ArticleData = namedtuple('ArticleData', ['id', 'language', 'title', 'authors', 'url', 'issue', 'views', 'visitors', 'downloads'])

DomainFilter = "ym:pv:URLDomain=='iorj.hse.ru'"
UserFromRussiaFilter = "ym:pv:regionCountryIsoName=='RU'"
HumanFilter = "ym:s:isRobot=='No'"

RussianPageFilter = "ym:pv:URLPath!*'/en/*'"
EnglishPageFilter = "ym:pv:URLPath=*'/en/*'"
def page_language_filter(language):
    return EnglishPageFilter if language == 'en' else RussianPageFilter


def default_query():
    return {'ids': '32635220',
            'accuracy': 'high',
            'limit': '50',
            'oauth_token': oauth.token,
            'date1': data_start_date,
            'date2': data_end_date,
            'filters': f'{DomainFilter} AND {HumanFilter}',
            'metrics': 'ym:pv:pageviews,ym:pv:users',
            'sort': '-ym:pv:users'}


def default_query_with(**diff):
    return combine_queries(default_query(), diff)


def combine_queries(*query_parts):
    result = {}

    for query in query_parts:
        for key, value in query.items():
            if callable(value):
                original = result.get(key, '')
                result[key] = value(original)
            else:
                result[key] = str(value)

    return result


def execute_query(*query_parts):
    headers = {'Authorization': f'OAuth {oauth.token}'}
    query = combine_queries(*query_parts)

    def make_url(query_dict):
        scheme = 'https'
        netloc = 'api-metrika.yandex.ru'
        path = '/stat/v1/data.json'

        return urllib.parse.urlunsplit((scheme, netloc, path, urllib.parse.urlencode(query_dict), ''))

    return requests.get(make_url(query), headers=headers).json()


def process_request(query, keys):
    print(f"Processing query with {query['filters']}")

    if isinstance(keys, str):
        keys = [keys]

    sorted_result_keys = []
    dependant_dimensions = {}
    result = defaultdict(list)

    def execute_with_period(date1, date2):
        print(f'Executing for period {(date1, date2)}')

        query_result = execute_query(query, {'date1': date1, 'date2': date2})

        if 'data' in query_result:
            for data in query_result['data']:
                if not keys:
                    key, other_dimensions = '', []
                else:
                    key, *other_dimensions = [data['dimensions'][0][name] for name in keys]

                if key not in result:
                    sorted_result_keys.append(key)
                    dependant_dimensions[key] = other_dimensions

                metrics = [int(item) for item in data['metrics']]
                result[key] = [a + b for a, b in zip_longest(result[key], metrics, fillvalue=0)]
        else:
            error_types = {error['error_type'] for error in query_result['errors']}

            # 'message': 'Запрос слишком сложный. Пожалуйста, уменьшите интервал дат или семплирование.'
            if 'query_error' in error_types:
                period1, period2 = util.split_period(date1, date2)
                print(f'Failed for period {(date1, date2)}. Retrying with {period1} and {period2}')
                execute_with_period(*period1)
                execute_with_period(*period2)
            else:
                print('query:', query)
                print('result:', query_result)
                raise KeyError('data is missing')

    execute_with_period(data_start_date, data_end_date)
    return [tuple([key] + dependant_dimensions[key] + result[key]) for key in sorted_result_keys]


def combine_datasets(data_list_1, data_list_2):
    data1 = {key: values for key, *values in data_list_1}
    data2 = {key: values for key, *values in data_list_2}

    return sorted(
        [
            [key] + data1.get(key, [0, 0]) + data2.get(key, [0, 0])
            for key in set(data1) | set(data2)
        ],
        key=lambda item: item[1:],
        reverse=True)


def views_by_country(language):
    print(f'Querying by country for {language}')
    query = default_query_with(
        dimensions='ym:pv:regionCountry',
        filters=lambda filters: f'{filters} AND {page_language_filter(language)}',
        limit=300)

    return process_request(query, 'name')


def views_by_city(language):
    print(f'Querying by city for {language}')
    query = default_query_with(
        dimensions='ym:pv:regionCity',
        filters=lambda filters: f'{filters} AND {page_language_filter(language)}')

    return process_request(query, 'name')


def language_suffix(language):
    return '-en' if language == 'en' else ''


def views_of_issues(issues):
    issue_articles = {issue: {} for issue in issues}

    query = default_query_with(
        dimensions='ym:pv:URLPath',
        limit=5000)

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
        print('Processing issue', issue_name)
        soup = bs4.BeautifulSoup(f.read(), 'html5lib')
        soup = soup.find('table', class_='issue_type2_maintable').find_all('td', class_='link')
        soup = [td.find('div', recursive=False) for td in soup]

        return [parse_item(item) for item in soup]


def parse_raw_downloads(filepath) -> {str: Download}:
    def grouper(iterable, n, fillvalue=None):
        args = [iter(iterable)] * n
        return zip_longest(fillvalue=fillvalue, *args)

    articles = {}

    with open(filepath, 'r') as f:
        for url, _, _, _, n in grouper(f, 5, ''):
            path = urllib.parse.urlsplit(url.strip()).path
            match = patterns.match_path(path)
            if match and match.id:
                article_id = match.id + language_suffix(match.language)
                articles.setdefault(article_id, Download(match.id, 0))
                articles[article_id] = Download(match.id, articles[article_id].count + int(n))

    return articles


def ensure_issue_file(issue_number, language):
    issue_file = issue_number + language_suffix(language)

    if not os.path.exists(issue_file):
        issue_url = f"https://iorj.hse.ru{'/en' if language == 'en' else ''}/{issue_number}.html"

        issue_content = requests.get(issue_url).text

        with open(os.path.join(issues_dir, issue_file), 'w', encoding='utf-8') as file:
            file.write(issue_content)

    return issue_file


def gather_issue_data(issues_numbers, raw_downloads_path):
    downloads = parse_raw_downloads(raw_downloads_path)
    issue_views = views_of_issues(issues_numbers)

    issues = []
    for issue in issues_numbers:
        for language in ['ru', 'en']:
            if (issue, language) in ignore_issues:
                continue

            issue_file = ensure_issue_file(issue, language)

            articles = []
            for article in parse_issue_file(issue_file):
                assert(article.language == language)

                article_key = article.id + language_suffix(article.language)
                download_count = downloads[article_key].count if article_key in downloads else 0
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


def read_csv(filename):
    with open(os.path.join(dirpath, filename + '.csv'), 'r', encoding='utf-8') as file:
        return list(csv.reader(file, delimiter=';'))


def write_csv(filename, rows):
    with open(os.path.join(dirpath, filename + '.csv'), 'w', encoding='utf-8') as file:
        writer = csv.writer(file, delimiter=';')
        writer.writerows(rows)


def round_to_10(dataset):
    return [row[:1] + [round(int(item), -1) for item in row[1:]] for row in dataset]


def write_user_reports():
    datasets = {}

    languages = ['ru', 'en']
    dataset_builders = [views_by_country, views_by_city]

    for language in languages:
        for builder in dataset_builders:
            datasets[f'{builder.__name__}_{language}'] = builder(language)

    for builder in dataset_builders:
        name = builder.__name__
        datasets[name] = combine_datasets(*[datasets[f'{name}_{language}'] for language in languages])

    for name, values in datasets.items():
        write_csv(name, values)


def write_issue_report(issues=interesting_issues):
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
                '–' if downloads < 0 else str(downloads)))

    write_csv('article_data', article_data)


def basic_stats():
    _, ru_views, ru_visits = process_request(default_query_with(filters=lambda filters: f'{filters} AND {RussianPageFilter}'), [])[0]
    _, en_views, en_visits = process_request(default_query_with(filters=lambda filters: f'{filters} AND {EnglishPageFilter}'), [])[0]

    downloads = parse_raw_downloads(raw_downloads_path)
    ru_downloads = sum(download.count for (id, download) in downloads.items() if not id.endswith('en'))
    en_downloads = sum(download.count for (id, download) in downloads.items() if id.endswith('en'))

    print('Russian page (views, visits, downloads):', int(ru_views), int(ru_visits), ru_downloads)
    print('English page (views, visits, downloads):', int(en_views), int(en_visits), en_downloads)


def main():
    basic_stats()
    write_user_reports()
    write_issue_report()


if __name__ == '__main__':
    main()
