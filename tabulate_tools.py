import requests
from bs4 import BeautifulSoup
from invisibleroads_macros.text import compact_whitespace


def get_expression(
        journal_name=None, text_terms=None, mesh_terms=None,
        from_date=None, to_date=None, custom_expression=None,
        author_name=None):
    """
    Retrieve expression based on inputs.
    Expressions are constructed in this layout:

    ("%s"[Journal]) AND ("%s"[Text Word] OR
    "%s"[Text Word] OR "%s"[MeSH Terms] OR
        "%s"[MeSH Terms]) AND
        ("%s"[Date - Publication] : "%s"[Date - Publication])
    """
    expression_parts = []
    if journal_name:
        expression_parts.append('"{0}"[Journal]'.format(journal_name))
    if author_name:
        expression_parts.append('{0}[Author]'.format(author_name))
    if custom_expression:
        expression_parts.append(custom_expression)
    if text_terms or mesh_terms:
        terms = []
        terms.extend('"{0}"[Text Word]'.format(x) for x in text_terms or [])
        terms.extend('"{0}"[MeSH Terms]'.format(x) for x in mesh_terms or [])
        expression_parts.append(' OR '.join(terms))
    if from_date:
        from_date_string = from_date.strftime(
            '%Y/%m/%d')
        to_date_string = to_date.strftime(
            '%Y/%m/%d') if to_date else '3000'
        expression_parts.append(
            '"{0}"[Date - Publication] : "{1}"[Date - Publication]'.format(
                from_date_string, to_date_string))
    if len(expression_parts) <= 1:
        expression = ''.join(expression_parts)
    else:
        expression = '({0})'.format(') AND ('.join(expression_parts))
    return compact_whitespace(expression)


def get_search_count(expression, retstart=0):
    """
    Retrieve search count from page requested by url+expression
    """
    url = 'http://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi'
    # max num of articles to list
    retmax = 1000
    params = {'db': 'pubmed', 'term': expression,
              'retmax': str(retmax), 'retstart': str(retstart)}
    response = requests.get(url, params=params)
    soup = BeautifulSoup(response.text, 'xml')
    count = int(soup.find('Count').next_element)
    articles_list = [str(article.next_element) for
                     article in soup.find('IdList').find_all('Id')]
    if count > (retmax + retstart):
        articles_list.extend(get_search_count(expression, retstart+retmax)[1])
    return (count, articles_list)


def get_first_name_articles(author, articles):
    first_named_articles = []
    # articles = list(set(articles))
    translated_name = translate_name(author)
    url = 'http://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi'
    articles_param = ','.join(articles)
    params = {'db': 'pubmed', 'id': articles_param}
    response = requests.get(url, params=params)
    soup = BeautifulSoup(response.text, 'xml')
    for article, article_info in zip(articles, soup.find_all('DocSum')):
        auth = article_info.find(
                "Item", attrs={"Name": "AuthorList"}).findChild().next_element
        if auth.lower() == translated_name:
            first_named_articles.append(article)
    return first_named_articles


def translate_name(name):
    first_middle_last = 3
    parts_of_name = name.split(' ')
    translated_name = parts_of_name[-1] + ' ' + parts_of_name[0][0]
    if len(parts_of_name) == first_middle_last:
        translated_name += parts_of_name[1][0]
    return translated_name.lower()
