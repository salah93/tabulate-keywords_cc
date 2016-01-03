import datetime
import requests
from bs4 import BeautifulSoup
from invisibleroads_macros.text import compact_whitespace
from os import makedirs
from os.path import join, exists


class ToolError(Exception):
    pass

def get_expression(journal_name=None, text_terms=None, mesh_terms=None,
        from_date=None, to_date=None, custom_expression=None, author_name=None):
    '''
        Retrieve expression based on inputs.
        Expressions are constructed in this layout:
        
        ("%s"[Journal]) AND ("%s"[Text Word] OR 
        "%s"[Text Word] OR "%s"[MeSH Terms] OR 
            "%s"[MeSH Terms]) AND 
            ("%s"[Date - Publication] : "%s"[Date - Publication])
    
        author: Roy
    '''
    expression_parts = []
    if journal_name:
        expression_parts.append('"%s"[Journal]' % journal_name)
    if author_name:
        expression_parts.append('%s[Author]' % author_name)
    if custom_expression:
        expression_parts.append(custom_expression)
    if text_terms or mesh_terms:
        terms = []
        terms.extend('"%s"[Text Word]' % x for x in text_terms or [])
        terms.extend('"%s"[MeSH Terms]' % x for x in mesh_terms or [])
        expression_parts.append(' OR '.join(terms))
    if from_date:
        from_date_string = from_date.strftime(
            '%Y/%m/%d')
        to_date_string = to_date.strftime(
            '%Y/%m/%d') if to_date else '3000'
        expression_parts.append(
            '"%s"[Date - Publication] : "%s"[Date - Publication]' % (
                from_date_string, to_date_string))
    if len(expression_parts) <= 1:
        expression = ''.join(expression_parts)
    else:
        expression = '(%s)' % ') AND ('.join(expression_parts)
    return compact_whitespace(expression)

def get_search_count(expression):
    '''
        Retrieves search count from page requested by url+expression
    '''
    url = 'http://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&'
    response = requests.get(url, params=dict(term=expression))
    soup = BeautifulSoup(response.text, 'xml')
    return int(soup.find('Count').next_element)

def get_date_ranges(from_date, to_date, interval_in_years):
    '''
        Retrieve pairs of date ranges based on interval number
    '''

    if interval_in_years:
        if from_date and to_date and from_date > to_date:
            raise ToolError('Invalid date range')
        date_ranges = []
        date_b = from_date - datetime.timedelta(days=1)
        while date_b < to_date:
            date_a = date_b + datetime.timedelta(days=1)
            date_b = datetime.datetime(
                date_a.year + interval_in_years, date_a.month, date_a.day,
            ) - datetime.timedelta(days=1)
            if date_b > to_date:
                date_b = to_date
            date_ranges.append((date_a, date_b))
        return date_ranges
    else:
        return [(from_date,to_date)]

def parse_list(list_):
    for i,n in enumerate(list_):
        n = n.replace(',','')
        n = n.replace(';','')
        list_[i] = n
    while '' in list_:
        list_.remove('')

def load_unique_lines(source_path):
    source_text = open(source_path, 'rt').read().strip()
    return sorted(set([x.strip() for x in source_text.splitlines()]))

def input_retrieval(path):
    if path:
        text_list = load_unique_lines(path)
        parse_list(text_list)
        return text_list
    else:
        return []

def parse_date(string):
    return datetime.datetime.strptime(string, '%m-%d-%Y')

def mkdir_local(path):
    if not exists(path):
        makedirs(path)
    return path
