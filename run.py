"""
Count PubMed search results for specified queries:

journals -> Science, Nature
keywords -> poverty, income
MeSh -> social class, socioeconomic factors

("Nature"[Journal]) AND ("income"[Text Word] OR
    "poverty"[Text Word] OR "social class"[MeSH Terms] OR
        "socioeconomic factors"[MeSH Terms])

("Science"[Journal]) AND ("income"[Text Word] OR
    "poverty"[Text Word] OR "social class"[MeSH Terms] OR
        "socioeconomic factors"[MeSH Terms])
"""
import collections
import sqlite3

from argparse import ArgumentParser
from contextlib import contextmanager
from datetime import datetime
from dateutil.parser import parse as parse_date
from invisibleroads_macros.disk import make_folder
from os.path import join
from pandas import DataFrame

from tabulate_tools import (
    get_expression, get_search_count,
    get_first_name_articles)
from load_lines import get_date_ranges, load_unique_lines, ToolError


@contextmanager
def query():
    """
    Creates a cursor from a database functions can access
    using 'with' statement
    """
    db = './queries.db'
    conn = sqlite3.connect(db)
    cursor = conn.cursor()
    yield cursor
    conn.commit()
    conn.close()


def run(
        target_folder, journals_path=None, authors_path=None,
        keywords_path=None, mesh_terms_path=None,
        from_date=None, to_date=None, date_interval_in_years=0):
    target_path = join(target_folder, 'results.csv')
    image_path = join(target_folder, 'keyword_article_count.jpg')
    log_path = join(target_folder, 'log_results.txt')

    # Input retrieval
    journals = load_unique_lines(journals_path)
    text_words = load_unique_lines(keywords_path)
    mesh_terms = load_unique_lines(mesh_terms_path)
    authors = load_unique_lines(authors_path)

    # Get list containing intervals of date ranges based on interval specified
    #  by user
    try:
        date_ranges = get_date_ranges(
            from_date, to_date, date_interval_in_years)
    except ToolError as e:
        exit('to_date.error = {0}'.format(e))

    isAuthor, query_list = ((True, authors) if authors
                                  else (False, journals))
    # Tabulate keywords
    results = tabulate(
        query_list, date_ranges, text_words, mesh_terms, isAuthor)


def tabulate(query_list, date_ranges, text_words, mesh_terms, isAuthor):
    # O(n*y) for n=len(query_list) and y=len(date_ranges)
    author_articles = collections.defaultdict(list)
    sc = []
    if query_list:
        for from_date, to_date in date_ranges:
            for item in query_list:
                query_param = ({'author_name': item} if isAuthor
                               else {'journal_name': item})
                # Query totals (w/o keywords)
                item_expression = get_expression(
                    from_date=from_date, to_date=to_date, **query_param)
                expression = get_expression(
                    text_terms=text_words,
                    mesh_terms=mesh_terms,
                    from_date=from_date, to_date=to_date,
                    **query_param)
                with query() as cursor:
                    item_count = cursor.execute("""SELECT count from count
                                                where query = ?""",
                                                (item_expression,)).fetchone()
                    articles = cursor.execute("""SELECT article from articles
                                              where query = ?""",
                                              (item_expression,)).fetchall()
                if not item_count:
                    item_count, a = get_search_count(item_expression)
                    with query() as cursor:
                        cursor.execute("""INSERT INTO count(query, count)
                                       values(?, ?)""",
                                       (item_expression, item_count))
                else:
                    item_count = item_count[0]
                if not articles and isAuthor:
                    articles = a
                    print("inside loop")
                    insert_articles = [(item_expression, article) 
                                       for article in articles]
                    with query() as cursor:
                        cursor.executemany("""INSERT INTO
                                         articles(query, article)
                                         values(?, ?)""", insert_articles)
                    author_articles[item].extend(articles)
                # Get search count data for each Query (w/ keywords)
                with query() as cursor:
                    count = cursor.execute("""SELECT count from count
                                           where query=?""",
                                           (expression,)).fetchone()
                if not count:
                    count, _ = get_search_count(expression)
                    with query() as cursor:
                        cursor.execute("""INSERT INTO count(query, count)
                                       values(?, ?)""", (expression, count))
                else:
                    count = count[0]
                sc.append((from_date, to_date, item, item_count, count))
    else:
        for from_date, to_date in date_ranges:
            expression = get_expression(
                text_terms=text_words, mesh_terms=mesh_terms,
                from_date=from_date, to_date=to_date)
            with query() as cursor:
                count = cursor.execute("""SELECT count from count
                                       where query = ?""",
                                       (expression,)).fetchone()
            if not count:
                count, _ = get_search_count(expression)
                with query() as cursor:
                    cursor.execute("""INSERT INTO count(query, count)
                                   values(?, ?)""", (expression, count))
            else:
                count = count[0]
            sc.append((from_date, to_date, count))
    return dict(
        search_counts=sc,
        author_articles=author_articles)


def get_first_name(authors, author_articles):
    first_name_articles = [get_first_name_articles(
                                name, author_articles[name])
                           for name in authors]
    log = []
    for name, articles in zip(authors, first_name_articles):
        log.append('\n' + name + ': ')
        for article in articles:
            log.append('\t' + article)
    table_data = list(zip(authors, [len(article_list)
                               for article_list in first_name_articles]))

if __name__ == '__main__':
    argument_parser = ArgumentParser()
    argument_parser.add_argument(
        '--target_folder', nargs='?', default='results',
        type=make_folder, metavar='FOLDER')
    group = argument_parser.add_mutually_exclusive_group()
    group.add_argument(
        '--journals_text_path', '-J',
        type=str, metavar='PATH')
    group.add_argument(
        '--authors_text_path', '-A',
        type=str, metavar='PATH')
    argument_parser.add_argument(
        '--keywords_text_path', '-K',
        type=str, metavar='PATH')
    argument_parser.add_argument(
        '--mesh_terms_text_path', '-M',
        type=str, metavar='PATH')
    argument_parser.add_argument(
        '--from_date', '-F', nargs='?',
        type=parse_date, metavar='DATE',
        default=parse_date('01-01-1900'),
        help='%%m-%%d-%%Y')
    argument_parser.add_argument(
        '--to_date', '-T', nargs='?',
        type=parse_date, metavar='DATE',
        default=datetime.today(),
        help='%%m-%%d-%%Y')
    argument_parser.add_argument(
        '--date_interval_in_years', '-I',
        type=int, metavar='INTEGER')
    args = argument_parser.parse_args()
    run(
        args.target_folder,
        journals_path=args.journals_text_path,
        authors_path=args.authors_text_path,
        keywords_path=args.keywords_text_path,
        mesh_terms_path=args.mesh_terms_text_path,
        from_date=args.from_date,
        to_date=args.to_date,
        date_interval_in_years=args.date_interval_in_years)
