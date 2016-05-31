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
import csv
import sqlite3

from argparse import ArgumentParser
from collections import defaultdict
from contextlib import contextmanager
from datetime import datetime
from dateutil.parser import parse as parse_date
from invisibleroads_macros.disk import make_folder
from os.path import join

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
    search_count_path = join(target_folder, 'search_counts.csv')
    first_name_path = join(target_folder, 'first_named_articles.csv')
    image_path = join(target_folder, 'keyword_article_count.jpg')

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
    author_articles = results['author_articles']
    search_count = results['search_counts']
    cols = [('Author', 'No. first name articles')]
    first_name_articles = cols + [(name, len(get_first_name_articles(
                                      name, author_articles[name])))
                            for name in authors]
    create_csv(search_count_path, search_count)
    create_csv(first_name_path, first_name_articles)
    # crosscompute print statement
    print("search_count_table_path = " + search_count_path)
    print("first_name_articles_table_path = " + first_name_path)


def tabulate(query_list, date_ranges, text_words, mesh_terms, isAuthor):
    # O(n*y) for n=len(query_list) and y=len(date_ranges)
    author_articles = defaultdict(list)
    sc = []
    if query_list:
        sc = [('from', 'to', 'name', 'count', 'count w/ keywords')]
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
                    search_list = [(item_expression,), (expression,)]
                    item_articles= [str(article[0]) for article in cursor.execute("""SELECT article from
                                                  articles where query=?  """,
                                                  (item_expression,)
                                                  ).fetchall()]
                    keyword_articles = cursor.execute("""SELECT article from
                                                      articles where query=?
                                                      """,
                                                      (expression,)
                                                      ).fetchall()
                    keyword_count = len(keyword_articles)
                    item_count = len(item_articles)
                if not item_count:
                    item_articles = get_search_count(item_expression)
                    item_count = len(item_articles)
                    insert_articles = [(item_expression, article)
                                       for article in item_articles]
                    with query() as cursor:
                        cursor.executemany("""INSERT INTO
                                         articles(query, article)
                                         values(?, ?)""", insert_articles)
                if isAuthor:
                    author_articles[item].extend(item_articles)
                # Get search count data for each Query (w/ keywords)
                if not keyword_count:
                    keyword_articles = get_search_count(expression)
                    keyword_count = len(keyword_articles)
                    with query() as cursor:
                        insert_articles = [(expression, article)
                                           for article in keyword_articles]
                        cursor.executemany("""INSERT INTO
                                           articles(query, article)
                                           values(?, ?)""", insert_articles)
                sc.append((from_date, to_date, item,
                           item_count, keyword_count))
    else:
        sc = [('from', 'to', 'count')]
        for from_date, to_date in date_ranges:
            expression = get_expression(
                text_terms=text_words, mesh_terms=mesh_terms,
                from_date=from_date, to_date=to_date)
            with query() as cursor:
                count = len(cursor.execute("""SELECT article from articles
                                           where query = ?""",
                                           (expression,)).fetchall())
            if not count:
                articles = get_search_count(expression)
                count = len(articles)
                with query() as cursor:
                    insert_articles = [(expression, article)
                                       for article in articles]
                    cursor.executemany("""INSERT INTO articles(query, article)
                                        values(?, ?)""", list_articles)
            sc.append((from_date, to_date, count))
    return dict(search_counts=sc, author_articles=author_articles)


def create_csv(results_path, data):
    with open(results_path, 'w') as f:
        writer = csv.writer(f)
        writer.writerows(data)


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
