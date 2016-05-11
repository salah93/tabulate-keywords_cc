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
import datetime
import sqlite3

from argparse import ArgumentParser
from contextlib import contextmanager
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
    try:
        yield cursor
    except:
        raise
    else:
        cursor.commit()
    finally:
        cursor.close()


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
    # Get list containing tuples of date ranges based on interval
    try:
        date_ranges = get_date_ranges(
            from_date, to_date, date_interval_in_years)
    except ToolError as e:
        exit('to_date.error = {0}'.format(e))

    search_authors, query_list = ((True, authors) if authors
                                  else (False, journals))
    # Tabulate keywords
    results = tabulate(
        query_list, date_ranges, text_words, mesh_terms, search_authors)
    search_counts = results['search_counts']
    queries = results['queries']
    query_totals = results['query_totals']
    first_name_articles = results['first_name_log']
    first_name_table = results['first_name_table']

    # Output setup
    with open(log_path, 'w') as f:
        f.write(queries)
        f.write(query_totals)
    results_table = DataFrame(search_counts)
    results_table.to_csv(target_path, index=False)
    # Required print statement for crosscompute tool
    print('results_table_path = ' + target_path)
    print('log_text_path = ' + log_path)
    # image of plot
    if date_interval_in_years:
        axes = (results_table * 100).plot()
        axes.set_title('Percent frequency over time')
        figure = axes.get_figure()
        figure.savefig(image_path)
        print('keyword_article_count_image_path = ' + image_path)
    if search_authors:
        first_name_path = join(target_folder, 'first_name_articles.txt')
        first_name_table_path = join(target_folder, 'first_name_articles.csv')
        with open(first_name_path, 'w') as f:
            f.write(first_name_articles)
        table = DataFrame(first_name_table, columns=['Author', 'Count'])
        table.to_csv(first_name_table_path, index=False)
        print('first_named_articles_text_path = ' + first_name_path)
        print('first_named_articles_table_path = ' + first_name_table_path)


def tabulate(query_list, date_ranges, text_words, mesh_terms, search_authors):
    log = ""
    table_data = None
    search_counts, queries, query_totals = collections.OrderedDict(), [], []
    # O(n*y) for n=len(query_list) and y=len(date_ranges)
    author_articles = (collections.defaultdict(list) if search_authors
                       else None)
    if query_list:
        from_col, to_col = 'From Date', 'To Date'
        query_col = 'Author Name' if search_authors else 'Journal Name'
        partial = 'Keyword Article Count'
        total = 'Total Article Count'
        search_counts[from_col], search_counts[to_col] = [], []
        search_counts[query_col] = []
        search_counts[partial], search_counts[total] = [], []
        for from_date, to_date in date_ranges:
            for item in query_list:
                # date_index = lambda x: str(x)[:10]
                search_counts[from_col].append(from_date)
                search_counts[to_col].append(to_date)
                search_counts[query_col].append(item)
                # Query totals (w/o keywords)
                if search_authors:
                    item_expression = get_expression(
                        author_name=item,
                        from_date=from_date, to_date=to_date)
                    expression = get_expression(
                        author_name=item, text_terms=text_words,
                        mesh_terms=mesh_terms,
                        from_date=from_date, to_date=to_date)
                else:
                    item_expression = get_expression(
                        journal_name=item,
                        from_date=from_date, to_date=to_date)
                    expression = get_expression(
                        journal_name=item, text_terms=text_words,
                        mesh_terms=mesh_terms,
                        from_date=from_date, to_date=to_date)
                with query() as cursor:
                    item_count = cursor.execute("""SELECT count from count
                                                where query = ?""",
                                                (item_expression,)).fetchone()
                    articles = cursor.execute("""SELECT article from articles
                                              where query = ?""",
                                              (item_expression,)).fetchall()
                if not item_count or not articles:
                    item_count, articles = get_search_count(item_expression)
                    with query() as cursor:
                        cursor.execute("""INSERT INTO count(query, count)
                                       values(?, ?)""",
                                       (expression, item_count))
                        (cursor.execute("""INSERT INTO articles(query, article)
                                       values(?, ?)""", (expression, article))
                         for article in articles)
                if search_authors:
                    author_articles[item].extend(articles)
                search_counts[total].append(item_count)
                print('Total - ' + item_expression)
                print(str(item_count) + '\n')
                query_totals.append(
                    'Total - ' + item_expression + '\n' + str(item_count))
                # Get search count data for each Query (w/ keywords)
                with query() as cursor:
                    count = cursor.execute("""SELECT count from count
                                           where query=?""",
                                           (expression,)).fetchone()
                if not count:
                    count, articles = get_search_count(expression)
                    with query() as cursor:
                        cursor.execute("""INSERT INTO count(query, count)
                                       values(?, ?)""", (expression, count))
                search_counts[partial].append(count)
                # Log is printed to standard output and file
                print(expression)
                print(str(count) + '\n')
                queries.append(expression + '\n' + str(count))
    else:
        from_col = 'From Date'
        to_col = 'To Date'
        keyword_count = 'Count'
        search_counts[from_col], search_counts[to_col] = [], []
        search_counts[keyword_count] = []
        for from_date, to_date in date_ranges:
            expression = get_expression(
                text_terms=text_words, mesh_terms=mesh_terms,
                from_date=from_date, to_date=to_date)
            with query() as cursor:
                count = cursor.execute("""SELECT count from count
                                       where query = ?""",
                                       (expression,)).fetchone()
            if not count:
                count, articles = get_search_count(expression)
                with query() as cursor:
                    cursor.execute("""INSERT INTO count(query, count)
                                   values(?, ?)""", (expression, count))
            search_counts[from_col].append(from_date)
            search_counts[to_col].append(to_date)
            search_counts[keyword_count].append(count)
            # Log is printed to standard output and file
            print(expression)
            print(str(count) + '\n')
            queries.append(expression + '\n' + str(count))
    if author_articles:
        authors = query_list
        first_name_articles = [get_first_name_articles(
                                    name, author_articles[name])
                               for name in authors]
        log = []
        for name, articles in zip(authors, first_name_articles):
            log.append('\n' + name + ': ')
            for article in articles:
                log.append('\t' + article)
        table_data = zip(authors, [len(article_list)
                                   for article_list in first_name_articles])
    return dict(
        first_name_log='\n'.join(log),
        first_name_table=table_data,
        search_counts=search_counts,
        queries='\n\n'.join(queries),
        query_totals='\n\n'.join(query_totals))


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
        default=datetime.datetime.today(),
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
