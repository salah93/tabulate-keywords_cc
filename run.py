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
import sys
from argparse import ArgumentParser
from collections import defaultdict
from datetime import datetime
from dateutil.parser import parse as parse_date
from os.path import join

from invisibleroads_macros.disk import make_folder
import matplotlib
matplotlib.use('Agg')
from pandas import DataFrame
from sqlalchemy.orm import sessionmaker

from tabulate_tools import (
    get_expression, get_search_count,
    get_first_name_articles)
from load_lines import get_date_ranges, load_unique_lines, ToolError
from models import Base, Query, engine


def tabulate(query_list, date_ranges, text_words, mesh_terms, isAuthor):
    # O(n*y) for n=len(query_list) and y=len(date_ranges)
    author_articles = defaultdict(list)
    sc = []
    log = []
    if query_list:
        sc = [('from', 'to', 'name', 'count', 'count w/ keywords')]
        for from_date, to_date in date_ranges:
            for item in query_list:
                query_param = ({'author_name': item} if isAuthor
                               else {'journal_name': item})
                # Query totals (w/o keywords)
                item_query = get_expression(
                    from_date=from_date, to_date=to_date, **query_param)
                query = session.query(Query).filter_by(query=item_query)
                # TODO: does the query for previous list change?
                item_articles = [article.article_id for article in query]
                item_count = len(item_articles)
                query = get_expression(
                    text_terms=text_words,
                    mesh_terms=mesh_terms,
                    from_date=from_date, to_date=to_date,
                    **query_param)
                keyword_count = session.query(Query.id).filter_by(
                                                  query=query).count()
                if item_count == 0:
                    item_articles = cache_results(item_query)
                    item_count = len(item_articles)
                if keyword_count == 0:
                    articles = cache_results(query)
                    keyword_count = len(articles)
                log.append("{query}\n{count}".format(query=item_query, count=item_count))
                log.append("{query}\n{count}".format(query=query, count=keyword_count))
                if isAuthor:
                    author_articles[item].extend(item_articles)
                # Get search count data for each Query (w/ keywords)
                sc.append((from_date, to_date, item,
                           item_count, keyword_count))
    else:
        sc = [('from', 'to', 'count')]
        for from_date, to_date in date_ranges:
            query = get_expression(
                text_terms=text_words, mesh_terms=mesh_terms,
                from_date=from_date, to_date=to_date)
            count = session.query(Query.id).filter_by(query=query).count()
            if count == 0:
                articles = cache_results(query)
                count = len(articles)
            log.append("{query}\n{count}".format(query=query, count=count))
            sc.append((from_date, to_date, count))
    return dict(search_counts=sc, author_articles=author_articles, log=log)


def create_csv(results_path, data):
    with open(results_path, 'w') as f:
        writer = csv.writer(f)
        writer.writerows(data)


def cache_results(query):
    articles = get_search_count(query)
    for article in articles:
        q = Query(query=query, article_id=article)
        session.add(q)
        # TODO: can i add this commit after loop?
        session.commit()
    return articles


if __name__ == '__main__':
    Base.metadata.bind = engine
    dbsession = sessionmaker(bind=engine)
    session = dbsession()

    argument_parser = ArgumentParser()
    argument_parser.add_argument(
        '--target_folder', nargs='?', default='results',
        type=make_folder, metavar='FOLDER')
    group = argument_parser.add_mutually_exclusive_group()
    group.add_argument(
        '--journals_path', '-J',
        type=str, metavar='PATH')
    group.add_argument(
        '--authors_path', '-A',
        type=str, metavar='PATH')
    argument_parser.add_argument(
        '--keywords_path', '-K',
        type=str, metavar='PATH')
    argument_parser.add_argument(
        '--mesh_terms_path', '-M',
        type=str, metavar='PATH')
    argument_parser.add_argument(
        '--from_date', '-F', nargs='?',
        type=parse_date, metavar='FROM',
        default=parse_date('01-01-1900'),
        help='%%m-%%d-%%Y')
    argument_parser.add_argument(
        '--to_date', '-T', nargs='?',
        type=parse_date, metavar='TO',
        default=datetime.today(),
        help='%%m-%%d-%%Y')
    argument_parser.add_argument(
        '--interval_in_years', '-I',
        type=int, metavar='INTERVAL')
    args = argument_parser.parse_args()
    # Input retrieval
    journals = load_unique_lines(args.journals_path)
    text_words = load_unique_lines(args.keywords_path)
    mesh_terms = load_unique_lines(args.mesh_terms_path)
    authors = load_unique_lines(args.authors_path)
    # Get date ranges based on interval
    try:
        date_ranges = get_date_ranges(
            args.from_date, args.to_date, args.interval_in_years)
    except ToolError as e:
        sys.exit('to_date.error = {0}'.format(e))
    isAuthor, query_list = ((True, authors) if authors
                            else (False, journals))
    # Tabulate keywords
    results = tabulate(
        query_list, date_ranges, text_words, mesh_terms, isAuthor)
    author_articles = results['author_articles']
    search_count = results['search_counts']
    sc_df = DataFrame(search_count[1:], columns=search_count[0])
    search_count_path = join(args.target_folder, 'search_counts.csv')
    sc_df.to_csv(search_count_path, index=False)
    # crosscompute print statement
    print("search_count_table_path = " + search_count_path)
    if isAuthor:
        cols = ['Author', 'No. first name articles']
        first_name_articles = [(name, len(get_first_name_articles(
                                          name, author_articles[name])))
                               for name in authors]
        fa_df = DataFrame(first_name_articles, columns=cols)
        first_name_path = join(args.target_folder, 'first_named_articles.csv')
        fa_df.to_csv(first_name_path, index=False)
        print("first_name_articles_table_path = " + first_name_path)
    # log
    queries = results['log']
    log_path = join(args.target_folder, 'log.txt')
    with open(log_path, 'w') as f:
        f.write('\n\n'.join(queries))
    print('log_text_path = ' + log_path)
    # image of plot
    if args.interval_in_years:
        image_path = join(args.target_folder, 'keyword_article_count.png')
        axes = (sc_df * 100).plot()
        axes.set_title('Percent frequency over time')
        figure = axes.get_figure()
        figure.savefig(image_path)
        print('plot_image_path = ' + image_path)
