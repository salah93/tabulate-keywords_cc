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
from argparse import ArgumentParser
from collections import defaultdict
from datetime import datetime
from dateutil.parser import parse as parse_date
from os.path import join

# needed for image
from invisibleroads_macros.disk import make_folder
import pandas as pd

from load_lines import get_date_ranges, load_unique_lines, ToolError
from tabulate_tools import (
    get_expression, get_search_count,
    get_first_name_articles)
import matplotlib
matplotlib.use('Agg')


def tabulate_entities(query_list, date_ranges, text_words, mesh_terms, author):
    dates = []
    log = []
    author_articles = defaultdict(list)
    counts = defaultdict(list)
    keyword_counts = defaultdict(list)

    for from_date, to_date in date_ranges:
        dates.append(pd.Timestamp(from_date))
        for item in query_list:
            query_param = (
                {'author_name': item} if author else {'journal_name': item})
            # Query totals (w/o keywords)
            item_query = get_expression(
                from_date=from_date, to_date=to_date, **query_param)
            item_articles = get_search_count(item_query)
            item_count = len(item_articles)
            query = get_expression(
                text_terms=text_words,
                mesh_terms=mesh_terms,
                from_date=from_date, to_date=to_date,
                **query_param)
            articles = get_search_count(query)
            keyword_count = len(articles)
            log.append("{query}\n{count}".format(
                query=item_query, count=item_count))
            log.append("{query}\n{count}".format(
                query=query, count=keyword_count))
            if author:
                author_articles[item].extend(item_articles)
            # Get search count data for each Query (w/ keywords)
            counts[item].append(item_count)
            keyword_counts[item].append(keyword_count)
    index = pd.Index(dates, name='dates')
    search_counts = pd.DataFrame(counts, index=index)
    keyword_search_counts = pd.DataFrame(keyword_counts, index=index)
    return dict(
        search_counts=search_counts,
        keyword_search_counts=keyword_search_counts,
        author_articles=author_articles,
        log=log)


def tabulate_keywords(date_ranges, text_words, mesh_terms):
    counts = defaultdict(list)
    dates = []
    log = []
    for from_date, to_date in date_ranges:
        query = get_expression(
            text_terms=text_words, mesh_terms=mesh_terms,
            from_date=from_date, to_date=to_date)
        articles = get_search_count(query)
        count = len(articles)
        log.append("{query}\n{count}".format(query=query, count=count))
        dates.append(pd.Timestamp(from_date))
        counts['count'].append(count)
    index = pd.Index(dates, name='dates')
    search_counts = pd.DataFrame(counts, index=index)
    return dict(search_counts=search_counts, log=log)


def saveimage(df, image_path, title):
    axes = df.plot()
    axes.set_title(title)
    figure = axes.get_figure()
    figure.savefig(image_path)


if __name__ == '__main__':
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
    journals = load_unique_lines(args.journals_path)
    text_words = load_unique_lines(args.keywords_path)
    mesh_terms = load_unique_lines(args.mesh_terms_path)
    authors = load_unique_lines(args.authors_path)

    try:
        date_ranges = get_date_ranges(
            args.from_date, args.to_date, args.interval_in_years)
    except ToolError as e:
        print('date_ranges.error = {0}'.format(e))
        raise ToolError
    author, query_list = (
        (True, authors) if authors else (False, journals))
    if not query_list:
        results = tabulate_keywords(date_ranges, text_words, mesh_terms)
    else:
        results = tabulate_entities(
            query_list, date_ranges, text_words, mesh_terms, author)
        keyword_search_counts = results['keyword_search_counts']
        keyword_search_count_path = join(
            args.target_folder, 'keyword_search_counts.csv')
        keyword_search_counts.to_csv(keyword_search_count_path)
        print("keyword_search_count_table_path = " + keyword_search_count_path)
        if args.interval_in_years:
            title = 'Article Counts over time with Keywords'
            image_path = join(args.target_folder, 'keyword_article_count.png')
            saveimage(
                keyword_search_counts,
                image_path,
                title)
            print('keywords_plot_image_path = ' + image_path)
        if author:
            author_articles = results['author_articles']
            cols = ['Author', 'No. first name articles']
            first_name_articles = [
                (name, len(
                    get_first_name_articles(name, author_articles[name])))
                for name in authors]
            df = pd.DataFrame(first_name_articles, columns=cols)
            first_name_path = join(
                args.target_folder, 'first_named_articles.csv')
            df.to_csv(first_name_path, index=False)
            print("first_name_articles_table_path = " + first_name_path)
    search_counts = results['search_counts']
    search_count_path = join(args.target_folder, 'search_counts.csv')
    search_counts.to_csv(search_count_path)
    print("search_count_table_path = " + search_count_path)
    if args.interval_in_years:
        title = 'Article Counts over time'
        image_path = join(args.target_folder, 'article_count.png')
        saveimage(search_counts, image_path, title)
        print('plot_image_path = ' + image_path)
    # log
    queries = results['log']
    log_path = join(args.target_folder, 'log.txt')
    with open(log_path, 'w') as f:
        f.write('\n\n'.join(queries))
    print('log_text_path = ' + log_path)
