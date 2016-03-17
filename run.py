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
from argparse import ArgumentParser
from dateutil.parser import parse as parse_date
from invisibleroads_macros.disk import make_folder
from os.path import join
from pandas import DataFrame
from tabulate_tools import (
    get_expression, get_search_count,
    get_first_name_articles)
from load_lines import get_date_ranges, load_unique_lines, ToolError


def run(
        target_folder, journals_path=None, authors_path=None,
        keywords_path=None, mesh_terms_path=None,
        from_date=None, to_date=None, date_interval_in_years=None):
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

    search_journals, query_list = ((True, journals) if journals
                                   else (False, authors))
    # Tabulate keywords
    results = tabulate(
        query_list, date_ranges, text_words, mesh_terms, search_journals)
    search_counts = results['search_counts']
    queries = results['queries']
    query_totals = results['query_totals']
    author_articles = results['author_articles']

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
    if author_articles:
        first_name_articles = [get_first_name_articles(
                                    name, author_articles[name])
                               for name in authors]
        first_name_path = join(target_folder, 'first_name_articles.txt')
        with open(first_name_path, 'w') as f:
            log = []
            for name, articles in zip(authors, first_name_articles):
                log.append('\n' + name + ': ')
                for article in articles:
                    log.append('\t' + article)
            f.write('\n'.join(log))
        first_name_table_path = join(target_folder, 'first_name_articles.csv')
        table_data = {'Author Name': authors,
                      'First-Named Articles':
                      [len(article) for article in first_name_articles]}
        table = DataFrame(table_data)
        table.to_csv(first_name_table_path, index=False)
        print('first_named_articles_text_path = ' + first_name_path)
        print('first_named_articles_table_path = ' + first_name_table_path)


def tabulate(query_list, date_ranges, text_words, mesh_terms, search_journals):
    search_counts, queries, query_totals = collections.OrderedDict(), [], []
    # O(n*y) for n=len(query_list) and y=len(date_ranges)
    author_articles = (None if search_journals
                       else collections.defaultdict(list))
    if query_list:
        from_col, to_col = 'From Date', 'To Date'
        query_col = 'Journal Name' if search_journals else 'Author Name'
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
                if search_journals:
                    item_expression = get_expression(
                        journal_name=item,
                        from_date=from_date, to_date=to_date)
                    expression = get_expression(
                        journal_name=item, text_terms=text_words,
                        mesh_terms=mesh_terms,
                        from_date=from_date, to_date=to_date)
                else:
                    item_expression = get_expression(
                        author_name=item,
                        from_date=from_date, to_date=to_date)
                    expression = get_expression(
                        author_name=item, text_terms=text_words,
                        mesh_terms=mesh_terms,
                        from_date=from_date, to_date=to_date)
                item_count, articles = get_search_count(item_expression)
                if not search_journals:
                    author_articles[item].extend(articles)
                search_counts[total].append(item_count)
                print('Total - ' + item_expression)
                print(str(item_count) + '\n')
                query_totals.append(
                    'Total - ' + item_expression + '\n' + str(item_count))

                # Get search count data for each Query (w/ keywords)
                count, articles = get_search_count(expression)
                search_counts[partial].append(count)
                # Log is printed to standard output and file
                print(expression)
                print(str(count) + '\n')
                queries.append(expression + '\n' + str(count))
    else:
        search_counts['Counts'] = []
        for from_date, to_date in date_ranges:
            expression = get_expression(
                text_terms=text_words, mesh_terms=mesh_terms,
                from_date=from_date, to_date=to_date)
            count = get_search_count(expression)
            search_counts['Counts'].append(count)
            # Log is printed to standard output and file
            print(expression)
            print(str(count) + '\n')
            queries.append(expression + '\n' + str(count))
    return dict(
        author_articles=author_articles,
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
