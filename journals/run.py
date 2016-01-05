'''
 Get search Count results from pubmed for specified queries:
 example:
 journals -> Science, Nature
 keywords -> poverty, income
 MeSh ->     social class, socioeconomic factors
 resulting queries = 
 [
 ("Nature"[Journal]) AND ("income"[Text Word] OR 
        "poverty"[Text Word] OR "social class"[MeSH Terms] OR 
            "socioeconomic factors"[MeSH Terms]),
 
 ("Science"[Journal]) AND ("income"[Text Word] OR 
        "poverty"[Text Word] OR "social class"[MeSH Terms] OR 
            "socioeconomic factors"[MeSH Terms])
 ]
    and the output would be their corresponding search count results found on pubmed
'''
from argparse import ArgumentParser
from pandas import DataFrame
from tabulate_tools import *


def run(target_folder, journals_path=None, authors_path=None, keywords_path=None, 
                mesh_terms_path=None, from_date=None, to_date=None, 
                    date_interval_in_years=None):
    target_path = join(target_folder, 'results.csv')
    # Input retrieval
    journals = input_retrieval(journals_path)
    text_words = input_retrieval(keywords_path)
    mesh_terms = input_retrieval(mesh_terms_path) 
    authors = input_retrieval(authors_path)
    # Get list containing tuples of date ranges based on interval
    date_ranges = get_date_ranges(
        from_date, to_date, date_interval_in_years)

    query_list = journals if journals else authors
    search_journals = True if journals else False
    
    # Tabulate keywords
    results = tabulate(query_list, date_ranges, text_words, mesh_terms, search_journals)
    search_counts = results['search_counts']
    queries = results['queries']
    query_totals = results['query_totals']

    # Output setup
    log_path = join(target_folder, 'log_results.txt')
    with open(log_path, 'w') as f:
        f.write(queries)
        f.write(query_totals)
    dates_index= [str(x)[:10] + ' to ' + 
            str(y)[:10] for x, y in date_ranges]
    results_table = DataFrame(search_counts, index=dates_index)
    results_table.to_csv(target_path)

    # Required print statement for crosscompute tool
    print('results_table_path = ' + target_path)
    print('log_text_path = ' + log_path)

def tabulate(query_list, date_ranges, text_words, mesh_terms, search_journals):
    search_counts = {}
    queries = ""
    query_totals = ""
    # O(n*y) for n=len(query_list) and y=len(date_ranges) 
    if len(query_list) > 0:
        for item in query_list:
            total = 'Total Count - [' + item + ']' 
            partial = 'Count - [' + item + ']' 
            search_counts[partial] = []
            search_counts[total] = []
            # search_counts[item] = {'partial':[], 'total':[]} 
            for from_date, to_date in date_ranges:
                # Query totals (w/o keywords)
                if search_journals:
                    item_expression = get_expression(journal_name=item, 
                            from_date=from_date, to_date=to_date)
                else:
                    item_expression = get_expression(author_name=item, 
                            from_date=from_date, to_date=to_date)
                item_count = get_search_count(item_expression)
                search_counts[total].append(item_count)
                print("Total - " + item_expression)
                print(str(item_count) + '\n')
                query_totals += ('Total - ' 
                     + item_expression + '\n' + str(item_count) + '\n\n')

                # Get search count data for each Query (w/ keywords)
                if search_journals:
                    expression = get_expression(journal_name=item, text_terms=text_words, 
                            mesh_terms=mesh_terms, from_date=from_date, to_date=to_date)
                else:
                    expression = get_expression(author_name=item, text_terms=text_words, 
                            mesh_terms=mesh_terms, from_date=from_date, to_date=to_date)

                count = get_search_count(expression)
                search_counts[partial].append(count)
                # Log is printed to standard output and file
                print(expression)
                print(str(count) + '\n')
                queries += expression + '\n' + str(count) + '\n\n'
    else:
        search_counts['Counts'] = []
        for from_date, to_date in date_ranges:
            expression = get_expression(text_terms=text_words, 
                    mesh_terms=mesh_terms, from_date=from_date, to_date=to_date)
            count = get_search_count(expression)
            search_counts['Counts'].append(count)
            # Log is printed to standard output and file
            print(expression)
            print(str(count) + '\n')
            queries += expression + '\n' + str(count) + '\n\n'
    return dict(search_counts=search_counts, queries=queries, 
            query_totals=query_totals)

if __name__ == '__main__':
    argument_parser = ArgumentParser()
    argument_parser.add_argument(
        '--target_folder', nargs='?', default = 'results',
        type=mkdir_local, metavar='FOLDER')
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
        authors_path= args.authors_text_path,
        keywords_path=args.keywords_text_path,
        mesh_terms_path=args.mesh_terms_text_path,
        from_date=args.from_date, to_date=args.to_date,
        date_interval_in_years=args.date_interval_in_years
    )
