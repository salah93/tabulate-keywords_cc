import datetime
import sys
from os.path import abspath, dirname, join


TEST_FOLDER = dirname(abspath(__file__))
# insert tabulate tools in sys path so you can import
sys.path.insert(0, dirname(TEST_FOLDER))
from tabulate_tools import get_search_count
from run import tabulate
from load_lines import load_unique_lines


def test_retstart():
    expression = 'Reshma%20Jagsi[author]'
    articles_list_20 = get_search_count(expression, retmax=20)
    articles_list_100 = get_search_count(expression, retmax=1000)
    assert len(articles_list_20) == len(articles_list_100)


def test_run():
    authors = ['Reshma Jagsi', 'Curtiland Deville', 'Emma Holliday'] 
    keywords = ['income', 'poverty']
    mesh = ['social class', 'socioeconomic factors']
    dates = [(datetime.datetime(1990, 1, 1),
              datetime.datetime(2015, 12, 31))]
    res = tabulate(authors, dates, keywords, mesh, True)
    print(res['search_counts'])
    print(res['author_articles'])
    assert res['author_articles']['Reshma Jagsi']


def test_load_unique_lines():
    lines = load_unique_lines(join(TEST_FOLDER, 'lines.txt'))
    assert lines == ['one', 'two']
