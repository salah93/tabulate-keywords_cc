import sys
from os.path import abspath, dirname, join


TEST_FOLDER = dirname(abspath(__file__))
# insert tabulate tools in sys path so you can import
sys.path.insert(0, dirname(TEST_FOLDER))
from tabulate_tools import get_search_count
from load_lines import load_unique_lines


def test_retstart():
    expression = 'Reshma%20Jagsi[author]'
    count, articles_list_20 = get_search_count(expression, retmax=20)
    count, articles_list_100 = get_search_count(expression, retmax=1000)
    assert len(articles_list_20) == len(articles_list_100)


def test_load_unique_lines():
    lines = load_unique_lines(join(TEST_FOLDER, 'lines.txt'))
    assert lines == ['one', 'two']
