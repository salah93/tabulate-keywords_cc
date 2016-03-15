import sys
from os.path import abspath, dirname, join

print(__file__)
print(abspath(__file__))
TEST_FOLDER = dirname(abspath(__file__))
print(TEST_FOLDER)
sys.path.insert(0, dirname(TEST_FOLDER))
from tabulate_tools import load_unique_lines


def test_load_unique_lines():
    lines = load_unique_lines(join(TEST_FOLDER, 'lines.txt'))
    assert lines == ['one', 'two']
