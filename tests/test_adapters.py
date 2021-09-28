from src.compose import composable, adapter
from src.adapters import *

def test_simple_adapter():
    assert (composable([1, 2, 3]) >> adapter(enumerate) > list) == [(0, 1), (1, 2), (2, 3)]


def test_dropwhile():
    result = [1, 2, 3] >> composable(lambda x: x * 4) >> dropwhile(lambda x: x < 5) > list
    assert result == [8, 12]


def test_take():
    result = [1, 2, 3] >> composable(lambda x: x * 4) >> take(2) > list
    assert result == [4, 8]
    result = [1, 2, 3] >> composable(lambda x: x * 4) >> take(4) > list
    assert result == [4, 8, 12]
    result = [1, 2, 3] >> composable(lambda x: x * 4) >> take(0) > list
    assert result == [0]

