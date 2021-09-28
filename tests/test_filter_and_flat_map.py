from src.compose import composable, as_filter, flat_map


@composable
def add_one(x):
    return x + 1


def triple(x):
    return x * 3


even_filter = as_filter(lambda x: x % 2 == 0)


def test_pipeline_with_filter():
    result = [1, 2, 3] >> add_one >> even_filter >> triple > list
    assert result == [6, 12]


def test_pipeline_with_two_filters():
    result = [1, 2, 3] >> add_one >> even_filter >> triple >> as_filter(lambda x: x % 4 == 0) >> add_one > list
    assert result == [13]


def test_pipeline_with_filter_and_flat_map():
    result = [1, 2, 3] >> add_one >> flat_map(lambda x: [i for i in range(1, x + 1)]) >> even_filter >> triple > list
    assert result == [6, 6, 6, 12]


def test_pipeline_with_two_flat_maps():
    result = [1] >> add_one >> flat_map(lambda x: [i for i in range(0, x + 1)]) >> flat_map(lambda x: [i for i in range(x + 1)]) > list
    assert result == [0, 0, 1, 0, 1, 2]


def test_call_pipeline_with_filter():
    pipeline = add_one >> even_filter
    assert pipeline(1) == [2]


def test_call_pipeline_with_filter_and_flat_map():
    pipeline = add_one >> even_filter >> flat_map(lambda x: [i for i in range(0, x + 1)])
    assert pipeline(1) == [0, 1, 2]
