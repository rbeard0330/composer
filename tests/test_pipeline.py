import pytest

from src.compose import composable, Composable


@composable
def add_one(x):
    return x + 1


def noncomposable_add_one(x):
    return x + 1


def double(x):
    return x * 2


@pytest.fixture
def pipeline_starting_with_composable():
    return add_one >> double >> add_one


@pytest.fixture
def pipeline_starting_with_noncomposable():
    return noncomposable_add_one >> composable(double) >> add_one


@pytest.fixture
def pipeline_with_lambda_at_end():
    return add_one >> double >> (lambda x: x + 1)


@pytest.fixture
def pipeline_with_lambda_at_start():
    return (lambda x: x + 1) >> composable(double) >> add_one


@pytest.fixture(params=[i for i in range(4)])
def pipeline(request, pipeline_starting_with_composable, pipeline_starting_with_noncomposable,
             pipeline_with_lambda_at_start, pipeline_with_lambda_at_end):
    fixtures = [pipeline_starting_with_composable, pipeline_starting_with_noncomposable,
                pipeline_with_lambda_at_start, pipeline_with_lambda_at_end]
    return fixtures[request.param]


@pytest.fixture
def pipeline_with_reducer(pipeline):
    return pipeline > sum


@pytest.fixture(scope="session")
def sample_data():
    return [1, 2, 3]


@pytest.fixture
def data_into_pipeline(sample_data, pipeline):
    return sample_data >> pipeline


@pytest.fixture
def pipeline_from_composable_data(sample_data):
    return composable(sample_data) >> noncomposable_add_one >> double >> noncomposable_add_one


@pytest.fixture(params=[i for i in range(2)])
def pipelined_data(request, data_into_pipeline, pipeline_from_composable_data):
    fixtures = [data_into_pipeline, pipeline_from_composable_data]
    return fixtures[request.param]


@pytest.mark.parametrize(["value", "expected"], [[3, 9], [-1, 1], [10.4, 23.8]])
def test_pipeline_as_function(value, expected, pipeline):
    assert pipeline(value) == expected


def test_pipelined_data_call(pipelined_data):
    assert pipelined_data() == [5, 7, 9]


def test_applying_reducer_to_pipelined_data_evaluates_immediately(pipelined_data):
    assert (pipelined_data > sum) == 21


def test_piping_data_into_pipeline_with_reducer_evaluates_immediately(pipeline_with_reducer):
    assert isinstance(pipeline_with_reducer, Composable)
    assert [1, 2, 3] >> pipeline_with_reducer == 21
