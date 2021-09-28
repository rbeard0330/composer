import pytest

from src.compose import composable, Composable


def test_decorator_returns_composable_with_no_source():
    @composable
    def fn(x):
        return x

    assert isinstance(fn, Composable)
    assert fn.source is None


def test_composable_raises_error_if_used_on_fn_with_as_data():
    def fn(x):
        return x
    with pytest.raises(TypeError) as cm:
        composable(fn, as_data=True)
    assert 'not iterable' in str(cm.value)


def test_composable_fn_returns_composable_when_given_data():
    data = [1, 2, 3]
    result = composable(data)
    assert isinstance(result, Composable)
    assert result.source is data

    result = composable(data, as_data=True)
    assert isinstance(result, Composable)
    assert result.source is data


def test_composable_fn_with_as_data_false_raises_when_given_data():
    with pytest.raises(TypeError) as cm:
        composable([1, 2, 3], as_data=False)
    assert 'noncallable' in str(cm.value)


class CallableIterable:

    def __call__(self, *args, **kwargs):
        return True

    def __iter__(self):
        return iter([1, 2, 3])


def test_as_data_controls_for_callable_iterable():
    for arg in [True, False, None]:
        result = composable(CallableIterable(), as_data=arg)
        assert isinstance(result, Composable)
        if arg is True:
            assert result.source is not None
        else:
            assert result.source is None

