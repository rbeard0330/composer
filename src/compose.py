from collections import Iterable, namedtuple
from functools import update_wrapper
from itertools import chain

import composable

# TODO: Lazy composables, .evaluate() method, better error messages for missing adapter, slicing/indexing

def _is_iterable(obj):
    return isinstance(obj, Iterable) or hasattr(obj, '__getitem__')


MAP_TRANSFORM = 0
FILTER_TRANSFORM = 1
FLAT_MAP_TRANSFORM = 2


def as_filter(fn):
    return Composable([Transformation(fn=fn, transform_type=FILTER_TRANSFORM)])


def flat_map(fn):
    return Composable([Transformation(fn=fn, transform_type=FLAT_MAP_TRANSFORM)])


def adapter(fn):
    return Adapter(fn)


Transformation = namedtuple('Transformation', ['fn', 'transform_type'])


class Adapter:
    def __init__(self, fn):
        self.fn = fn


def map_fn_to_transform(fn):
    if isinstance(fn, Transformation):
        return fn
    else:
        return Transformation(fn=fn, transform_type=MAP_TRANSFORM)


class Composable:
    def __init__(self, fns, source=None):
        self.fn_list = [map_fn_to_transform(fn) for fn in fns]
        if source is not None:
            if not _is_iterable(source):
                raise TypeError(f'Source {source!r} is not iterable')
        self.source = source
        self._reducer = None

    @staticmethod
    def merge(first, second):
        return first._copy_with(fns=second.fn_list, source=second.source, reducer=second._reducer)

    def _has_data(self):
        return self.source is not None

    def _copy_with(self, *, fns=None, source=None, reducer=None):
        if source is not None and self.source is not None:
            raise TypeError('Cannot add additional data source to Composable that already has a data source')
        if reducer is not None and self._reducer is not None:
            raise TypeError('Cannot add additional reducer to Composable that already has a reducer')
        result = Composable(self.fn_list + (fns or []), source=self.source or source)
        result._reducer = reducer
        return result._return_or_reduce()

    def apply_all_fns(self, arg):
        result = arg
        for fn, _ in self.fn_list:
            result = fn(result)
        return result

    def result_iterable_for_single_value(self, initial):
        results_unnested = True
        result = initial
        for fn, transform_type in self.fn_list:
            if results_unnested:  # Result is still an unwrapped value
                if transform_type is MAP_TRANSFORM:
                    result = fn(result)
                elif transform_type is FILTER_TRANSFORM:
                    if not fn(result):
                        return ()
                elif transform_type is FLAT_MAP_TRANSFORM:
                    result = fn(result)
                    results_unnested = False
            else:
                if not result:
                    return result  # Bail if there are no items left
                if transform_type is MAP_TRANSFORM:
                    result = [fn(i) for i in result]
                elif transform_type is FILTER_TRANSFORM:
                    result = [i for i in result if fn(i)]
                elif transform_type is FLAT_MAP_TRANSFORM:
                    new_result = []
                    for i in result:
                        new_result.extend(fn(i))
                    result = new_result

        return (result,) if results_unnested else result

    def _return_or_reduce(self):
        return self if self._reducer is None or self.source is None else self._reducer(iter(self))

    def __call__(self, arg=None):
        if arg is None and not self._has_data():
            raise TypeError('Composable must be provided with an iterable data source or an argument')
        if arg is not None and self._has_data():
            raise TypeError('Composable that has been provided with an iterable data source cannot accept an argument')
        if self._has_data():
            return type(self.source)(iter(self))
        elif self._is_simple_pipeline():
            return self.apply_all_fns(arg)
        else:
            return list(self.result_iterable_for_single_value(arg))

    def __rshift__(self, other):
        reducer = fns = source = None
        if isinstance(other, Composable):
            reducer = other._reducer
            fns = other.fn_list
            source = other.source
        if isinstance(other, Adapter):
            return Composable([], source=other.fn(self))
        elif callable(other):
            fns = [other]
        if reducer or fns or source:
            return self._copy_with(fns=fns, reducer=reducer, source=source)
        else:
            return NotImplemented

    def __rrshift__(self, other):
        if self.source is not None:
            raise ValueError("Cannot pipe into a Composable that already has a data source")
        elif callable(other):
            return self.merge(Composable([other]), self)
        else:
            try:
                return self.merge(Composable([], source=other), self)
            except TypeError:
                pass
            raise TypeError(f'Cannot pipe non-iterable {other!r} into Composable')

    def _is_simple_pipeline(self):
        return all(fn.transform_type is MAP_TRANSFORM for fn in self.fn_list)

    def __iter__(self):
        if self.source is None:
            raise ValueError('Cannot iterate over Composable with no data source')

        if self._is_simple_pipeline():
            # map is faster, but can't handle filters/flat_maps
            return map(self.apply_all_fns, self.source)
        return chain.from_iterable(self.result_iterable_for_single_value(i) for i in self.source)

    def __gt__(self, other):
        return self._copy_with(reducer=other)


def composable(arg, as_data=None):
    if callable(arg) and as_data is not True:
        result = Composable([arg])
        update_wrapper(result, arg)
        return result
    elif as_data is not False:
        return Composable([], source=arg)
    else:
        raise TypeError(f'Cannot create function Composable from noncallable argument {arg!r}')

