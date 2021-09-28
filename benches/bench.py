from random import randint
from time import perf_counter

from src.compose import composable, as_filter


@composable
def add_one(x):
    return x + 1


def noncomposable_add_one(x):
    return x + 1


def double(x):
    return x * 2


def time(fn, data):
    start = perf_counter()
    fn(data)
    elapsed = perf_counter() - start
    print(f'Ran {fn.__name__} in {elapsed:.2f} seconds')
    return elapsed


def generators(source):
    added = (add_one(x) for x in source)
    doubled = (double(x) for x in added)
    added_again = (add_one(x) for x in doubled)
    doubled_again = (double(x) for x in added_again)
    return [add_one(x) for x in doubled_again]


def lists(source):
    added = [add_one(x) for x in source]
    doubled = [double(x) for x in added]
    added_again = [add_one(x) for x in doubled]
    doubled_again = [double(x) for x in added_again]
    return [add_one(x) for x in doubled_again]


def composed(source):
    return source >> add_one >> double >> add_one >> double >> add_one > list


numbers = [randint(0, 10 ** 5) for _ in range(1_000_000)]
assert time(composed, numbers) < time(generators, numbers)

numbers = [randint(0, 10 ** 5) for _ in range(1_000_000)]
assert time(composed, numbers) < time(lists, numbers)

numbers = [randint(0, 10 ** 5) for _ in range(1_000_000)]
assert time(generators, numbers) > time(composed, numbers)

numbers = [randint(0, 10 ** 5) for _ in range(1_000_000)]
assert time(lists, numbers) > time(composed, numbers)


def generators(source):
    added = (add_one(x) for x in source)
    doubled = (double(x) for x in added if x % 3 == 0)
    added_again = (add_one(x) for x in doubled)
    doubled_again = (double(x) for x in added_again if x % 3 == 0)
    return [add_one(x) for x in doubled_again]


def lists(source):
    added = [add_one(x) for x in source]
    doubled = [double(x) for x in added if x % 3 == 0]
    added_again = [add_one(x) for x in doubled]
    doubled_again = [double(x) for x in added_again if x % 3 == 0]
    return [add_one(x) for x in doubled_again]


def composed(source):
    return source >> add_one >> as_filter(lambda x: x % 3 == 0) >> double >> add_one >> as_filter(lambda x: x % 3 == 0) >> double >> add_one > list


numbers = [randint(0, 10 ** 5) for _ in range(1_000_000)]
time(composed, numbers)
time(generators, numbers)

numbers = [randint(0, 10 ** 5) for _ in range(1_000_000)]
time(composed, numbers)
time(lists, numbers)

numbers = [randint(0, 10 ** 5) for _ in range(1_000_000)]
assert time(generators, numbers) > time(composed, numbers)

numbers = [randint(0, 10 ** 5) for _ in range(1_000_000)]
assert time(lists, numbers) > time(composed, numbers)