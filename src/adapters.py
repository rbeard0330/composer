from src.compose import adapter
import itertools


def dropwhile(pred):
    return adapter(lambda it: itertools.dropwhile(pred, it))


def take(n):
    return adapter(lambda it: it[:n])
