from contextlib import contextmanager
from time import perf_counter

@contextmanager
def timing(label : str, timings: dict[str,float]):
    """
    Measure and record the execution time of a code block.

    Args:
        label (str): Key name to store timing in the timings dictionary.
        timings (dict[str, float]): Dictionary to save the elapsed time.

    Yields:
        None: Allows timing of the enclosed code block.

    Example:
        timings = {}
        with timing("step1", timings):
            do_something()
        print(timings["step1"])
    """
    start = perf_counter()
    yield
    end = perf_counter()
    duration = end - start
    timings[label] = duration