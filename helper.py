import timeit


def log_time(function):

    def wrapper(*args, **kwargs):
        start = timeit.default_timer()
        result = function(*args, **kwargs)
        stop = timeit.default_timer()
        print("The function", function.__name__, " took %.3f" % (stop - start))
        return (stop - start), result, function.__name__

    return wrapper
