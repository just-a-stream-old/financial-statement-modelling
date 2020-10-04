import timeit


def log_time(method):

    def wrapper(*args, **kwargs):
        start = timeit.default_timer()
        result = method(*args, **kwargs)
        end = timeit.default_timer()
        print(f"Method: {method.__name__} executed in: {'%.3f' % (end - start)} seconds")
        return result

    return wrapper
