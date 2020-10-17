import timeit
from datetime import datetime, timedelta


def log_time(method):
    def wrapper(*args, **kwargs):
        start = timeit.default_timer()
        result = method(*args, **kwargs)
        end = timeit.default_timer()
        print(f"Method: {method.__name__} executed in: {'%.3f' % (end - start)} seconds")
        return result

    return wrapper


def map_to_weekday_datetime(date):
    datetime_object = parse_datetime_object(date)

    day_of_the_week = datetime_object.weekday()

    if day_of_the_week < 5:
        return datetime_object

    else:
        return datetime_object + timedelta(days=2)


def parse_datetime_object(date):
    try:
        return datetime.strptime(date.split(" ")[0], "%Y-%m-%d")
    except:
        return datetime(2050, 1, 1, 0, 0)
