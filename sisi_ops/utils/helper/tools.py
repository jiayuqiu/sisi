import time


def timer(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        elapsed = time.time() - start_time
        # Use your logger to record the elapsed time.
        print(f"{func.__name__} ran in {elapsed:.2f} seconds")
        return result
    return wrapper