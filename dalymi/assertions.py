from functools import wraps


def assert_input(*resources):
    @wraps
    def decorator(func):
        def func_wrapped(*args, **kwargs):
            for resource, arg in zip(resources, args):
                resource.assert_integrity(arg)
            output = func(*args, **kwargs)
            return output
        return func_wrapped
    return decorator


def assert_output(*resources):
    @wraps
    def decorator(func):
        def func_wrapped(*args, **kwargs):
            output = func(*args, **kwargs)
            if isinstance(output, tuple):
                results = output
            else:
                results = (output, )
            for resource, result in zip(resources, results):
                resource.assert_integrity(result)
            return output
        return func_wrapped
    return decorator


def assert_input_output(resources_in, resources_out):
    @wraps
    def decorator(func):
        def func_wrapped(*args, **kwargs):
            return assert_output(assert_input(func(*args, **kwargs)))
        return func_wrapped
    return decorator
