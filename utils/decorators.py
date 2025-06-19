import functools
import polars as pl

def handle_data_errors(func):  # This is the decorator factory
    @functools.wraps(func)     # Preserves metadata (like function name and docstring)
    def wrapper(*args, **kwargs):  # This is the actual wrapped function
        try:
            return func(*args, **kwargs)  # Call the original function
        except FileNotFoundError as e:
            print(f"[File Error] {e}")
        except pl.ComputeError as e:
            print(f"[Polars Error] {e}")
        except ValueError as e:
            print(f"[Value Error] {e}")
        except Exception as e:
            print(f"[Unexpected Error] {e}")
        return []  # If any error occurs, return an empty list as a fallback
    return wrapper  # Return the wrapper function to replace the original
