import pandas as pd
from dalymi import IO


io = IO()


@io.save_output(df='hello_world.csv')
def say_hello():
    return {'df': pd.DataFrame()}


if __name__ == '__main__':
    say_hello()
