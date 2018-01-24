import pandas as pd
from dalymi import Pipeline
from dalymi.resources import PandasCSV

pl = Pipeline(verbose_during_setup=True)


first_df = PandasCSV('first_df', 'first_df.csv')
second_df = PandasCSV('second_df', 'second_df.csv')


@pl.output(first_df)
def first(**context):
    return pd.DataFrame({'a': range(5)})


@pl.output(second_df)
@pl.input(first_df)
def second(first_df, **context):
    return first_df


if __name__ == '__main__':
    pl.cli()
