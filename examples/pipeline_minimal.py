import pandas as pd
from dalymi import Pipeline, PandasDataFrameResource

pl = Pipeline(verbose_during_setup=True)


some_df = PandasDataFrameResource('some_df', 'some_file.csv')
final_df = PandasDataFrameResource('final_df', 'final_file.csv')


@pl.output(some_df)
def first(**context):
    return pd.DataFrame()


@pl.output(final_df)
@pl.input(some_df)
def second(some_df, **context):
    return some_df


if __name__ == '__main__':
    pl.cli()
