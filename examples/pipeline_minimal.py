import pandas as pd
from dalymi import Pipeline

pl = Pipeline(verbose_during_setup=True)


@pl.io(output={'some_df': 'some_file.csv'})
def first(**context):
    return {'some_df': pd.DataFrame()}


@pl.io(input=['some_df'],
       output={'final_df': 'final_file.csv'})
def second(some_df, **context):
    return {'final_df': some_df}


if __name__ == '__main__':
    pl.run()
