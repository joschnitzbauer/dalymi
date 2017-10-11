import dalymi as di
import pandas as pd


pl = di.Pipeline(pd.read_csv, lambda df: df.to_csv)


@pl.save_output({'some_df': 'some_file.csv'})
def first(**context):
    return {'some_df': pd.DataFrame()}


@pl.save_output({'final_df': 'final_file.csv'})
@pl.ensure_dependencies(first)
def second(some_df, **context):
    return {'final_df': some_df}


if __name__ == '__main__':
    pl.run(locals())
