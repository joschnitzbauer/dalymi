from dalymi import Pipeline
from dalymi.resources import PandasCSV
import pandas as pd


# Define resources:
numbers_resource = PandasCSV(name='numbers', loc='numbers.csv', columns=['number'])
squares_resource = PandasCSV(name='squares', loc='squares.csv', columns=['number', 'square'])


# Define the pipeline
pl = Pipeline()


@pl.output(numbers_resource)
def create_numbers(**context):
    return pd.DataFrame({'number': range(11)})


@pl.output(squares_resource)
@pl.input(numbers_resource)
def square_numbers(numbers, **context):
    numbers['square'] = numbers['number']**2
    return numbers


if __name__ == '__main__':
    # run the default command line interface
    pl.cli()
