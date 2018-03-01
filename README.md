# dalymi

*[data like you mean it]*

[![Documentation Status](https://readthedocs.org/projects/dalymi/badge/?version=latest)](http://dalymi.readthedocs.io/en/latest/?badge=latest)

A lightweight, data-focused and non-opinionated pipeline manager written in and for Python.

## Installation
``` bash
pip install dalymi
```

For the latest development:
``` bash
pip install git+https://github.com/joschnitzbauer/dalymi.git
```

## Documentation
http://dalymi.readthedocs.io/

## Simple example
``` python
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
```

## Roadmap
- API reference
- Unit tests
- Continuous integration
- Parallel task processing
- REST API during pipeline run
- Web interface for pipeline run

## Contributions
... are welcome!
