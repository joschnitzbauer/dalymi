# dalymi

*[data like you mean it]*

[![Documentation Status](https://readthedocs.org/projects/dalymi/badge/?version=latest)](http://dalymi.readthedocs.io/en/latest/?badge=latest) ![](https://github.com/joschnitzbauer/dalymi/workflows/tests/badge.svg?branch=master)

A lightweight, data-focused and non-opinionated pipeline manager written in and for Python.

--------------------------------------------------------------------------------

_dalymi_ allows to build data processing pipelines as [directed acyclic graphs]([https://en.wikipedia.org/wiki/Directed_acyclic_graph]) (DAGs) and facilitates rapid, but controlled, model development. The goal is to prototype quickly, but scale to production with ease.
To achieve this, _dalymi_ uses "make"-style workflows, _i.e._ tasks with missing input trigger the execution of input-producing tasks before being executed themselves. At the same time, _dalymi_ provides fine control to run and undo specific pipeline parts for quick test iterations. This ensures output reproducability and minimizes manual errors.

Several features facilitate _dalymi_'s goal:

- simple, non-opinionated API (most choices left to user)
- no external dependencies for pipeline execution
- one-line installation (ready for use)
- no configuration
- auto-generated command line interface for pipeline execution
- quick start, but high flexibility to customize and extend:
    - task output can be stored in any format Python can touch (local files being the default)
    - customizable command line arguments
    - templated output location (e.g. timestamped files)
    - support for automated checks on data integrity during runtime
- DAG visualization using [graphviz](https://www.graphviz.org/)
- API design encourages good development practices (modular code, defined data schemas, self-documenting code, easy workflow viz, etc.)

## Installation
_dalymi_ requires Python >= 3.5.

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
simple.py:
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
    # Run the default command line interface
    pl.cli()
```

Command line:
```bash
python simple.py run     # executes the pipeline. skips tasks for which output already exists.
```

More examples can be found [here](https://github.com/joschnitzbauer/dalymi/tree/master/examples).

## Roadmap
- More docstrings
- Unit tests
- Action to publish on PyPI on new release
- Parallel task processing
- REST API during pipeline run
- Web interface for pipeline run

## Warranty
Although _dalymi_ is successfully used in smaller applications, it is not battle-tested yet and lacks unit tests. If you decide to use it, be prepared to communicate issues or fix bugs (it's not a lot of code... :)).

## Contributions
... are welcome!
