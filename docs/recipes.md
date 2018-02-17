# Recipes

## Custom command line arguments
The default command line interface can be executed by calling the `cli` method of a `Pipeline` object. However, to add
custom commands or arguments, one needs to wrap the `Pipeline` object with a `PipelineCLI` object and call its `run`
method instead:

``` python
from dalymi import Pipeline, PipelineCLI

pl = Pipeline()

''' add pipeline definition here '''

if __name__ == '__main__':
    cli = PipelineCLI(pl)

    ''' add custom commands and arguments here '''

    cli.run()
```

The `PipelineCLI` object has several attributes to add custom functionality, most importantly:

- `parser`: an instance of `argparse.ArgumentParser` handling the global interface.
- `subparsers`: the special action object returned by `parser.add_subparsers(dest='command')`.
- `run_parser`: an instance of `argparse.ArgumentParser` handling the `run` subcommand.
- `ls_parser`: an instance of `argparse.ArgumentParser` handling the `ls` subcommand.
- `dot_parser`: an instance of `argparse.ArgumentParser` handling the `dot` subcommand.

There is no object to handle the `undo` subcommand as it is auto-generated during runtime, so that the `undo`'s
arguments match the ones of `run`, even if custom arguments were added to `run`. In addition, the `-d`/`--downstream`
option is added to `undo`.

The above listed objects can be used as in regular `argparse` command line interfaces. So, additional arguments could
be added to the subcommand parsers (e.g. `run_parser`) or further subcommands to `subparsers`.
Any parsed commands and arguments are added during runtime to the `context` dictionary which has to be accepted by
pipeline task functions and is used to format resource location templates. This makes default **and** custom arguments
available as variables in pipeline tasks during execution.

See below for specific examples of adding custom functionality to the command line interface.

### Execution date
Continuing from the code snippet above, the following describes how to add an "execution date" argument to the command
line interface. This can be useful to simulate pipeline executions in the past or future, and to store resources
at locations specific to the actual execution date of the program (see "Templating resource locations" below for more
details).

Before calling the `run` method of the `PipelineCLI` object:

``` python
cli.run_parser.add_argument('-e', '--execution-date', default=pd.Timestamp('today').date(),
                            type=lambda x: pd.to_datetime(x).date(), help='the date of execution'))
```

If the command line option is not used explicitely, today's date is used as default. With the `type` keyword argument,
we convert the provided value to a `datetime` object. Any string compatible with this transformation is hence valid,
e.g. '2018-01-30'.

Now, the `context` dictionary passed to each pipeline function will contain an entry with key `'execution_date'` and a
`datetime` object as value (`argparse` converts hyphens to underscores to ensure valid Python naming).

## Custom resource classes

_dalymi_ ships with a default set of resource classes (see `dalymi.resources`), most notably `PandasCSV` and `Pickle`.
These classes are ready to go and provide all the functionality a resource needs for pipeline execution. However,
custom resources can be easily defined by subclassing `dalymi.resources.Resource` and overriding key methods:

``` python
from dalymi.resources import Resource

class CustomResource(Resource):

    def check(self, path):
        # custom code to check whether this type of resource exists at location `path`.

    def delete(self, path):
        # custom code to delete this type of resource at location `path`.
        # only required when using the `undo` command line interface.

    def load(self, path):
        # custom code to load this type of resource from location `path`.
        # only required if this resource type is being used as task input.

    def save(self, path, data):
        # custom code to save the `data` object as this resource type at location `path`.
        # only required if this resource type is being used as a task output.
```

Since any i/o functionality of resources can be customized like this, resources can be any type of object. Most
straightforward are files stored locally, but it could as well be database entries, remote files or anything else that
is touchable with Python code.

### Local files

For convenience, custom local files can inherit, in addition to `Resource`, from `dalymi.resource.LocalFileMixin` which
provides sensible implementations of the `check` and `delete` methods. Example for a `matplotlib` figure:

``` python
from dalymi.resources import LocalFileMixin, Resource

class FigureResource(LocalFileMixin, Resource):

    # `LocalFileMixin` provides `check` and `delete` methods.

    def save(self, path, figure):
        return figure.savefig(path)

    # we omit the `load` method since this class is only intended for task output, not input.
```

!!! warning
    `LocalFileMixin` has to be inherited **before** anything else. Otherwise the Python "Method Resolution Order" does
    not find the relevant object methods.


### Pandas DataFrames

Since `pandas.DataFrames` are so essential for data processing pipelines, they hold a special status within `dalymi`,
mostly for the functionality to specify their columns during resource definition. Recall the example from the tutorial:

``` python
from dalymi.resources import PandasCSV

squares_resource = PandasCSV(name='squares', loc='squares.csv', columns=['number', 'square'])
```

`PandasCSV` objects represent `pandas.DataFrame`s that are stored as local files in CSV format. In addition, during
i/o operations, _dalymi_ asserts whether the data frame has the expected columns (here `['number', 'square']`) and
raises an `AssertionError` if not.

Custom storing methods can be defined for `pandas.DataFrame` by subclassing `dalymi.resources.PandasDF`. Subclasses of
this type retain the assertion functionality for the data frame columns, but must implement their own `check`,
`delete`, `save` and `load` methods. In fact `dalymi.resources.PandasCSV` is itself a subclass of `PandasDF`.

Column assertion can be turned off for `PandasDF` type classes by instatiating the object with keyword argument
`columns=None`. In this case, column assertions are generally ignored.

!!! note
    An additional benefit of specifying data frame columns is that column names can be represented in pipeline graphs
    using the `dot` command line interface.

!!! note
    The current implementation of `PandasDF` works for any object that holds a `column` attribute which enlists the
    object columns. Hence, it could be used for other classes too. However, future development of _dalymi_ might not
    guarantee that `PandasDF` objects do not rely further attributes specific to `pandas.DataFrame`. Use with care.


## Logging
Pipeline internals are logged using the Python `logging` module with level `logging.INFO`. Logging messages include
which tasks _dalymi_ attempts to run, which inputs are being loaded, which tasks are skipped, because input exists,
etc.
By default, Python does not log messages with level `logging.INFO`. So, to make _dalymi_ pipeline internals verbose,
set the logging level before pipeline definition to `logging.INFO` or lower. For example:

``` python
import logging

logging.basicConfig()
logging.getLogger('dalymi').setLevel(logging.INFO)
```

Since we specified `'dalymi'` in `getLogger`, this setting will only affect the `dalymi` Python package.

## Templating resource locations
When defining resources, the `loc` keyword argument can be a templated string using standard Python curly brackets
format. Upon i/o operations on the resource, the string is formatted using the `context` dictionary. This allows for
parameter-specific resource location for reproducable data output. E.g.:

``` python
from dalymi.resources import Pickle

model = Pickle(name='model', loc='data/{execution_date}/model.pkl')
```

In this case, when `model` is saved or loaded, the provided `path` to the `save` and `load` methods of the resource is
`'data/{execution_date}/model.pkl'.format(**context)`, where `context` is the _dalymi_ runtime dictionary.
Obviously, `execution_date` must be present in the dictionary which can be achieved by adding it as a custom command
line argument to the `run_parser` (see above).

The `undo` CLI command will be able to find the same file and delete it when being called using the same command line
arguments as the `run` command. This will always be possible since custom arguments added to the `run` command will
automatically be added to the `undo` command.

In this example, one could think of the pipeline being executed daily and each day, a freshly trained machine learning
model would be pickled and saved in a folder of the current date.

## Custom assertions
During i/o operations on pipeline resources, _dalymi_ can run a set of assertions on the resource. This can be quite handy to check whether pipeline data is as expected.

Custom resources, sub-classed from `resources.Resource` can implement assertions by submitting a list of functions to the `__init__` method of `resources.Resource` (keyword argument `assertions`). The submitted functions will be executed during loading/saving of the resource a must accept a single argument: the data object of the resource itself. The return value of the assertion function is ignored, so any error raising or logging should be handled in the assertion function itself, e.g. by using the `assert` statement.

Other default resources shipped with _dalymi_ also accept the `assertion` keyword argument in their `__init__` method.

Here is an example to assert the existence of null values in a Pandas dataframe:

``` python
def none_null(df):
    ''' Asserts that a dataframe does not contain any nulls. '''
    assert df.isnull().sum().sum() == 0, 'Dataframe contains nulls.'

prepared = resources.PandasCSV(name='prepared',
                               loc='data/prepared.csv',
                               columns=['sepal_length', 'sepal_width', 'petal_length', 'petal_width'],
                               assertions=[none_null])
```
