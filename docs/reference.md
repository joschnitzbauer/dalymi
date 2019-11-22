# dalymi.pipeline

## Pipeline
```python
Pipeline(self)
```

The main API to generate dalymi pipelines.

### input
```python
Pipeline.input(self, *input)
```

A decorator to specify input resources for the decorated task.

!!! warning
    A potential `output` decorator **must** wrap an `input` decorator to ensure correct pipeline functionality.

__Arguments__

- __*input__: a list of resource objects

### output
```python
Pipeline.output(self, *output)
```

A decorator to specify output resources for the decorated task.

!!! warning
    The `output` decorator **must** wrap a potential `input` decorator to ensure correct pipeline
    functionality.

__Arguments__

- __*output__: a list of resource objects

### cli
```python
Pipeline.cli(self)
```

Runs the default command line interface of this `Pipeline`.

### log
```python
Pipeline.log(self, message)
```

Logs the supplied message to a Python logger named `__name__` on log level `INFO`.

## PipelineCLI
```python
PipelineCLI(self, pipeline)
```

A class representing the command line interface of a `Pipeline`.

__Arguments__

- __pipeline (dalymi.pipeline.Pipeline)__: the pipeline object to create a CLI for.

__Attributes__

- `run_parser (argparse.ArgumentParser)`: handles the `run` sub-command
- `dot_parser (argparse.ArgumentParser)`: handles the `dot` sub-command
- `ls_parser (argparse.ArgumentParser)`: handles the `ls` sub-command

### run
```python
PipelineCLI.run(self, external_context={})
```

Parses arguments and runs the provided command.

# dalymi.resources

## LocalFileMixin
```python
LocalFileMixin(self, /, *args, **kwargs)
```

Provides default `check` and `delete` methods for local file resources.
Inherit from this before other resource classes to avoid `NotImplementedError`.

### makedirs
```python
LocalFileMixin.makedirs(self, path)
```

Convenience method to recursively create directories for a path if it does not exist.
Could be called, for example, in the `save` method of a sub-class.

