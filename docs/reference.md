<h1 id="dalymi.pipeline">dalymi.pipeline</h1>


<h1 id="dalymi.pipeline.Pipeline">Pipeline</h1>

```python
Pipeline(self)
```

The main API to generate dalymi pipelines.

<h1 id="dalymi.pipeline.Pipeline.input">input</h1>

```python
Pipeline.input(self, *input)
```

A decorator to specify input resources for the decorated task.

!!! warning
    A potential `output` decorator **must** wrap an `input` decorator to ensure correct pipeline functionality.

__Arguments__

- __*input__: a list of resource objects

<h1 id="dalymi.pipeline.Pipeline.output">output</h1>

```python
Pipeline.output(self, *output)
```

A decorator to specify output resources for the decorated task.

!!! warning
    The `output` decorator **must** wrap a potential `input` decorator to ensure correct pipeline
    functionality.

__Arguments__

- __*output__: a list of resource objects

<h1 id="dalymi.pipeline.Pipeline.cli">cli</h1>

```python
Pipeline.cli(self)
```

Runs the default command line interface of this `Pipeline`.

<h1 id="dalymi.pipeline.PipelineCLI">PipelineCLI</h1>

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

<h2 id="dalymi.pipeline.PipelineCLI.run">run</h2>

```python
PipelineCLI.run(self)
```

Parses arguments and runs the provided command.

<h1 id="dalymi.resources">dalymi.resources</h1>


<h2 id="dalymi.resources.LocalFileMixin">LocalFileMixin</h2>

```python
LocalFileMixin(self, /, *args, **kwargs)
```

Provides default `check` and `delete` methods for local file resources.
Inherit from this before other resource classes to avoid `NotImplementedError`.

<h3 id="dalymi.resources.LocalFileMixin.makedirs">makedirs</h3>

```python
LocalFileMixin.makedirs(self, path)
```

Convenience method to recursively create directories for a path if it does not exist.
Could be called, for example, in the `save` method of a sub-class.

