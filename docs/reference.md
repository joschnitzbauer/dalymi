<h1 id="dalymi.pipeline">dalymi.pipeline</h1>


<h2 id="dalymi.pipeline.Pipeline">Pipeline</h2>

```python
Pipeline(self)
```

The main API to generate dalymi pipelines.

<h3 id="dalymi.pipeline.Pipeline.input">input</h3>

```python
Pipeline.input(self, *input)
```

A decorator to specify input resources for the decorated task.

__Arguments__

- __*input__: a list of resource objects

<h3 id="dalymi.pipeline.Pipeline.cli">cli</h3>

```python
Pipeline.cli(self)
```

Runs the default command line interface of this `Pipeline`.

<h3 id="dalymi.pipeline.Pipeline.log">log</h3>

```python
Pipeline.log(self, message)
```

Logs the supplied message to a Python logger named `__name__` on log level `INFO`.

<h1 id="dalymi.resources">dalymi.resources</h1>


<h2 id="dalymi.resources.LocalFileMixin">LocalFileMixin</h2>

```python
LocalFileMixin(self, /, *args, **kwargs)
```

Provides default `check` and `delete` methods for local file resources.
Inherit from this before other resource classes to avoid `NotImplementedError`.

