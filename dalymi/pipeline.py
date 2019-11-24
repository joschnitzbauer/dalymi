import argparse
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, wait, FIRST_COMPLETED
from functools import wraps
from itertools import chain
import logging
import pprint


# @TODO:
# - make all strings use double quots (all other files)
# - Refine log messages (use task instead of function)
# - Update docu
# - Write tests for threaded execution (move it to bash script)
# - New dot file creation
# - Enable process pool executor: currently fails, update docu, write tests


def log(message):
    """Logs the supplied message to a Python logger named `__name__` on log level `INFO`."""
    logger = logging.getLogger(__name__)
    logger.info(message)


class Task:

    def __init__(self, func):
        self.func = func
        self.input = set()
        self.output = set()

    def __str__(self):
        return self.func.__name__

    def __repr__(self):
        return self.func.__name__

    def add_input(self, input):
        if self.input:
            raise ValueError("Task <{}> already has input defined.".format(self.func.__name__))
        self.input.update(input)

    def add_output(self, output):
        if self.output:
            raise ValueError("Task <{}> already has output defined.".format(self.func.__name__))
        self.output.update(output)

    def complete(self, context):
        """Returns True if all output resources exist, False otherwise."""
        return all([resource._check(context) for resource in self.output])

    def ready(self, context):
        """Returns True if all input resources exist, False otherwise."""
        return all([resource._check(context) for resource in self.input])

    def run(self, context):
        """Convenience function to facilitate logging."""
        log("Running <{}>.".format(self))
        self.func(**context)
        log("<{}> finished.".format(self))

    def undo(self, context):
        """Deletes all possible output."""
        log("Undoing <{}>.".format(self))
        for resource in self.output:
            if resource._check(context):
                loc = resource.loc.format(**context)
                log("Deleting <{}> at '{}'.".format(resource.name, loc))
                resource._delete(context)


class Pipeline:
    """The main API to generate dalymi pipelines."""

    def __init__(self):
        self.tasks = {}

    def input(self, *input):
        """
        A decorator to specify input resources for the decorated task.

        !!! warning
            A potential `output` decorator **must** wrap an `input` decorator to ensure correct pipeline functionality.

        # Arguments
        *input: a list of resource objects
        """
        def decorator(func):
            task_name = func.__name__

            @wraps(func)
            def func_wrapped(**context):
                log("Loading inputs {}.".format([resource.name for resource in input]))
                input_dict = {resource.name: resource._load(context) for resource in input}
                kwargs = {**input_dict, **context}
                results = func(**kwargs)
                return results

            log("Registering <{}> as a consumer function.".format(task_name))
            if task_name in self.tasks:
                # Input decorators should always be the inner decorator and inner decorators are executed first.
                # Hence, there should not be a task with the same name already:
                raise KeyError("A task with name <{}> already exists".format(task_name))
            else:
                task = Task(func_wrapped)
                log("Registering <{}> as input to <{}>.".format([resource.name for resource in input], task_name))
                task.add_input(input)
                self.tasks[task_name] = task

            return func_wrapped
        return decorator

    def output(self, *output):
        """
        A decorator to specify output resources for the decorated task.

        !!! warning
            The `output` decorator **must** wrap a potential `input` decorator to ensure correct pipeline
            functionality.

        # Arguments
        *output: a list of resource objects
        """
        def decorator(func):
            task_name = func.__name__

            @wraps(func)
            def func_wrapped(**context):
                results = func(**context)
                if not isinstance(results, tuple):
                    results = (results,)
                log("Saving outputs of function <{}>.".format(task_name))
                task = self.tasks[task_name]
                for resource, result in zip(task.output, results):
                    resource._save(result, context)
                return results

            log("Registering <{}> as a producer function.".format(task_name))
            if task_name in self.tasks:
                # Task could have been created by an inner input decorator.
                # Hence, we need to update the wrapped function, so that also output code is run on execution:
                task = self.tasks[task_name]
                task.func = func_wrapped
            else:
                # Function has no input decorator, which is fine. We create a new task:
                task = Task(func_wrapped)
                self.tasks[task_name] = task
            log("Registering <{}> as producer of <{}>.".format(task_name, [resource.name for resource in output]))
            task.add_output(output)

            return func_wrapped
        return decorator

    def get_completed(self, context):
        """Returns all tasks that are completed."""
        return [task for task in self.tasks.values() if task.complete(context)]

    def get_producer(self, resource):
        """Finds the producing task for a resource."""
        for task in self.tasks.values():
            if resource in task.output:
                return task
        # if we arrive here, no producer was found
        raise ValueError("No producer found for task <{}>.".format(task.func.__name__))

    def get_ready(self, context):
        """Returns all tasks that are ready to run."""
        return [task for task in self.tasks.values() if task.ready(context)]

    def get_upstream(self, task, already_found=set()):
        """Recursively finds all tasks that are required to run so that all inputs for the given task are present."""
        input_producers = set([self.get_producer(resource) for resource in task.input])
        already_found.update(input_producers)
        for input_producer in input_producers:
            self.get_upstream(input_producer, already_found=already_found)
        return already_found

    def cli(self):
        """Runs the default command line interface of this `Pipeline`."""
        pipeline_cli = PipelineCLI(self)
        pipeline_cli.run()

    def dot(self, T="pdf"):
        # @TODO: rewrite

        dot = "digraph pipeline {\n"
        for func in self.funcs:
            dot += "\t{} [fontsize=13]\n".format(func)
        for resource, func in self.producers.items():
            table = "<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\">"
            table += "<TR><TD bgcolor=\"grey\">{}</TD></TR>".format(resource.name)
            if hasattr(resource, "columns") and resource.columns is not None:
                for column in resource.columns:
                    table += "<TR><TD>{}</TD></TR>".format(column)
            table += "</TABLE>>"
            dot += "\t{} [label={} fontsize=12 height=0 margin=0 shape=none width=0]".format(resource.name, table)
            # the edge:
            dot += "\t{} -> {}\n".format(func.__name__, resource.name)
        # edges for consumers:
        for resource_name, func_name in self.consumers:
            dot += "\t{} -> {}\n".format(resource_name, func_name)
        dot += "}\n"
        with open("pipeline.dot", "w") as f:
            f.write(dot)

    def ls(self):
        tasks = list(self.tasks.keys())
        msg = "Tasks in pipeline:\n"
        for task in tasks:
            msg += "\t{}\n".format(task)
        print(msg, end="")

    def run(self, task=None, parallelism=None, **context):
        context["task"] = task
        allowed_parallelism = [None, "none", "threads", "processes"]
        if parallelism not in allowed_parallelism:
            raise ValueError("Parallelism has to be one of {}".format(allowed_parallelism))
        parallelism = None if parallelism == "none" else parallelism
        context["parallelism"] = parallelism
        pretty_context = pprint.pformat(context)
        pretty_indented_context = "\n".join(["  " + _ for _ in pretty_context.split("\n")])
        log("Running with context:\n" + pretty_indented_context)
        if task is not None:
            task_instance = self.tasks[task]
            to_complete = set([task_instance]) | self.get_upstream(task_instance)
        else:
            to_complete = self.tasks.values()
        to_complete = set(to_complete)
        log("Tasks to be completed: {}".format(list(to_complete)))
        if parallelism:
            workers = context["workers"]
            submitted = set()
            futures = dict()  # keys: futures, values: tasks
        if parallelism == "threads":
            log("Activating threaded mode with {} threads.".format(workers))
            executor = ThreadPoolExecutor(max_workers=workers)
        if parallelism == "processes":
            log("Activating multiprocessing mode with {} workers.".format(workers))
            executor = ProcessPoolExecutor(max_workers=workers)
        while True:
            completed = set(self.get_completed(context))
            log("Completed tasks: {}".format(list(completed)))
            if set(completed) == set(to_complete):
                log("All tasks completed.")
                break
            ready = self.get_ready(context)
            log("Tasks with all required input: {}".format(list(ready)))
            to_start = (set(to_complete) - set(completed)) & set(ready)
            log("Tasks to be executed: {}".format(list(to_start)))
            if parallelism:
                to_start -= submitted
                log("Submitting tasks {} to the worker pool.".format(list(to_start)))
                new_futures = {executor.submit(task.run, context): task for task in to_start}
                submitted.update(to_start)
                futures.update(new_futures)
                log("Waiting for a task to finish.")
                done, not_done = wait(futures.keys(), return_when=FIRST_COMPLETED)
                single_done = tuple(done)[0]
                # finished_task = futures[single_done]  # we may use this at some point
                # remove the future so that we don"t get it returned by the waiting function anymore:
                del futures[single_done]
            else:
                to_run = tuple(to_start)[0]
                to_run.run(context)
                # finished_task = to_run  # we may use this at some point

    def get_consumers(self, resource):
        """Finds consuming tasks of a resource."""
        return [task for task in self.tasks.values() if resource in task.input]

    def get_downstream(self, task, already_found=set()):
        """Recursively finds all tasks downstream of the given task."""
        output_consumers = set(chain(*[self.get_consumers(resource) for resource in task.output]))
        already_found.update(output_consumers)
        for output_consumer in output_consumers:
            self.get_downstream(output_consumer, already_found=already_found)
        return already_found

    def undo(self, task=None, downstream=False, **context):
        context["task"] = task
        context["downstream"] = downstream
        pretty_context = pprint.pformat(context)
        pretty_indented_context = "\n".join(["  " + _ for _ in pretty_context.split("\n")])
        log("Undoing with context:\n" + pretty_indented_context)
        if task and downstream:
            task_instance = self.tasks[task]
            to_undo = set([task_instance]) | self.get_downstream(task_instance)
        elif task and not downstream:
            to_undo = [self.tasks[task]]
        else:
            to_undo = list(self.tasks.values())
        log("Undoing tasks {}.".format(list(to_undo)))
        for task_instance in to_undo:
            task_instance.undo(context)


class PipelineCLI():
    """
    A class representing the command line interface of a `Pipeline`.

    # Arguments
    pipeline (dalymi.pipeline.Pipeline): the pipeline object to create a CLI for.

    # Attributes
    run_parser (argparse.ArgumentParser): handles the `run` sub-command
    dot_parser (argparse.ArgumentParser): handles the `dot` sub-command
    ls_parser (argparse.ArgumentParser): handles the `ls` sub-command
    """

    def __init__(self, pipeline):
        self.pipeline = pipeline

        self.parser = argparse.ArgumentParser()
        self.subparsers = self.parser.add_subparsers(dest="command")

        self.run_parser = self.subparsers.add_parser("run", help="run the pipeline")
        self.run_parser.add_argument("-t", "--task", help="run/undo a specific task")
        self.run_parser.add_argument("-p", "--parallelism", help="none, threads or processes",
                                     choices=["none", "threads", "processes"], default="none")
        self.run_parser.add_argument("-w", "--workers", help="number of workers for parallelism", type=int)

        self.dot_parser = self.subparsers.add_parser("dot", help="create a graphviz dot file of the DAG")

        self.ls_parser = self.subparsers.add_parser("ls", help="list pipeline tasks")

    def run(self, external_context={}):
        """
        Parses arguments and runs the provided command.
        """
        undo_parser = self.subparsers.add_parser("undo", parents=[self.run_parser], add_help=False,
                                                 description="undo tasks")
        undo_parser.add_argument("-d", "--downstream", action="store_true", help="undo downstream tasks")
        args = self.parser.parse_args()
        context = {**external_context, **vars(args)}
        if args.command == "run":
            self.pipeline.run(**context)
        elif args.command == "undo":
            self.pipeline.undo(**context)
        elif args.command == "dot":
            self.pipeline.dot()
        elif args.command == "ls":
            self.pipeline.ls()
        else:
            self.parser.print_help()
