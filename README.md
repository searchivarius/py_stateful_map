[![PyPI version](https://img.shields.io/pypi/v/py_stateful_map.svg)](https://pypi.python.org/pypi/py_stateful_map/)

# Overview

This is a missing piece of the Python multitask (both threads and processes) API, which:
* Permits enjoyable parallelization of iteration through both unsizeable and sizeable collections with a possibility to display
a progress bar of your choice (i.e., not necessarily ``tqdm``)
* It also supports stateful worker pools, which can be initialized directly in a worker process/thread. 

This package is **dependency-free** and supports both threads and processes. 
Due to GIL-limitations threads provide only limited parallelization as of
now, but this [will change in the foreseeable  future](https://www.reddit.com/r/Python/comments/1bcggx9/disabling_the_gil_option_has_been_merged_into/). 

PS: Note that the use of this package (and multiprocessing map in general) in a Jupyter notebook may require changing
the process starting behavior (see [this sample notebook](examples/py_stateful_map_ex1.ipynb))

# Summary of advantages

A standard worker pools together with map-style distribution of tasks is a convenient abstraction,
but it lacks support for initialization of stateful workers (inside each process
without copying them from the main process) and treats  both sizeable and unsizeable iterators equally. 
As one core task for deep learners, you can load an embedding-generation model to a specific GPU only once.

Because standard map functions do not use the ``__len__`` function even if it is provided by the input iterable, 
one cannot easily display a nice progress bar (going from 0% to 100%).
A nice [pqdm package]() does solve this problem, but it does not support lazy (memory-efficient)
iteration and it has to load all inputs into the memory. ``py_stateful_map`` fixes all these issues
without directly incorporating tqdm.

To summarize ``py_stateful_map`` overcomes the followings shortcomings of the standard API and/or ``pqdm``
 without requiring any additional dependencies:

1. Pain-free support for stateful workers, which are initialized separately in each process using a worker-specific set of arguments (stateless workers are supported as well).
2. Support for lazy, memory-efficient, iterators (unlike ``pqdm`).
3. The package simulates an iterator that has the ``__len__`` function (unlike standard Python map functions) if an input iterable is **sizealbe**.
4. Support for pain free handling of exceptions. Exceptions can be just stored in the return object, immediately fired, or fired after all concurrent tasks are finished.
5. Support for both ordered and **un**ordered return of results (unlike ``pqdm``). Unordered return of results is sometimes more efficient.

# Install & Use

To install

```
pip install py_stateful_map 
```

and use (duplicated from [this sample file](examples/py_stateful_map_ex1.py)):

```
from py_stateful_map import StateFullWorkerPool, WorkerResultWrapper, ExceptionBehavior, ArgumentPassing
# Use of tqdm is optional!
from tqdm import tqdm 

class SimpleArithmeticClassWorker:
    def __init__(self, proc_id):
        print(f'Init. process {proc_id}')
        self.proc_id = proc_id

    def __call__(self, input_arg):
        ret_val = input_arg
        for t in range(100_000):
            ret_val = (ret_val * ret_val) % 337
        return ret_val
        
if __name__ == '__main__':  
    N_TASKS = 1000      
    N_WORKERS = 4
    
    worker_kwarg_dict_arr = []
    for pid in range(N_WORKERS):
        worker_kwarg_dict_arr.append(dict(proc_id=pid))
        
    input_arr = [k * 10 for k in range(N_TASKS)]
    
    tot_res = 0    
    
    with StateFullWorkerPool(num_workers=N_WORKERS,
                         worker_class_or_func=SimpleArithmeticClassWorker,
                         worker_kwarg_dict_arr=worker_kwarg_dict_arr,
                         argument_passing=ArgumentPassing.AS_SINGLE_ARG,
                         exception_behavior=ExceptionBehavior.DEFERRED,
                         join_timeout=1) as proc_pool:
    
        # just marking the type
        result: WorkerResultWrapper
        for result in tqdm(proc_pool.map(input_arr, is_unordered=False)):
            # With deferred exceptions, they are simply returned as a part of the result object
            if result.exception_obj is not None:
                print('Error:', result.exception_obj)
            else:
                tot_res += result.ret_val
                assert result.exception_obj is None
    
    print('Total:', tot_res)
```

A wrapper example file that can be used to play with all possible options
can [be found here](examples/py_stateful_map_demo.py).

# Usage in details

## Passing arguments

Python function support two types of arguments: positional and kwargs (as a key-value dictionary).
The function ``StateFullWorkerPool.map`` reads element from an input iterable object and converts them
into arguments for a worker, accordingly. This is done in three ways (similar to ``pqdm```):

1. A single positional argument. This is a default behavior that can be enabled explicitly by specifying the argument passing type as `ArgumentPassing.AS_SINGLE_ARG`. In this case, each input element is directly passage to the worker function (as **the only argument**).
2. Multiple positional arguments. In this case, we assume that the input iterable contains lists or tuples of equal lengths. These tuples/arrays are interprted as positional arguments.
3. Named arguments (KWARGS). In this case, the input iterable should provide dictioinaries where keys correspond to the worker function names.

## Implementation of the stateful workers

Stateful workers are implemented as class objects, which implement a ``__call__`` function. Worker 
arguments are passed to the class ``__init__`` function. For simplicity, we support passing only KWARG-style arguments,
i.e., these arguments should be provided as dictionaries. A shared argument is passed to all workers during 
the initialization. If you specify a worker-specific list of arguments the number of elements must be equal
to the number of workers.

## Exception handling

All exceptions generated by the worker are caught. There are three options to handle these further:
1. ExceptionBehavior.DEFERRED : just save the exception object and return it to the main process.
2. ExceptionBehavior.IMMEDIATE : raise the exception in the main process without waiting for other tasks to finish.
3. ExceptionBehavior.IMMEDIATE_WAIT_OTHERS : wait for currently working tasks and raise the exception once they are finished

