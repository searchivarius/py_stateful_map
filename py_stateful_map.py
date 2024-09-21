import multiprocessing as mp
import inspect

from typing import Dict, List, Type, NamedTuple
from copy import copy

from version import __version__


class ArgumentPassing(NamedTuple):
    """
        A slightly modified version of pqdm constants
        https://github.com/niedakh/pqdm/blob/master//pqdm/constants.py
    """
    AS_SINGLE_ARG = 'single_arg'
    AS_ARGS = 'args'
    AS_KWARGS = 'kwargs'


class ExceptionBehavior(NamedTuple):
    IMMEDIATE = 'immediate'
    IMMEDIATE_WAIT_OTHERS = 'immediate_wait_others'
    DEFERRED = 'deferred'


class WorkerResultWrapper:
    def __init__(self, obj_id, ret_val, exception_obj):
        self.obj_id = obj_id
        self.ret_val = ret_val
        self.exception_obj = exception_obj


class StateFullWorkerWrapper:
    def __init__(self, worker_class_or_func: Type, arg_dict: Dict):
        if inspect.isclass(worker_class_or_func):
            self.worker = worker_class_or_func(**arg_dict)
        elif inspect.isfunction(worker_class_or_func):
            if arg_dict:
                raise Exception('The worker is a stateless function, but you passed it initialization parameters ' +
                                '(only possible with a class-based worker)!')
            self.worker = worker_class_or_func
        else:
            raise Exception(f'Wrong worker type: {type(worker_class_or_func)}')

    def __call__(self, in_queue, out_queue, argument_passing: ArgumentPassing):
        while True:
            packed_arg = in_queue.get()
            if packed_arg is None:
                break

            obj_id, worker_arg = packed_arg
            exception_obj = None
            ret_val = None
            try:
                if argument_passing == ArgumentPassing.AS_KWARGS:
                    ret_val = self.worker(**worker_arg)
                elif argument_passing == ArgumentPassing.AS_ARGS:
                    ret_val = self.worker(*worker_arg)
                else:
                    ret_val = self.worker(worker_arg)

            except Exception as e:
                exception_obj = e

            out_queue.put(WorkerResultWrapper(obj_id=obj_id,
                                              ret_val=ret_val,
                                              exception_obj=exception_obj))


class StateFullWorkerPoolResultGenerator:
    def __init__(self, parent_obj, input_iterable, is_unordered):
        self.parent_obj = parent_obj
        self.input_iter = iter(input_iterable)
        self.is_unordered = is_unordered

        # If the length is None, then TQDM will not know the total length and will not display the progress bar:
        # See __len__ function https://github.com/tqdm/tqdm/blob/master/tqdm/std.py
        if hasattr(input_iterable, '__len__'):
            self._length = len(input_iterable)  # Store the length of the iterable
        else:
            self._length = None
        self._iterator = self._generator()  # Create the generator

    def __len__(self):
        return self._length

    def __iter__(self):
        return self

    def __next__(self):
        return next(self._iterator)

    def _generator(self):
        submitted_qty = 0
        received_qty = 0

        assert type(self.parent_obj.chunk_size) == int
        if self.is_unordered:
            assert type(self.parent_obj.chunk_prefill_ratio) == int and self.parent_obj.chunk_prefill_ratio >= 1
            curr_batch_size = self.parent_obj.chunk_size * self.parent_obj.chunk_prefill_ratio
        else:
            curr_batch_size = self.parent_obj.chunk_size

        finished_input = False

        while not finished_input or received_qty < submitted_qty:
            try:
                for k in range(curr_batch_size):
                    self.parent_obj.in_queue.put((submitted_qty, next(self.input_iter)))
                    assert self._length is None or submitted_qty < self._length
                    submitted_qty += 1
            except StopIteration:
                finished_input = True

            curr_batch_size = self.parent_obj.chunk_size
            left_qty = submitted_qty - received_qty

            output_arr = []
            for k in range(min(self.parent_obj.chunk_size, left_qty)):
                result: WorkerResultWrapper = self.parent_obj.out_queue.get()
                if result.exception_obj is not None and \
                    self.parent_obj.exception_behavior in [ExceptionBehavior.IMMEDIATE,
                                                           ExceptionBehavior.IMMEDIATE_WAIT_OTHERS]:
                    self.parent_obj.send_term_signal()
                    # Wait to finish before raising an exception
                    if self.parent_obj.exception_behavior == ExceptionBehavior.IMMEDIATE_WAIT_OTHERS:
                        self.parent_obj.join_workers()
                    raise result.exception_obj
                if self.is_unordered:
                    yield result
                else:
                    output_arr.append(result)

                assert received_qty < submitted_qty
                assert self._length is None or received_qty < self._length
                received_qty += 1

            output_arr.sort(key=lambda result_obj: result_obj.obj_id)
            for result in output_arr:
                yield result

        self.parent_obj.send_term_signal()
        self.parent_obj.join_workers()


class StateFullWorkerPool:
    def __enter__(self):
        return self

    def map(self, input_iterable, is_unordered):
        return StateFullWorkerPoolResultGenerator(parent_obj=self,
                                                  input_iterable=input_iterable,
                                                  is_unordered=is_unordered)

    def __init__(self, num_workers: int, worker_class_or_func: Type,
                 shared_kwarg_dict: Dict = None,
                 worker_kwarg_dict_arr: List[Dict] = None,
                 chunk_size: int = 100, chunk_prefill_ratio: int = 2,
                 use_threads: bool = False,
                 argument_passing: ArgumentPassing = ArgumentPassing.AS_SINGLE_ARG,
                 exception_behavior: ExceptionBehavior = ExceptionBehavior.IMMEDIATE,
                 join_timeout: float = None):
        self.num_workers = max(int(num_workers), 1)
        self.chunk_prefill_ratio = max(int(chunk_prefill_ratio), 1)
        self.chunk_size = int(chunk_size)
        self.exception_behavior = exception_behavior
        self.argument_passing = argument_passing

        self.in_queue = mp.Queue()
        self.out_queue = mp.Queue()

        self.use_threads = use_threads

        if self.use_threads:
            import threading
            process_class = threading.Thread
            daemon = None
        else:
            process_class = mp.Process
            daemon = True

        self.join_timeout = join_timeout

        self.workers = []

        # Start worker processes
        if worker_kwarg_dict_arr is not None:
            assert len(worker_kwarg_dict_arr) == self.num_workers
        for proc_id in range(self.num_workers):
            if shared_kwarg_dict is None:
                shared_kwarg_dict = {}
            all_arg_dict = copy(shared_kwarg_dict)
            if worker_kwarg_dict_arr is not None:
                for arg_key, arg_val in worker_kwarg_dict_arr[proc_id].items():
                    all_arg_dict[arg_key] = arg_val

            one_proc = process_class(target=StateFullWorkerWrapper(worker_class_or_func, all_arg_dict),
                                     args=(self.in_queue, self.out_queue, self.argument_passing),
                                     daemon=daemon)
            self.workers.append(one_proc)
            one_proc.start()

    def __exit__(self, type, value, tb):
        # If a process "refuses" to stop it can be terminated.
        # Unfortunately, threads cannot be stopped / terminated in Python,
        # but they will die when the main process terminates.
        if not self.use_threads:
            for p in self.workers:
                p.terminate()

    def join_workers(self):
        for p in self.workers:
            p.join(self.join_timeout)

    def send_term_signal(self):
        for _ in range(self.num_workers):
            self.in_queue.put(None)  # end-of-work signal: one per worker

            self.term_signal_sent = True


