#!/usr/bin/env python
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
