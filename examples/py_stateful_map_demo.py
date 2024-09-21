#!/usr/bin/env python
import argparse
import multiprocessing as mp

from tqdm import tqdm

from py_stateful_map import StateFullWorkerPool, WorkerResultWrapper, ExceptionBehavior, ArgumentPassing


def simple_arithmetic_func_worker(input_arg):
    ret_val = input_arg
    for t in range(100_000):
        ret_val = (ret_val * ret_val) % 337
    return ret_val


class SimpleArithmeticClassWorker:
    def __init__(self, proc_id, fire_exception_proc_id=None):
        print(f'Init. process {proc_id} with shared arg: {fire_exception_proc_id} ')
        self.proc_id = proc_id
        self.fire_exception_proc_id = fire_exception_proc_id

    def __call__(self, input_arg):
        if self.proc_id == self.fire_exception_proc_id:
            raise Exception("Rogue exception!")
        return simple_arithmetic_func_worker(input_arg)


def main(args):
    if args.iterable_arg_passing == ArgumentPassing.AS_SINGLE_ARG:
        input_arr = [k * 10 for k in range(args.num_lines)]
    elif args.iterable_arg_passing == ArgumentPassing.AS_ARGS:
        input_arr = [[k * 10] for k in range(args.num_lines)]
    else:
        assert args.iterable_arg_passing == ArgumentPassing.AS_KWARGS
        input_arr = [dict(input_arg=k * 10) for k in range(args.num_lines)]

    def input_arr_generator():
        for e in input_arr:
            yield e

    if args.use_unsized_iterable:
        iterable = input_arr_generator()
    else:
        iterable = input_arr

    tot_res = 0

    n_workers = args.n_workers
    print('Number of workers:', n_workers)
    print('Use stateless (function) worker?:', args.use_stateless_function_worker)
    print('Use threads?:', args.use_threads)
    print('Number of input items:', len(input_arr))
    print('Input iterable without __len__?', args.use_unsized_iterable)
    print('UN-ordered?:', args.is_unordered)

    if args.use_stateless_function_worker:
        worker_class_or_func = simple_arithmetic_func_worker
        shared_kwarg_dict = None
        worker_kwarg_dict_arr = None
    else:
        worker_class_or_func = SimpleArithmeticClassWorker
        shared_kwarg_dict = dict(fire_exception_proc_id=args.fire_exception_proc_id)
        worker_kwarg_dict_arr = []

        for pid in range(n_workers):
            worker_kwarg_dict_arr.append(dict(proc_id=pid))

    with StateFullWorkerPool(num_workers=n_workers,
                             worker_class_or_func=worker_class_or_func,
                             shared_kwarg_dict=shared_kwarg_dict, worker_kwarg_dict_arr=worker_kwarg_dict_arr,
                             chunk_size=args.chunk_size, chunk_prefill_ratio=args.chunk_prefill_ratio,
                             use_threads=args.use_threads,
                             argument_passing=args.iterable_arg_passing,
                             exception_behavior=args.exception_behavior,
                             join_timeout=1) as proc_pool:
        prev_obj_id = None

        # just marking the type
        result: WorkerResultWrapper
        for result in tqdm(proc_pool.map(iterable, args.is_unordered)):
            if not args.is_unordered:
                assert prev_obj_id is None or prev_obj_id + 1 == result.obj_id
                prev_obj_id = result.obj_id

            if result.exception_obj is not None:
                print('Error:', result.exception_obj)
            else:
                tot_res += result.ret_val
                assert result.exception_obj is None

    print('Total:', tot_res)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument('--num_lines', type=int, default=10000)
    parser.add_argument('--chunk_size', type=int, default=1000)
    parser.add_argument('--chunk_prefill_ratio', type=int, default=2)
    parser.add_argument('--n_workers', type=int, default=mp.cpu_count())
    parser.add_argument('--is_unordered', action='store_true')
    parser.add_argument('--use_threads',  action='store_true')

    parser.add_argument('--fire_exception_proc_id', type=int, default=None,
                        help='Fire a "rogue" exception from the process with this ID')
    parser.add_argument('--use_stateless_function_worker',
                        action='store_true', help='Use a regular (stateless) function-based worker instead of a class')
    parser.add_argument('--use_unsized_iterable', action='store_true')

    parser.add_argument('--iterable_arg_passing', required=True,
                        choices=[ArgumentPassing.AS_SINGLE_ARG, ArgumentPassing.AS_ARGS, ArgumentPassing.AS_KWARGS])
    parser.add_argument('--exception_behavior', default=ExceptionBehavior.DEFERRED,
                        choices=[ExceptionBehavior.DEFERRED, ExceptionBehavior.IMMEDIATE_WAIT_OTHERS, ExceptionBehavior.IMMEDIATE])

    args = parser.parse_args()
    main(args)
