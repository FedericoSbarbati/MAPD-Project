"""What happens when tasks share a process: its GIL, and its memory.

    python daniele/bench_threads.py ~/mapd-data/silver/papers

A worker is a PROCESS. Giving it more threads means running more tasks inside that one
process, and those tasks share two things: one GIL and one memory budget. This campaign
measures both effects, and it measures them separately from the number of processes,
which is `bench_scaling.py`.

THREE GROUPS OF POINTS, three questions:

    "thread"       3 workers, 1 / 2 / 4 threads each, at the reference k
                   -> does adding threads make it faster? (on the word count it did NOT:
                      +34% instead of the -40% one would expect, because that Map was
                      pure Python and held the GIL. Here the heavy phase is different -
                      parsing 600 M floats, which happens inside pandas in C and RELEASES
                      the GIL - so the answer is genuinely open.)

                   The same points read as memory: if the peaks of the N tasks in a
                   worker happened at the same moment, the worker's peak would be N times
                   the single-task peak. They do not: on the word count 4 threads cost
                   1.57x, not 4x. The hypothetical "if they summed" line is drawn by the
                   notebook from the 1-thread measure, so there is nothing extra to
                   measure here.

    "slot"         the same number of concurrent tasks, arranged as processes or as
                   threads: 6 = 6x1 / 3x2 / 1x6, and 2 = 2x1 / 1x2
                   -> this is the one that separates the two explanations. Same number of
                      slots, same k, same corpus: if the process version wins, the cost is
                      the GIL; if they tie, the cost was memory pressure.

    "k_thread4"    the partition sweep AT 4 THREADS
                   -> the second curve of the partitions figure. Threads multiply the
                      memory demand of a worker, so the wall moves: this says by how much.
"""

import argparse

import bench_common as bc

# Threads per worker to try, at a fixed number of processes.
THREADS = (1, 2, 4)

# How many processes the "thread" and "k_thread4" groups run on: all the machines, one each.
FIXED_WORKERS = 3

# The equivalence points: (workers, threads) pairs whose PRODUCT is the same, i.e. the
# same number of tasks running at once, arranged differently. 6 slots is the whole
# cluster (3 machines x 2 cores); 2 slots is one machine's worth.
EQUAL_SLOTS = ((6, 1), (3, 2), (1, 6),
               (2, 1), (1, 2))

# The partitionings for the 4-thread curve. Fewer values than bench_scaling: this is the
# second curve of a figure, not a grid, and each thread multiplies the memory demand, so
# the low values are expected to die.
K_VALUES_4T = (64, 128, 32, 256, 16)


def campaign(threads, fixed_workers, k_values):
    """The three groups of points, in the order they should be measured."""
    points = []

    # 1. The thread curve: same processes, same k, more threads inside each process.
    for t in threads:
        points.append(bc.base_point(
            "thread", f"w{fixed_workers}_t{t}",
            worker=fixed_workers, thread=t))

    # 2. Same slots, arranged differently. Skip the ones already measured above, so the
    #    same configuration never gets two names (it would split one point across two
    #    curves and make both of them wrong).
    already_done = {(fixed_workers, t) for t in threads}
    for w, t in EQUAL_SLOTS:
        if (w, t) in already_done:
            continue
        already_done.add((w, t))
        points.append(bc.base_point(
            "slot", f"w{w}_t{t}",          # label says the arrangement; `slot` says w*t
            worker=w, thread=t))

    # 3. The partition sweep at 4 threads: the second curve of the partitions figure.
    for k in k_values:
        points.append(bc.base_point(
            "k_thread4", f"k{k}_t4",
            worker=fixed_workers, thread=4, k=k))

    return points


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    bc.add_common_arguments(parser)
    parser.add_argument("--thread", type=int, nargs="+", default=list(THREADS),
                        help="threads per worker to try")
    parser.add_argument("--worker", type=int, default=FIXED_WORKERS,
                        help="how many processes the thread curve runs on")
    parser.add_argument("--k", type=int, nargs="+", default=list(K_VALUES_4T),
                        help="partitionings for the 4-thread curve")
    parser.add_argument("--group", choices=["thread", "slot", "k_thread4"], default=None,
                        help="run only one of the three groups")
    return parser.parse_args()


def main():
    args = bc.prepare(parse_args())
    scheduler, machines, _ = bc.available_machines()

    points = campaign(args.thread, args.worker, args.k)
    if args.group:                           # filter down to one group if asked
        points = [p for p in points if p["campaign"] == args.group]
        if not points:
            raise SystemExit(f"No points in group '{args.group}'")

    bc.run_campaign(bc.repeat_passes(points, args.repetitions), args, scheduler, machines)


if __name__ == "__main__":
    main()
