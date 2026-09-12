"""How the task scales with PROCESSES and with the number of partitions.

    python daniele/bench_scaling.py ~/mapd-data/silver/papers

This is the biggest campaign and the one that answers the two questions the course
requires: how the time depends on the number of partitions and on the number of
processing units.

WHAT IT MEASURES: a GRID - every number of workers crossed with every number of
partitions, always with ONE thread per worker. One grid, three figures:

    time vs k, one curve per worker count      the "more processes, the curve drops and
                                               its left branch gets shorter" picture
    time vs workers, at the reference k        the speedup curve, with its efficiency
    peak RAM vs k                              where the memory wall is, and why the low
                                               values of k die

Everything here runs at ONE thread per worker on purpose: this campaign is about
processes. What happens when tasks share a process is `bench_threads.py`.

WHY A GRID AND NOT THREE SEPARATE SWEEPS: the three figures above are three readings of
the same table. Measuring them separately would mean paying for the same points two or
three times, and a night of cluster is not something we have to waste.

    python daniele/bench_scaling.py ~/mapd-data/silver/papers --worker 3 --k 64 --repetitions 3
        the calibration: three measures of one point, to get the spread and the budget

    python daniele/bench_scaling.py ~/mapd-data/silver/papers --worker 1 2 3 6
        the whole grid (this is the default)
"""

import argparse

import bench_common as bc

# How many worker PROCESSES to try. With 3 machines of 2 cores each:
#   1, 2, 3  -> one process per machine, the classic speedup curve
#   6        -> two per machine, i.e. one process per core: on the word count campaign
#               this was the configuration that won by far (16 processes gave 11.6x,
#               the same 16 cores as 4x4 threads gave 2.4x)
# Multiples of the number of machines come first because they are the balanced ones: 4 or
# 5 processes on 3 machines means somebody carries two and the others one, and Dask hands
# out work assuming the workers are equivalent.
WORKERS = (3, 2, 1, 6)

# The partitionings to sweep, from the middle outwards. The low values are the ones that
# die - the peak of one task goes as 1/k - so they sit at the end: if the night stops
# early, what is left in hand is the minimum of the curve with the branches next to it,
# which is the part that answers the question.
K_VALUES = (64, 128, 32, 256, 16, 8)


def campaign(workers, k_values):
    """The list of points: every worker count crossed with every partitioning."""
    points = []
    for w in workers:                      # outer loop: one cluster size at a time
        for k in k_values:                 # inner loop: sweep the partitions on it
            points.append(bc.base_point(
                "scaling",                 # campaign name, ends up in the CSV
                f"w{w}_k{k}",              # label of this point, unique within the campaign
                worker=w,
                thread=1,                  # ALWAYS one: this campaign is about processes
                k=k))
    return points


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    bc.add_common_arguments(parser)
    parser.add_argument("--worker", type=int, nargs="+", default=list(WORKERS),
                        help="how many worker processes to try")
    parser.add_argument("--k", type=int, nargs="+", default=list(K_VALUES),
                        help="the partitionings of the titles to sweep")
    return parser.parse_args()


def main():
    args = bc.prepare(parse_args())
    # Read the machines ONCE, before any point overrides CORD19_HOSTS.
    scheduler, machines, origin = bc.available_machines()

    if machines:
        # Asking for more processes than cores is a way to measure nothing useful.
        most = max(args.worker)
        print(f"machines  : {len(machines)} -> {most} processes means "
              f"up to {-(-most // len(machines))} per machine "
              f"({origin})")

    points = bc.repeat_passes(campaign(args.worker, args.k), args.repetitions)
    bc.run_campaign(points, args, scheduler, machines)


if __name__ == "__main__":
    main()
