"""The knobs that belong to THIS task, and to no other.

    python daniele/bench_knobs.py ~/mapd-data/silver/papers

`bench_scaling.py` and `bench_threads.py` ask questions any distributed job would ask -
more processes, more threads, more partitions. This one asks the three questions that only
the title-embedding job can ask, and each of them is a decision somebody had to take while
writing the code.

    "blocksize"   in what blocks the 4.5 GB model file is read.
                  It is the SECOND width of the graph: the titles are cut by `k`, the
                  model is cut by this. A block is what one task reads, parses and
                  filters, so it sets both how many tasks exist and how much memory each
                  one needs. Too big and a task chokes, too small and the scheduler pays
                  a fixed cost per task for nothing.

    "split_out"   in how many parts the final Reduce comes out.
                  With 1 a single task holds every embedding at once: a serial tail, and
                  a memory ceiling on top of it.

    "broadcast"   how the vectors are attached to the words.
                  True  = the filtered model (~0.11 GB) is handed to every worker, the
                          join happens locally and nothing is shuffled.
                  False = a real distributed join, which shuffles BOTH sides by `word`.
                  After the join every row carries 300 float32, so this is not a detail:
                  it decides whether several GB cross the network.

All three run at the reference cluster shape, because here we are changing the WORK, not
who executes it.
"""

import argparse

import bench_common as bc

# Model block sizes to try, around the 64MB reference.
BLOCK_SIZES = ("64MB", "32MB", "128MB", "16MB", "256MB")

# Output partitions of the Reduce. 1 is the interesting extreme: one task holding
# everything. It goes last because it is the one that may die.
SPLIT_OUT = (8, 16, 4, 1)

# How many processes these campaigns run on: the whole cluster, one process per machine.
FIXED_WORKERS = 3
FIXED_THREADS = 1


def campaign(block_sizes, split_out, workers, threads, skip_broadcast=False):
    """The three groups of points, each one knob turned from the reference."""
    points = []

    # 1. How the model file is cut.
    for b in block_sizes:
        points.append(bc.base_point(
            "blocksize", b,
            worker=workers, thread=threads, blocksize=b))

    # 2. How wide the tail of the Reduce is.
    for s in split_out:
        points.append(bc.base_point(
            "split_out", str(s),
            worker=workers, thread=threads, split_out=s))

    # 3. Broadcast against a real shuffling join. One point: the reference already is the
    #    broadcast version, so only the alternative needs measuring.
    if not skip_broadcast:
        points.append(bc.base_point(
            "broadcast", "shuffle",
            worker=workers, thread=threads, broadcast=False))

    return points


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    bc.add_common_arguments(parser)
    parser.add_argument("--blocksize", nargs="+", default=list(BLOCK_SIZES),
                        help="model block sizes to try")
    parser.add_argument("--split-out", type=int, nargs="+", default=list(SPLIT_OUT),
                        help="output partitions of the Reduce to try")
    parser.add_argument("--worker", type=int, default=FIXED_WORKERS,
                        help="how many processes to run these on")
    parser.add_argument("--thread", type=int, default=FIXED_THREADS,
                        help="threads per worker")
    parser.add_argument("--group", choices=["blocksize", "split_out", "broadcast"],
                        default=None, help="run only one of the three groups")
    return parser.parse_args()


def main():
    args = bc.prepare(parse_args())
    scheduler, machines, _ = bc.available_machines()

    points = campaign(args.blocksize, args.split_out, args.worker, args.thread)
    if args.group:
        points = [p for p in points if p["campaign"] == args.group]
        if not points:
            raise SystemExit(f"No points in group '{args.group}'")

    bc.run_campaign(bc.repeat_passes(points, args.repetitions), args, scheduler, machines)


if __name__ == "__main__":
    main()
