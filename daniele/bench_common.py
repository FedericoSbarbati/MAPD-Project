"""Shared machinery for the 2.3.3 benchmark campaigns. Not meant to be run directly.

Three campaign scripts import from here:

    bench_scaling.py   how time scales with PROCESSES and with the number of partitions
    bench_threads.py   what happens when tasks share a process (and its GIL, and its RAM)
    bench_knobs.py     the knobs specific to this task: blocksize, split_out, broadcast

They differ only in WHICH points they measure. Everything else - starting a cluster,
timing one configuration, reading the memory peak, appending the row - is written here
once instead of three times.

THE RULE THAT HOLDS EVERYTHING TOGETHER: one row of the CSV = one measure = a brand new
cluster. It costs about a minute per point, and in exchange every measure starts from
freshly born workers. That is not fussiness: a worker that has already chewed through
millions of strings keeps RSS it is no longer using (the allocator does not give it back),
so reusing one would add wear to whatever we are measuring.
"""

import csv
import os
import sys
import time
from collections import Counter
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(REPO / "daniele"))

from distributed import performance_report, wait  # noqa: E402

import title_embeddings as te  # noqa: E402
from cluster import get_client, read_hosts  # noqa: E402

# The file that must reach every machine before anything is computed: see `measure`.
CODE = REPO / "daniele" / "title_embeddings.py"

# Defaults shared by the three campaigns.
DEFAULT_INPUT = "~/mapd-data/silver/papers"
DEFAULT_OUT = "~/mapd-out/bench-embeddings"

# The reference configuration: every campaign starts from this and changes ONE thing.
K_REFERENCE = te.PARTITIONS                 # 64 - see the note in title_embeddings.py
BLOCK_REFERENCE = te.BLOCKSIZE              # "64MB"
SPLIT_OUT_REFERENCE = te.SPLIT_OUT          # 8

# Total share of a machine's RAM handed to the workers living on it. 0.85 leaves ~15% to
# the operating system. When two workers share a machine each gets half of this, which is
# the whole reason this number is written here and not hard-coded inside cluster.py.
MEMORY_SHARE = 0.85

# Seconds between closing one cluster and opening the next: port 8786 needs a moment to
# come back free, and the nannies need one to actually die.
PAUSE_BETWEEN_CLUSTERS = 10

COLUMNS = ["campaign", "point", "repetition",
           "worker", "thread", "slot", "per_machine",
           "k", "blocksize", "split_out", "broadcast",
           "seconds", "peak_gb", "mean_peak_gb",
           "partitions", "vocabulary", "real_workers", "real_threads", "error"]


# ----------------------------------------------------------------------------------
# Where the processes go
# ----------------------------------------------------------------------------------


def available_machines(repo_root=REPO):
    """The machines we may use. -> (scheduler ip, [worker machine ips], where it came from)

    Reads cluster.txt exactly like the task does, then REMOVES DUPLICATES from the worker
    list: cluster.txt may already repeat an address, but here we want the list of
    physical machines. How many processes to put on each one is a decision the campaign
    makes point by point.

    Must be called ONCE, at startup, before any campaign sets CORD19_HOSTS - otherwise it
    would read back its own choice instead of the file.
    """
    hosts, origin = read_hosts(str(repo_root))
    if not hosts:                                  # no cluster.txt: LocalCluster fallback
        return None, [], origin
    addresses = [h.strip() for h in hosts.split(",") if h.strip()]
    scheduler = addresses[0]                       # the first host is the scheduler only
    machines = []
    for ip in addresses[1:]:                       # keep the order, drop the repetitions
        if ip not in machines:
            machines.append(ip)
    return scheduler, machines, origin


def assign_processes(n_workers, machines):
    """Which machine each worker process goes to. -> list of ips, one per process

    Round robin: process i goes to machine i % len(machines). With 3 machines and 6
    processes that is two per machine, evenly. With 4 processes it is 2+1+1, which is
    UNEVEN - Dask hands out work assuming the workers are equivalent, so an uneven point
    bends the curve for a reason that has nothing to do with the algorithm. The campaigns
    therefore prefer multiples of the number of machines, and `per_machine` ends up in
    the CSV so that an uneven point can be recognised later.
    """
    return [machines[i % len(machines)] for i in range(n_workers)]


def configure_topology(n_workers, scheduler, machines):
    """Tell `cluster.py` exactly which processes to start, and how much RAM each may use.

    Returns the number of processes on the busiest machine (1 when there is no cluster).

    We do NOT rewrite cluster.txt: `read_hosts` looks at the environment variable FIRST,
    so setting it here overrides the file for this measure only. Nothing on disk changes,
    and a crashed campaign cannot leave a corrupted cluster.txt behind.
    """
    if not machines:                       # no cluster.txt -> LocalCluster, nothing to do
        return 1

    placement = assign_processes(n_workers, machines)
    # The host list get_client will read: the scheduler first, then one entry per process.
    # An address repeated twice means two worker PROCESSES on that machine.
    os.environ["CORD19_HOSTS"] = ",".join([scheduler] + placement)

    # Memory is per PROCESS, but the machine is shared: with two workers on one machine,
    # each asking for 85% of its RAM, they would together ask for 170% and both get
    # killed. So the share is divided by how many live on the busiest machine.
    per_machine = max(Counter(placement).values())
    os.environ["CORD19_WORKER_MEMORY_FRACTION"] = str(round(MEMORY_SHARE / per_machine, 3))
    return per_machine


# ----------------------------------------------------------------------------------
# Measuring one point
# ----------------------------------------------------------------------------------


def memory_peak(client):
    """The highest RSS each worker reached during this measure. -> {peak_gb, mean_peak_gb}

    `ru_maxrss` is the historical maximum of the process. Since every measure starts a NEW
    cluster, that historical maximum IS this measure's peak: no sampling and no watchdog
    are needed.

    `peak_gb` is the busiest worker, and it is the number that matters: it is what the
    nanny compares against the limit when it decides whether to kill somebody.
    """
    def max_rss():
        import resource
        import sys as _sys

        # The unit of ru_maxrss changes with the system: bytes on macOS, kilobytes on
        # Linux. The conversion is done HERE, on the worker, because the cluster is Linux
        # and the laptop launching this might not be.
        peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        return peak / 1e9 if _sys.platform == "darwin" else peak / 1e6

    peaks = list(client.run(max_rss).values())
    if not peaks:
        return {}
    return {"peak_gb": round(max(peaks), 2),
            "mean_peak_gb": round(sum(peaks) / len(peaks), 2)}


def cluster_state(client):
    """How many workers and threads are REALLY there now, not how many were asked for.

    `client.nthreads()` and NOT `client.scheduler_info()["workers"]`: the second one
    UNDER-COUNTS when several workers share a machine, which is exactly our case when we
    pack two processes per machine, and it never corrects itself. It has already reported
    5 workers for campaigns that really had 8 and 16.
    """
    threads = client.nthreads()
    return {"real_workers": len(threads), "real_threads": sum(threads.values())}


def measure(point, args, scheduler, machines):
    """Start a cluster, time ONE configuration, shut it down. -> the CSV row.

    A single try/except covers both a cluster that fails to start and a computation that
    dies: from the outside they are the same thing, a point that is not there. A failure
    is written down like any other row, because on the partition curve "it did not
    complete" IS the result: it marks where the memory wall is.

    The stopwatch covers BOTH phases of the task, vocabulary included: the pipeline is not
    fully lazy, and timing only the second half would mean timing something we do not
    deliver. The timeout covers the write, which is the long phase.
    """
    client = cluster = None
    row = dict(point, seconds=None, error="", vocabulary=None, partitions=None)
    name = f"{point['campaign']}_{point['point']}_{point['repetition']}"
    report = args.out / f"report_{name}.html"
    destination = args.out / "embeddings"

    try:
        # 1. Decide where the processes go and how much RAM each may use.
        row["per_machine"] = configure_topology(point["worker"], scheduler, machines)

        # 2. Start the cluster. When CORD19_HOSTS is set it already lists exactly the
        #    processes we want, so n_workers must stay None; without a cluster.txt we are
        #    on a LocalCluster and the count has to be passed explicitly.
        client, cluster = get_client(repo_root=REPO,
                                     n_workers=None if machines else point["worker"],
                                     n_threads=point["thread"])
        row.update(cluster_state(client))           # the configuration actually obtained

        # 3. Send the code. The data is replicated on every machine, the code is not: it
        #    only exists on the one we launch from. But in the graph we ship, functions
        #    travel BY NAME, so scheduler and workers must be able to import this module.
        client.upload_file(str(CODE))
        # 4. Create the output directory ON EVERY MACHINE: `to_parquet` creates it here on
        #    the client, but the workers are the ones writing, each on its own disk.
        client.run(os.makedirs, str(destination), exist_ok=True)

        with performance_report(filename=str(report), mode="inline"):
            started = time.perf_counter()
            # 5. Build the graph for exactly this configuration.
            embeddings, vocabulary, partitions = te.build(
                args.input, args.model,
                partitions=point["k"], blocksize=point["blocksize"],
                split_out=point["split_out"], broadcast=point["broadcast"],
                client=client)
            # 6. Compute it, with a ceiling: a configuration that hangs must not eat the
            #    hours that are left. No need to cancel anything - the cluster dies next.
            write_task = embeddings.to_parquet(destination, write_index=False,
                                               compression="zstd", overwrite=True,
                                               compute=False)
            futures = client.compute(write_task)
            wait(futures, timeout=args.timeout)
            futures.result()               # re-raises here if the computation died
            row["seconds"] = round(time.perf_counter() - started, 1)

        row["vocabulary"] = vocabulary
        row["partitions"] = partitions             # the REAL ones, not the ones asked for
        row.update(cluster_state(client))          # if a worker died, it shows up here
        row.update(memory_peak(client))
    except Exception as error:
        row["error"] = f"{type(error).__name__}: {error}"[:200]
    finally:
        if client is not None:
            client.close()
        if cluster is not None:
            cluster.close()
        time.sleep(PAUSE_BETWEEN_CLUSTERS)
    return row


def write_row(path, row):
    """Append one measure to the CSV. Immediately, not at the end of the campaign.

    A campaign runs for hours. Writing as we go means that if it dies at 3am, everything
    measured until then is already on disk.
    """
    is_new = not path.exists()
    with open(path, "a", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=COLUMNS, extrasaction="ignore", restval="")
        if is_new:
            writer.writeheader()
        writer.writerow(row)


# ----------------------------------------------------------------------------------
# Running a campaign
# ----------------------------------------------------------------------------------


def base_point(campaign, label, **changed):
    """One point of a campaign: the reference configuration with something changed.

    Every campaign is written as "the reference, with ONE knob turned". Changing one thing
    at a time is what makes a difference in the result attributable to that thing, and it
    is why each campaign file is a plain list of points rather than an algorithm.
    """
    base = {"campaign": campaign, "point": label,
            "worker": None, "thread": None,            # filled in by the campaign
            "k": K_REFERENCE, "blocksize": BLOCK_REFERENCE,
            "split_out": SPLIT_OUT_REFERENCE, "broadcast": True,
            "per_machine": 1}
    base.update(changed)
    # slot = how many tasks can run at the same time on the whole cluster. It is what
    # makes "3 workers x 2 threads" and "6 workers x 1 thread" comparable.
    base["slot"] = (base["worker"] or 1) * (base["thread"] or 1)
    return base


def run_campaign(points, args, scheduler, machines):
    """Measure every point, in order, writing as it goes. -> nothing, the CSV is the result."""
    destination = args.out / "measures.csv"
    print(f"input     : {args.input}")
    print(f"model     : {args.model}")
    print(f"csv       : {destination}")
    print(f"machines  : {machines if machines else '(LocalCluster: no cluster.txt)'}")
    print(f"measures  : {len(points)}   ceiling: {args.timeout}s each\n")

    for i, point in enumerate(points, 1):
        print(f"[{i}/{len(points)}] {point['campaign']}: {point['point']} "
              f"(worker={point['worker']} thread={point['thread']} k={point['k']}, "
              f"rep {point['repetition']})", flush=True)
        row = measure(point, args, scheduler, machines)
        write_row(destination, row)
        outcome = row["error"] or (f"{row['seconds']} s, peak {row.get('peak_gb', '?')} GB "
                                   f"({row.get('real_workers')}w x {row.get('real_threads', 0)}t)")
        print(f"          -> {outcome}\n", flush=True)

    print("written:", destination)


def add_common_arguments(parser):
    """The options every campaign shares. Each script then adds its own knob."""
    parser.add_argument("input", nargs="?", default=DEFAULT_INPUT,
                        help=f"silver/papers directory (default {DEFAULT_INPUT})")
    parser.add_argument("--model", default=te.DEFAULT_MODEL,
                        help="fastText .vec file, same path on every worker")
    parser.add_argument("--out", default=DEFAULT_OUT,
                        help=f"where to write CSV and reports (default {DEFAULT_OUT})")
    parser.add_argument("--timeout", type=int, default=3600,
                        help="ceiling in seconds for ONE measure (default 3600)")
    parser.add_argument("--repetitions", type=int, default=1,
                        help="how many WHOLE passes of the campaign (default 1)")
    return parser


def prepare(args):
    """Resolve the paths and check them before an hour of cluster time is spent."""
    args.input = Path(args.input).expanduser()
    if not args.input.is_absolute():
        args.input = REPO / args.input
    args.model = Path(args.model).expanduser()
    args.out = Path(args.out).expanduser()
    if not args.out.is_absolute():
        args.out = REPO / args.out
    args.out.mkdir(parents=True, exist_ok=True)

    if not args.input.exists():
        raise SystemExit(f"Input not found: {args.input}")
    if not args.model.exists():
        raise SystemExit(
            f"Model not found: {args.model}\n"
            "It must exist AT THE SAME PATH ON EVERY WORKER: there is no shared file\n"
            "system any more, each machine reads it from its own disk."
        )
    return args


def repeat_passes(points, repetitions):
    """Turn a list of points into whole passes.

    THE REPETITIONS ARE WHOLE PASSES, not consecutive measures of the same point. It costs
    the same and says more: hours pass between two repetitions of one point, so the spread
    we measure includes how the machine varies during the day, not just how two runs glued
    together differ. And if the night is interrupted, what is left in hand is a COMPLETE
    campaign rather than half a curve measured three times.
    """
    return [dict(p, repetition=r) for r in range(repetitions) for p in points]
