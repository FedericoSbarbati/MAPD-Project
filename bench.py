"""Shared cluster and measurement helpers for the four MAPD analysis tasks.

Cluster creation and benchmarking live in the same module on purpose: the glibc
settings that keep worker memory flat have to be applied to the *driver* config
BEFORE the cluster is created, because that is when the nannies spawn the worker
processes (docs/MEMORY_LEAK_REPORT.md, section 7.2). Splitting them into two modules
would make it easy to create a cluster that silently misses the configuration.

Usage:

    from bench import get_client
    client = get_client()          # LocalCluster, or the real cluster if configured
    ...
    client.close()
"""

import csv
import os
import time

# Where the cluster lives. Same convention as conversion_sanification.ipynb, so one
# cluster.txt serves every task and nothing is ever hard-coded.
#
#   DASK_SCHEDULER              a scheduler that is already running -> just connect
#   CORD19_HOSTS / cluster.txt  "ip_scheduler,ip_worker1,..." -> start an SSHCluster.
#                               The FIRST host is scheduler only; repeat it to make it
#                               a worker too.
#   neither                     -> LocalCluster, for development on the Mac
#
# cluster.txt is git-ignored: the Cloud Veneto VMs get new IPs at every session.
HOSTS_FILE = "cluster.txt"


def read_hosts(repo_root="."):
    """The cluster host list and where it came from: (hosts, source).

    Returns (None, list_of_places_searched) when nothing was found, so the caller can
    say WHERE it looked instead of silently running on one machine.
    """
    if os.environ.get("CORD19_HOSTS"):
        return os.environ["CORD19_HOSTS"], "$CORD19_HOSTS"

    searched = []
    for directory in (repo_root, os.getcwd(), os.path.expanduser("~")):
        path = os.path.abspath(os.path.join(directory, HOSTS_FILE))
        if path in searched:
            continue
        searched.append(path)
        if os.path.exists(path):
            for line in open(path):
                line = line.strip()
                if line and not line.startswith("#"):
                    return line, path
    return None, searched


def configure_memory():
    """Settings that keep worker RSS flat over a long run of text processing.

    MALLOC_TRIM_THRESHOLD_ and MALLOC_ARENA_MAX must reach the worker process at spawn
    time, before glibc initialises its allocator - passing them through
    `worker_options={"env": ...}` does not work, it arrives too late. `pre-spawn-environ`
    is the hook that runs early enough, and SSHCluster ships the driver's config to the
    nodes for us. See docs/MEMORY_LEAK_REPORT.md, act 3 and section 7.2.
    """
    import dask

    environ = dict(dask.config.get("distributed.nanny.pre-spawn-environ"))
    environ.update({"MALLOC_TRIM_THRESHOLD_": 0, "MALLOC_ARENA_MAX": 2})
    dask.config.set({"distributed.nanny.pre-spawn-environ": environ})


def worker_options():
    """Per-worker sizing, deliberately expressed in RELATIVE terms.

    The cluster is rebuilt from a snapshot at every session and the VM flavour changes:
    4 GB / 2 cores today, 8 GB tomorrow. So the defaults must adapt rather than name
    absolute numbers - a hard-coded "7GB" on a 3.8 GB VM is silently ignored by the nanny
    and the worker then believes it owns memory it does not have.

    - nthreads: unset by default, so each node uses its own core count.
    - memory_limit: a FRACTION of the node's RAM, leaving room for the OS and the NFS
      client. Dask accepts a float as "this share of system memory".
    """
    options = {"memory_limit": float(os.environ.get("CORD19_WORKER_MEMORY_FRACTION", "0.85"))}
    if os.environ.get("CORD19_WORKER_MEMORY_LIMIT"):
        options["memory_limit"] = os.environ["CORD19_WORKER_MEMORY_LIMIT"]
    if os.environ.get("CORD19_THREADS_PER_WORKER"):
        options["nthreads"] = int(os.environ["CORD19_THREADS_PER_WORKER"])
    return options


def describe(client, mode, detail):
    """Say out loud what we are actually running on.

    This exists because the silent fallback to a local cluster is expensive: a run that
    quietly uses one machine instead of four looks like a slow cluster, not like a
    misconfiguration, and you only find out after wasting a session.
    """
    workers = client.scheduler_info()["workers"].values()
    hosts = {w["host"] for w in workers}
    total_memory = sum(w.get("memory_limit", 0) for w in workers)
    print("=" * 70)
    print(f"cluster   : {mode}")
    print(f"            {detail}")
    print(f"workers   : {len(workers)} su {len(hosts)} host {sorted(hosts)}")
    print(f"threads   : {sum(w.get('nthreads', 0) for w in workers)}")
    print(f"memoria   : {total_memory / 1e9:.1f} GB totali "
          f"({total_memory / max(len(workers), 1) / 1e9:.1f} GB per worker)")
    print(f"dashboard : {client.dashboard_link}")
    if mode == "LocalCluster":
        print("            ATTENZIONE: tutto su questa sola macchina. Per usare il")
        print("            cluster serve cluster.txt nella root del repo.")
    print("=" * 70)


def get_client(repo_root="."):
    """Connect to whichever cluster this machine is configured for.

    Returns (client, cluster); `cluster` is None when we only attached to a scheduler
    somebody else started. Close both when done.
    """
    from dask.distributed import Client, LocalCluster

    configure_memory()  # must happen before any worker process is spawned

    scheduler = os.environ.get("DASK_SCHEDULER")
    hosts, source = read_hosts(repo_root)

    if scheduler:
        client = Client(scheduler)
        describe(client, "scheduler esistente", scheduler)
        return client, None

    if hosts:
        from dask.distributed import SSHCluster

        addresses = [h.strip() for h in hosts.split(",") if h.strip()]
        if len(addresses) < 2:
            raise SystemExit(
                f"{source} elenca un solo host ({addresses}). Il PRIMO host fa solo da\n"
                "scheduler: servono almeno due voci perche' esista un worker. Ripeti lo\n"
                "scheduler come secondo host se vuoi che lavori anche lui."
            )
        connect = {"known_hosts": None}  # the internal IPs get recycled between sessions
        if os.environ.get("CORD19_SSH_KEY"):
            connect["client_keys"] = [os.environ["CORD19_SSH_KEY"]]
        cluster = SSHCluster(
            addresses,
            connect_options=connect,
            worker_options=worker_options(),
            scheduler_options={"port": 8786, "dashboard_address": ":8787"},
            # None means "the same interpreter path as the driver". The workers are
            # snapshot clones of the scheduler, so ~/pyvenv sits at the same path.
            remote_python=os.environ.get("CORD19_REMOTE_PYTHON"),
        )
        client = Client(cluster)
        client.wait_for_workers(len(addresses) - 1, timeout="180s")
        describe(client, "SSHCluster", f"{source} -> scheduler {addresses[0]}, "
                                      f"worker {addresses[1:]}")
        return client, cluster

    # No host list: everything runs here. Size the local cluster from THIS machine,
    # not from a number that made sense on some other one.
    cores = os.cpu_count() or 2
    n_workers = int(os.environ.get("CORD19_WORKERS", max(1, min(4, cores // 2))))
    options = worker_options()
    cluster = LocalCluster(
        n_workers=n_workers,
        threads_per_worker=options.get("nthreads", max(1, cores // n_workers)),
        memory_limit=options["memory_limit"] / n_workers
        if isinstance(options["memory_limit"], float)
        else options["memory_limit"],
        processes=True,
    )
    client = Client(cluster)
    describe(client, "LocalCluster", f"nessun cluster.txt in: {', '.join(source)}")
    return client, cluster


def sweep(client):
    """Give back to the OS the memory the workers are holding but no longer using.

    Call it BETWEEN timed measurements, never inside one. On glibc it recovers the part
    of the heap the allocator is retaining after millions of small string allocations
    (54% of the growth, measured on the cluster) and it costs almost nothing.
    """

    def _sweep():
        import ctypes
        import ctypes.util
        import gc

        import pyarrow as pa

        gc.collect()
        pa.default_memory_pool().release_unused()
        try:
            ctypes.CDLL(ctypes.util.find_library("c")).malloc_trim(0)
        except Exception:
            pass  # not glibc (macOS): gc + Arrow release is all we can do

    client.run(_sweep)


def unmanaged_memory(client):
    """Bytes of worker RSS that Dask is not accounting for, per worker.

    The number to watch across repeated measurements: if it climbs linearly there is a
    churn hotspot in the task; if it wobbles around a plateau it is just working set.
    """
    info = client.scheduler_info()["workers"]
    return {
        address: worker["metrics"]["memory"] - worker["metrics"].get("managed_bytes", 0)
        for address, worker in info.items()
    }


# ----------------------------------------------------------------------------------
# Benchmarks. The course requires them: execution time against the number of dataset
# partitions AND against the number of workers. An analysis without them is graded as
# incomplete (InstructionsAndGuidelines, point 5).
# ----------------------------------------------------------------------------------


class WorkerTracker:
    """Counts how many workers were replaced between one measurement and the next.

    A worker the nanny kills and restarts comes back on a NEW port, so an address that was
    not there before means somebody died. Without this the campaign only learns that a
    configuration was slow, not that it was slow *because* it spent half the run
    recomputing what dead workers were holding - which is the whole story of `foldby` on
    small workers (README, "Dove finisce il Reduce").

    Caveat: a deliberate `scale()` down and back up would also count as new workers. The
    worker sweep only ever scales DOWN, so in this campaign the number is clean.
    """

    def __init__(self, client):
        self.seen = set(client.scheduler_info()["workers"])

    def restarted_since(self, client):
        current = set(client.scheduler_info()["workers"])
        fresh = current - self.seen
        self.seen |= current
        return len(fresh)


def dump_worker_logs(client, path):
    """Save every worker's log to a file, and count the two lines that matter.

    Run at the END of a block, never between measurements. `KilledWorker` says *that* the
    workers died; only these logs say whether it was the nanny enforcing the 95% budget
    (line "memory budget") or the kernel OOM killer (no line at all, the process just
    disappears) - a distinction we already paid for once (PROJECT_CONTEXT.md, section 7).
    """
    try:
        logs = client.get_worker_logs()
    except Exception as error:  # a cluster that has just died cannot be interrogated
        print("worker logs non recuperabili:", error)
        return {}

    markers = {"memory_budget": 0, "restarting": 0, "lines": 0}
    with open(path, "w") as fh:
        for address, lines in logs.items():
            fh.write(f"===== {address} =====\n")
            for level, message in lines:
                fh.write(f"{level} {message}\n")
                markers["lines"] += 1
                if "memory budget" in message or "Worker is at" in message:
                    markers["memory_budget"] += 1
                if "Restarting worker" in message or "Nanny killing" in message:
                    markers["restarting"] += 1
    print("written:", path, markers)
    return markers


def cluster_state(client):
    """How much machine is currently working, for the record of a measurement.

    Unmanaged memory is reported as the per-worker MAXIMUM, not the total: the total
    grows with the number of workers and so cannot be compared across the points of a
    worker sweep, while the worst worker is exactly the one that hits `memory_limit`.
    """
    workers = client.scheduler_info()["workers"].values()
    unmanaged = unmanaged_memory(client).values()
    return {
        "workers": len(workers),
        "threads": sum(w.get("nthreads", 0) for w in workers),
        "unmanaged_gb_max": round(max(unmanaged, default=0) / 1e9, 3),
    }


def measure(fn, client, /, repeats=3, out=None, tracker=None, **columns):
    """Time `fn()` `repeats` times and return one record per repetition.

    `fn` and `client` are positional-only: every other keyword becomes a column of the CSV,
    so a measurement that wants a column named `fn` or `client` must not be able to
    collide with the machinery.

    `sweep` runs BETWEEN repetitions, never inside a timed region: it gives back the
    memory the allocator is holding, so the third measurement starts from the same place
    as the first. No `client.restart()` here - restarting between repetitions of the same
    measurement is slow, fragile over SSH and pollutes the timings
    (docs/MEMORY_LEAK_REPORT.md, section 7.5).

    Keeping every repetition instead of only their mean is deliberate: the spread is what
    tells you whether a difference between two configurations is real.

    Two things this does for an UNATTENDED overnight campaign:

    - `out`: every record is appended to the CSV as soon as it exists. A run that dies at
      3 a.m. keeps everything it had measured until then;
    - a failing configuration is recorded (`seconds` empty, `error` filled) instead of
      killing the campaign. Several points of this campaign are EXPECTED to die - the
      naive combiner, `foldby` on 3.5 GB workers, 25 partitions - and "it did not complete"
      is a result we want in the table, not a traceback in a log. The repetitions of a
      failed point are abandoned: it would fail three times the same way, slowly.
    """
    records = []
    for repeat in range(repeats):
        sweep(client)
        started = time.perf_counter()
        try:
            fn()
            seconds = round(time.perf_counter() - started, 3)
            error = ""
        except Exception as exception:
            seconds, error = None, f"{type(exception).__name__}: {exception}"[:300]
            print(f"  {columns} repeat={repeat} -> FALLITO dopo "
                  f"{time.perf_counter() - started:.1f} s: {error}")
        else:
            print(f"  {columns} repeat={repeat} -> {seconds:6.1f} s")

        record = {**columns, "repeat": repeat, "seconds": seconds,
                  **cluster_state(client), "error": error}
        if tracker is not None:
            record["restarted"] = tracker.restarted_since(client)
        records.append(record)
        if out:
            append_record(record, out)
        if error:
            # Give the nannies a moment to bring back whatever died, so the NEXT
            # configuration does not start on a half cluster and get blamed for it.
            try:
                client.wait_for_workers(1, timeout="120s")
            except Exception:
                pass
            break
    return records


def append_record(record, path):
    """Append one measurement to a CSV, writing the header the first time.

    One CSV per block, which is what keeps this simple: the columns of a block are fixed,
    so the header written by its first record is right for all of them. Extra keys that
    turn up later are dropped rather than silently shifting the columns, and they are
    announced when it happens.
    """
    exists = os.path.exists(path)
    columns = list(record)
    if exists:
        with open(path, newline="") as fh:
            columns = next(csv.reader(fh), columns)
        unexpected = [key for key in record if key not in columns]
        if unexpected:
            print(f"  ATTENZIONE: colonne fuori schema, non salvate in {path}: {unexpected}")
    with open(path, "a", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=columns, extrasaction="ignore", restval="")
        if not exists:
            writer.writeheader()
        writer.writerow(record)


def scale(client, cluster, n_workers):
    """Resize the cluster and block until the workers have actually joined.

    Without the wait, the first measurement of a configuration would be timed while the
    workers are still connecting and would look artificially slow.

    On an SSHCluster the worker pool is the host list, so this can only scale DOWN from
    it: to benchmark 1, 2 and 3 workers, put three worker hosts in cluster.txt and walk
    the sweep downwards.
    """
    cluster.scale(n_workers)
    client.wait_for_workers(n_workers)


def save(records, path):
    """Write the measurements as CSV. One schema for every task, so the report has a
    single table to assemble."""
    if not records:
        return
    columns = list(dict.fromkeys(key for record in records for key in record))
    with open(path, "w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=columns)
        writer.writeheader()
        writer.writerows(records)
    print("written:", path)


def group_seconds(records, x):
    """{value of `x`: [seconds of the repetitions that completed]}.

    Records without a time are the configurations that did not complete. They are dropped
    HERE, at plotting time, and nowhere else: they stay in the CSV, where they say
    something the curve cannot.

    The three ways "no time" can look have to be caught together: `None` straight from
    `measure`, `""` from a CSV read by hand, and NaN from the same CSV read by pandas -
    and NaN is the one that slips through an `in (None, "")` test and poisons the mean.
    """
    by_x = {}
    for record in records:
        seconds = record.get("seconds")
        if seconds is None or seconds == "" or seconds != seconds:  # NaN != NaN
            continue
        by_x.setdefault(record[x], []).append(float(seconds))
    return by_x


def plot_scaling(records, x, path, title, ylabel="seconds"):
    """Execution time against `x`, with the spread of the repetitions visible."""
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    by_x = group_seconds(records, x)
    if not by_x:
        print("niente da plottare per", path)
        return
    xs = sorted(by_x)
    means = [sum(by_x[value]) / len(by_x[value]) for value in xs]
    lows = [mean - min(by_x[value]) for mean, value in zip(means, xs)]
    highs = [max(by_x[value]) - mean for mean, value in zip(means, xs)]

    fig, ax = plt.subplots(figsize=(7, 4.5))
    ax.errorbar(xs, means, yerr=[lows, highs], marker="o", capsize=4, color="#2f6f73")
    ax.set_xlabel(x)
    ax.set_ylabel(ylabel)
    ax.set_title(title)
    ax.grid(alpha=0.25)
    ax.set_ylim(bottom=0)
    fig.tight_layout()
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print("written:", path)


def plot_speedup(records, path, title, x="workers"):
    """Speedup S(n) = T(1)/T(n) and efficiency E(n) = S(n)/n against the number of workers.

    The two curves the course is really asking for behind "execution time vs number of
    executors": the raw times say the job got faster, S and E say by how much of what was
    added. Efficiency is the honest one - on a slice small enough it goes DOWN with more
    workers, and that observation (one worker beating four on the sample, NOTES.md v5) is
    worth more in the report than a curve that scales nicely.

    The reference is the SMALLEST configuration measured, not necessarily 1 worker: if the
    single-worker point did not complete, S is still defined against whatever did.
    """
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    by_x = group_seconds(records, x)
    if not by_x:
        print("niente da plottare per", path)
        return
    xs = sorted(by_x)
    means = {value: sum(by_x[value]) / len(by_x[value]) for value in xs}
    base_x = xs[0]
    speedup = [means[base_x] / means[value] for value in xs]
    efficiency = [s / (value / base_x) for s, value in zip(speedup, xs)]

    fig, (top, bottom) = plt.subplots(2, 1, figsize=(7, 7), sharex=True)
    top.plot(xs, speedup, marker="o", color="#2f6f73", label="misurato")
    top.plot(xs, [value / base_x for value in xs], "--", color="#999", label="ideale")
    top.set_ylabel(f"speedup vs {base_x} {x}")
    top.set_title(title)
    top.legend()
    top.grid(alpha=0.25)

    bottom.plot(xs, efficiency, marker="o", color="#b4674d")
    bottom.axhline(1.0, ls="--", color="#999")
    bottom.set_ylabel("efficienza")
    bottom.set_xlabel(x)
    bottom.set_ylim(bottom=0)
    bottom.grid(alpha=0.25)

    fig.tight_layout()
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print("written:", path)
