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
    """The cluster host list, or None when we should run locally."""
    if os.environ.get("CORD19_HOSTS"):
        return os.environ["CORD19_HOSTS"]
    path = os.path.join(repo_root, HOSTS_FILE)
    if os.path.exists(path):
        for line in open(path):
            line = line.strip()
            if line and not line.startswith("#"):
                return line
    return None


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


def get_client(repo_root="."):
    """Connect to whichever cluster this machine is configured for.

    Returns (client, cluster); `cluster` is None when we only attached to a scheduler
    somebody else started. Close both when done.
    """
    from dask.distributed import Client, LocalCluster

    configure_memory()  # must happen before any worker process is spawned

    scheduler = os.environ.get("DASK_SCHEDULER")
    hosts = read_hosts(repo_root)
    threads = int(os.environ.get("CORD19_THREADS_PER_WORKER", "4"))
    memory = os.environ.get("CORD19_WORKER_MEMORY_LIMIT", "7GB")

    if scheduler:
        return Client(scheduler), None

    if hosts:
        from dask.distributed import SSHCluster

        addresses = [h.strip() for h in hosts.split(",") if h.strip()]
        connect = {"known_hosts": None}  # the internal IPs get recycled between sessions
        if os.environ.get("CORD19_SSH_KEY"):
            connect["client_keys"] = [os.environ["CORD19_SSH_KEY"]]
        cluster = SSHCluster(
            addresses,
            connect_options=connect,
            worker_options={"nthreads": threads, "memory_limit": memory},
            scheduler_options={"port": 8786, "dashboard_address": ":8787"},
            # None means "the same interpreter path as the driver". The workers are
            # snapshot clones of the scheduler, so ~/pyvenv sits at the same path.
            remote_python=os.environ.get("CORD19_REMOTE_PYTHON"),
        )
        return Client(cluster), cluster

    cluster = LocalCluster(
        n_workers=int(os.environ.get("CORD19_WORKERS", "4")),
        threads_per_worker=threads,
        memory_limit=memory,
        processes=True,
    )
    return Client(cluster), cluster


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


def measure(fn, client, repeats=3, **columns):
    """Time `fn()` `repeats` times and return one record per repetition.

    `sweep` runs BETWEEN repetitions, never inside a timed region: it gives back the
    memory the allocator is holding, so the third measurement starts from the same place
    as the first. No `client.restart()` here - restarting between repetitions of the same
    measurement is slow, fragile over SSH and pollutes the timings
    (docs/MEMORY_LEAK_REPORT.md, section 7.5).

    Keeping every repetition instead of only their mean is deliberate: the spread is what
    tells you whether a difference between two configurations is real.
    """
    records = []
    for repeat in range(repeats):
        sweep(client)
        started = time.perf_counter()
        fn()
        seconds = time.perf_counter() - started
        records.append(
            {**columns, "repeat": repeat, "seconds": round(seconds, 3), **cluster_state(client)}
        )
        print(f"  {columns} repeat={repeat} -> {seconds:6.1f} s")
    return records


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


def plot_scaling(records, x, path, title, ylabel="seconds"):
    """Execution time against `x`, with the spread of the repetitions visible."""
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    by_x = {}
    for record in records:
        by_x.setdefault(record[x], []).append(record["seconds"])
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
