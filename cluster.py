import os

HOSTS_FILE = "cluster.txt"

def read_hosts(repo_root="."):
    """
    Return the host list and its source: (hosts, source).
    If none is found, return (None, searched locations) so callers can report where they
    looked.
    """
    if os.environ.get("CORD19_HOSTS"):
        return os.environ["CORD19_HOSTS"], "$CORD19_HOSTS"

    cercati = []
    for directory in (repo_root, os.getcwd(), os.path.expanduser("~")):
        percorso = os.path.abspath(os.path.join(directory, HOSTS_FILE))
        if percorso in cercati:
            continue
        cercati.append(percorso)
        if os.path.exists(percorso):
            for riga in open(percorso):
                riga = riga.strip()
                if riga and not riga.startswith("#"):
                    return riga, percorso
    return None, cercati


def available_workers(repo_root="."):
    """
    Return the number of workers available to `get_client(n_workers=...)`.
    Used by the "time vs. workers" benchmark to define its range. With `cluster.txt`,
    this is the number of hosts minus the first, which acts only as the scheduler.
    """
    hosts, _ = read_hosts(repo_root)
    if hosts:
        return len([h for h in hosts.split(",") if h.strip()]) - 1
    core = os.cpu_count() or 2
    return int(os.environ.get("CORD19_WORKERS", max(1, min(4, core // 2))))


def configure_memory():
    """
    BY THE STUDENT GROUP: CREATED BY CLAUDE CODE TO HELP DEBUG A SUSPECTED MEMORY LEAK.
    Not useful for the purpose of analysis since the memory leak problem could have been solved just
    using VM with a large amount of RAM during the conversion from JSON to Parquet.
    This is not meaningful for the purpose of the analysis and exam but we where still curious to find out why.


    Settings that keep worker memory stable during long text-processing jobs.

    `MALLOC_TRIM_THRESHOLD_` and `MALLOC_ARENA_MAX` must reach the worker process before
    it starts, before glibc initializes its allocator. Passing them through
    `worker_options={"env": ...}` is too late. `pre-spawn-environ` runs early enough, and
    `SSHCluster` propagates the driver's configuration to the nodes. See
    `docs/MEMORY_LEAK_REPORT.md` §7.2.

    Call this before creating the cluster; this is why Client creation is centralized here
    instead of being duplicated across tasks.
    """
    import dask

    environ = dict(dask.config.get("distributed.nanny.pre-spawn-environ"))
    environ.update({"MALLOC_TRIM_THRESHOLD_": 0, "MALLOC_ARENA_MAX": 2})
    dask.config.set({"distributed.nanny.pre-spawn-environ": environ})

    dask.config.set({"dataframe.shuffle.method": "tasks"})


def worker_options(n_threads=None):
    """
    Build the worker configuration used when starting a Dask cluster.

    The memory limit is set as a fraction of available memory, unless an explicit limit is
    provided through `CORD19_WORKER_MEMORY_LIMIT`. The number of threads comes from
    `n_threads`, when specified, or from `CORD19_THREADS_PER_WORKER`.

    Returns a dictionary containing the worker options.
    """

    opzioni = {"memory_limit": float(os.environ.get("CORD19_WORKER_MEMORY_FRACTION", "0.85"))}
    if os.environ.get("CORD19_WORKER_MEMORY_LIMIT"):
        opzioni["memory_limit"] = os.environ["CORD19_WORKER_MEMORY_LIMIT"]
    thread = n_threads or os.environ.get("CORD19_THREADS_PER_WORKER")
    if thread:
        opzioni["nthreads"] = int(thread)
    return opzioni


def describe(client, modo, dettaglio):
    """Print a summary of the active Dask cluster.

    It reports the number of workers, hosts, threads, memory limits, dashboard URL,
    and the cluster configuration.
    """

    thread = client.nthreads()
    host = {indirizzo.rsplit("://", 1)[-1].rsplit(":", 1)[0] for indirizzo in thread}

    conosciuti = list(client.scheduler_info()["workers"].values())
    per_worker = conosciuti[0].get("memory_limit", 0) if conosciuti else 0 # memory from a single worker
    memoria = per_worker * len(thread)
    print("=" * 70)
    print(f"cluster   : {modo}")
    print(f"            {dettaglio}")
    print(f"worker    : {len(thread)} su {len(host)} host {sorted(host)}")
    print(f"thread    : {sum(thread.values())}")
    print(f"memoria   : {memoria / 1e9:.1f} GB totali "
          f"({per_worker / 1e9:.1f} GB per worker)")
    print(f"dashboard : {client.dashboard_link}")
    if modo == "LocalCluster":
        print("            ATTENZIONE: tutto su questa sola macchina. Per usare il")
        print("            cluster serve cluster.txt nella root del repo.")
    print("=" * 70)


def get_client(repo_root=".", n_workers=None, n_threads=None):
    """
    Create or connect to a Dask cluster using the available configuration.

    The function first connects to the scheduler specified by `DASK_SCHEDULER`, then
    checks the configured hosts and creates an `SSHCluster`. If neither is available, it
    creates a local cluster. `n_workers` and `n_threads` control the cluster size.

    Returns a `(client, cluster)` tuple. `cluster` is `None` when connecting to an existing
    scheduler.
    """
    from dask.distributed import Client, LocalCluster

    configure_memory()  # before the creation of the worker processes

    scheduler = os.environ.get("DASK_SCHEDULER")
    hosts, provenienza = read_hosts(repo_root)

    if scheduler:
        client = Client(scheduler)
        describe(client, "scheduler esistente", scheduler)
        return client, None

    if hosts:
        from dask.distributed import SSHCluster

        indirizzi = [h.strip() for h in hosts.split(",") if h.strip()]
        if len(indirizzi) < 2:
            raise SystemExit(
                f"{provenienza} elenca un solo host ({indirizzi}). Il PRIMO host fa solo\n"
                "da scheduler: servono almeno due voci perché esista un worker. Ripeti\n"
                "lo scheduler come secondo host se vuoi che lavori anche lui."
            )
        if n_workers is not None:
            disponibili = len(indirizzi) - 1
            if not 1 <= n_workers <= disponibili:
                raise SystemExit(
                    f"Chiesti {n_workers} worker ma {provenienza} ne elenca "
                    f"{disponibili}. Aggiungi host a cluster.txt"
                )
            indirizzi = indirizzi[:n_workers + 1]

        connessione = {"known_hosts": None}  # ip recicled between session
        if os.environ.get("CORD19_SSH_KEY"):
            connessione["client_keys"] = [os.environ["CORD19_SSH_KEY"]]

        cluster = SSHCluster(
            indirizzi,
            connect_options=connessione,
            worker_options=worker_options(n_threads),
            scheduler_options={"port": 8786, "dashboard_address": ":8787"},
            remote_python=os.environ.get("CORD19_REMOTE_PYTHON"),
        )
        client = Client(cluster)
        client.wait_for_workers(len(indirizzi) - 1, timeout="180s")
        describe(client, "SSHCluster",
                 f"{provenienza} -> scheduler {indirizzi[0]}, worker {indirizzi[1:]}")
        return client, cluster

    # Local cluster setup
    core = os.cpu_count() or 2
    quanti = n_workers or available_workers(repo_root)
    opzioni = worker_options(n_threads)
    per_worker = max(1, core // available_workers(repo_root))
    cluster = LocalCluster(
        n_workers=quanti,
        threads_per_worker=opzioni.get("nthreads", per_worker),
        memory_limit=opzioni["memory_limit"] / quanti
        if isinstance(opzioni["memory_limit"], float)
        else opzioni["memory_limit"],
        processes=True,
    )
    client = Client(cluster)
    describe(client, "LocalCluster", f"nessun cluster.txt in: {', '.join(provenienza)}")
    return client, cluster
