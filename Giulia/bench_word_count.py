"""Benchmark campaign for task 2.3.1 - one block per invocation.

    python Giulia/bench_word_count.py A2 --input ~/mapd-data/silver/paragraphs

The course requires the benchmarks and grades an analysis without them as incomplete
(InstructionsAndGuidelines, point 5): execution time against AT LEAST the number of
dataset partitions and the number of executors. A2 and A6 are those two; everything else
is ours.

    A1   phases: read only / Map / whole job          where the time actually goes
    A2   time vs number of partitions   (MANDATORY)   is there an optimum?
    A3a  time vs reduce strategy, topk payload        foldby against a sharded groupby
    A3b  same strategies, writing the vocabulary      the payload that killed the cluster
    A4   combiner granularity, growing slices         what the Map phase key costs
    A5   time vs volume of data                       is the algorithm linear?
    A6   time vs number of workers      (MANDATORY)   speedup and efficiency - RUN LAST
    B    the standard payload, cluster shape from the environment (processes vs threads)
    D1   reference run on the WHOLE corpus, with a performance report
    D2   the same on the whole corpus with foldby, under a timeout

Why one block per invocation, rather than one script that runs them all: a block that
dies takes only itself down and is relaunched alone. Every measurement is appended to its
CSV the moment it exists, so a campaign interrupted at 3 a.m. keeps what it had.

TWO ORDERING RULES, both learned the expensive way:

    A6 goes LAST in any cluster session. It scales the cluster DOWN and on an SSHCluster
    the worker pool is the host list, so it cannot come back up. Running it before A3
    silently measures the reduce strategies on one worker - which is exactly what the
    notebook did.

    B needs a differently shaped cluster, so it is a separate process with its own
    environment (CORD19_THREADS_PER_WORKER, CORD19_WORKER_MEMORY_FRACTION, cluster.txt).
"""

import argparse
import sys
import time
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(REPO / "Giulia"))

import word_count as wc  # noqa: E402
from bench import (  # noqa: E402
    WorkerTracker,
    dump_worker_logs,
    get_client,
    measure,
)

TOP_N = 20
DEFAULT_INPUT = "data/silver/paragraphs"
DEFAULT_OUT = "reports/bench"

# The slice every sweep is measured on, as a fraction of the corpus. Fixed across the
# whole campaign so the blocks are comparable with each other, and small enough that the
# one-worker point of A6 stays under ten minutes. The absolute number of files is NOT
# hard-coded: the VM printed 990 partitions where the local copy has 1979, and a constant
# written here would mean a different experiment on the two machines.
SLICE_FRACTION = 0.20

# Timeout for D2 only, where the point of the measurement is that it does not finish.
D2_TIMEOUT = 3600


# ----------------------------------------------------------------------------------
# Building the measured job
# ----------------------------------------------------------------------------------


def slice_size(files):
    """(rows, parquet bytes) of a list of files, from the Parquet footers only.

    Metadata, so no text is read. `rows` counts the paragraphs BEFORE the
    `is_reference_like` filter (1.24% of them); it is here to turn seconds into a
    throughput, not to be an exact corpus statistic.
    """
    import pyarrow.parquet as pq

    return (
        sum(pq.ParquetFile(file).metadata.num_rows for file in files),
        sum(Path(file).stat().st_size for file in files),
    )


def bag_of(files, partitions=None):
    """The Bag of (cord_uid, text) for `files`, split into `partitions` partitions.

    Returns (bag, how). One file is the finest grain the grouping reader can reach - a
    silver Parquet holds a single row group, and a row group cannot be split - so asking
    for more partitions than files falls back to `repartition`, which splits the
    already-read partitions. That is a different mechanism and it is recorded as such
    instead of being hidden in the same column.
    """
    if partitions is None or partitions <= len(files):
        return wc.read_groups(wc.split_evenly(files, partitions or len(files))), "groups"
    bag = wc.read_groups(wc.split_evenly(files, len(files)))
    return bag.repartition(npartitions=partitions), "repartition"


def topk_payload(bag, split_out=wc.SPLIT_OUT, combiner="document"):
    """The measured job: the whole Map/Reduce, collecting only the top N.

    `topk` and not the full vocabulary because it keeps the driver out of the measurement
    while still forcing every partition through both phases - each partition forwards its
    own top N and nothing prunes the reduce. What it leaves out is measured separately in
    A3b, and A3b is where we found out that it leaves out the part that crashes.
    """
    _, global_counts = wc.word_count(bag, split_out=split_out, combiner=combiner)
    return lambda: global_counts.topk(TOP_N, key=1).compute()


def parquet_payload(bag, split_out, out):
    """The real job: the whole vocabulary written to Parquet."""
    _, global_counts = wc.word_count(bag, split_out=split_out)
    frame = global_counts.to_dataframe(meta={"word": "string", "count": "int64"})
    return lambda: frame.to_parquet(
        out, write_index=False, compression="zstd", overwrite=True
    )


def safe_count(collection, label):
    """Compute a count, untimed, and survive the configurations that cannot.

    Used for the number of rows LEAVING the Map phase, which is the shuffle volume - the
    x axis that makes A4 mean something. It is not part of any timed region.
    """
    started = time.perf_counter()
    try:
        value = collection.count().compute()
        print(f"    {label}: {value:,} righe dal Map ({time.perf_counter() - started:.0f} s)")
        return value
    except Exception as exception:
        print(f"    {label}: non calcolabile ({type(exception).__name__})")
        return None


# ----------------------------------------------------------------------------------
# The blocks
# ----------------------------------------------------------------------------------


def block_a1(ctx):
    """Where the time goes: reading, the Map phase, the whole job.

    Three nested jobs on the same data, so the differences are the phases. The read-only
    point matters more here than on a laptop: every byte comes from one NFS server
    (PROJECT_CONTEXT.md, section 5), and if it dominates then none of the other curves are
    about computation.
    """
    bag, _ = bag_of(ctx.files)
    doc_counts, _ = wc.word_count(bag)
    phases = {
        "read": lambda: bag.count().compute(),
        "map": lambda: doc_counts.count().compute(),
        "full": topk_payload(bag),
    }
    for phase, payload in phases.items():
        ctx.measure(payload, phase=phase, partitions=bag.npartitions)


def block_a2(ctx):
    """MANDATORY: time against the number of partitions, on a FIXED slice of data.

    The data does not change, only how it is cut. Measuring larger and larger slices - what
    the first version of this benchmark did - answers a different question, and that one is
    A5. At fixed data, "number of partitions" IS "partition size".

    The bottom of the range is expected to fail on 3.5 GB workers: a sixteenth of the
    partitions means sixteen times the peak per task. That is the result, not a gap in the
    table - too few partitions is not slower, it is infeasible.
    """
    base = len(ctx.files)
    for k in (base // 16, base // 8, base // 4, base // 2, base, base * 2, base * 4):
        bag, how = bag_of(ctx.files, k)
        ctx.measure(topk_payload(bag), partitions=bag.npartitions, partition_control=how)


def block_a3a(ctx):
    """Time against where the Reduce ends up. Same result, same data, same cluster.

    `split_out=0` is `Bag.foldby`, which collapses into ONE partition; the others shard the
    groupby over N. `split_out=1` is the point that makes the comparison readable: it also
    ends in a single output partition, so foldby-against-1 isolates "Python dict against
    Arrow" and 1-against-16 isolates "serial tail against parallel tail". Without it the
    two effects are read as one number.
    """
    bag, _ = bag_of(ctx.files)
    for split_out in (0, 1, 4, 16, 64, 256):
        ctx.measure(
            topk_payload(bag, split_out=split_out),
            partitions=bag.npartitions, split_out=split_out, payload="topk",
        )


def block_a3b(ctx):
    """The same strategies, but writing the whole vocabulary - the job we actually ship.

    This is the controlled reproduction of the crash of 2026-08-11: on the full corpus,
    with foldby, the run died in `KilledWorker` on task
    ('foldby-b-to_dataframe-...', 0) while writing the Parquet, AFTER the topk of the line
    above had gone through. So the two payloads do not merely differ in seconds, they
    differ in whether the job exists at all - and the single foldby partition breaks when
    it has to become a pandas DataFrame of millions of tuples, not when it reduces.
    """
    bag, _ = bag_of(ctx.files)
    for split_out in (0, 16):
        out = ctx.out / f"vocabulary_split{split_out}"
        ctx.measure(
            parquet_payload(bag, split_out, out),
            partitions=bag.npartitions, split_out=split_out, payload="parquet",
        )


def block_a4(ctx):
    """The combiner ladder, against growing slices: what the Map phase key costs.

    L0 sends one record per OCCURRENCE, L1 one per (document, word) - the assignment's own
    formulation - and L2 one per (partition, word). Measured against growing slices rather
    than at one size because the interesting quantity is not the ratio of the times, it is
    where each rung stops completing: the combiner does not only save time, it moves the
    frontier of what runs at all. `map_rows` is the shuffle volume, and it is what the
    times should be read against.
    """
    base = len(ctx.files)
    for n_files in (max(2, base // 40), max(4, base // 16), max(8, base // 8), base // 4):
        files = ctx.files[:n_files]
        rows, nbytes = slice_size(files)
        for combiner in ("none", "document", "partition"):
            bag, _ = bag_of(files)
            doc_counts, _ = wc.word_count(bag, combiner=combiner)
            print(f"  {combiner} su {n_files} file")
            ctx.measure(
                topk_payload(bag, combiner=combiner),
                partitions=bag.npartitions, combiner=combiner, files=n_files,
                rows=rows, parquet_bytes=nbytes,
                map_rows=safe_count(doc_counts, f"{combiner}/{n_files}"),
            )


def block_a5(ctx):
    """Time against the volume of data, at CONSTANT partition size.

    One partition per file at every point, so the partitions stay the same size and only
    how many there are changes. Together with A2 this separates the two things the old
    benchmark measured at once.
    """
    base = len(ctx.files)
    for n_files in (base // 8, base // 4, base // 2, base, min(base * 2, len(ctx.all_files))):
        files = ctx.all_files[:n_files]
        rows, nbytes = slice_size(files)
        bag, _ = bag_of(files)
        ctx.measure(
            topk_payload(bag), partitions=bag.npartitions, files=n_files,
            rows=rows, parquet_bytes=nbytes,
        )


def block_a6(ctx):
    """MANDATORY: time against the number of workers. ALWAYS THE LAST BLOCK OF A SESSION.

    Fixed partitioning, fixed data: only how much machine is working changes. The sweep
    goes DOWNWARDS because on an SSHCluster the worker pool is the host list and `scale`
    cannot add hosts that are not in it.
    """
    from bench import scale

    if ctx.cluster is None:
        raise SystemExit("A6 ha bisogno di un cluster ridimensionabile, non di un "
                         "Client attaccato a uno scheduler altrui (DASK_SCHEDULER).")
    bag, _ = bag_of(ctx.files)
    workers = len(ctx.client.scheduler_info()["workers"])
    for n_workers in range(workers, 0, -1):
        scale(ctx.client, ctx.cluster, n_workers)
        ctx.measure(topk_payload(bag), partitions=bag.npartitions, requested_workers=n_workers)


def block_b(ctx):
    """The standard payload on whatever shape the environment built. Processes vs threads.

    The block itself is trivial; the experiment is in how the cluster was created, and
    `cluster_state` records the workers and threads that really turned up:

        B1  CORD19_THREADS_PER_WORKER=1                        4 workers x 1 thread
        B2  the block A configuration                          4 workers x 2 threads
        B3  each IP twice in cluster.txt, FRACTION=0.40        8 workers x 1 thread

    B1 against B2 is the cleanest question we can ask this code: same cluster, same memory,
    twice the threads. The tokenizer is `re.findall` plus `Counter`, pure Python, and the
    GIL is not released by either - so if the two times coincide, the second core of each
    node is producing nothing. B2 against B3 then asks the same 8 cores to work as
    processes instead of threads.

    The memory fraction is not a detail in B3: `worker_options` sizes each worker as a
    share of the NODE's RAM and does not divide it by the number of workers on that node,
    so two workers per host at the default 0.85 would each believe they own 3.5 GB of a
    4 GB machine.
    """
    bag, _ = bag_of(ctx.files)
    ctx.measure(topk_payload(bag), partitions=bag.npartitions, shape=ctx.args.label or "?")


def block_d1(ctx):
    """Reference run on the WHOLE corpus, once, with a performance report.

    Not repeated: this is not a point on a curve, it is the number to quote and the task
    stream to show. The Bokeh report is where the shape of the job is visible at a glance -
    the reduce tail above all - and it is the artefact for the presentation.
    """
    from distributed import performance_report

    files = ctx.all_files
    rows, nbytes = slice_size(files)
    bag, _ = bag_of(files)
    with performance_report(filename=str(ctx.out / "d1_full_corpus.html")):
        ctx.measure(
            topk_payload(bag), repeats=1, partitions=bag.npartitions,
            files=len(files), rows=rows, parquet_bytes=nbytes, split_out=wc.SPLIT_OUT,
        )
    print("written:", ctx.out / "d1_full_corpus.html")


def block_d2(ctx):
    """The whole corpus with foldby, under a timeout. Expected to fail; run it last.

    On the Mac at 3.4 GB per worker this finished in 1762 s having lost 3 workers on the
    way. On the cluster, at 3.5 GB, it did not finish at all. The measurement exists to
    turn that traceback into a bounded statement - "it did not complete in N minutes on
    four real nodes" - with the worker logs to say who killed them. It goes last precisely
    because whatever it does, nothing is waiting behind it.
    """
    from distributed import wait

    files = ctx.all_files
    rows, nbytes = slice_size(files)
    bag, _ = bag_of(files)
    _, global_counts = wc.word_count(bag, split_out=0)

    def run():
        future = ctx.client.compute(global_counts.topk(TOP_N, key=1))
        try:
            wait([future], timeout=D2_TIMEOUT)
        except Exception:
            ctx.client.cancel(future)
            raise TimeoutError(f"non completato in {D2_TIMEOUT} s")
        return future.result()

    ctx.measure(run, repeats=1, partitions=bag.npartitions, files=len(files),
                rows=rows, parquet_bytes=nbytes, split_out=0)


BLOCKS = {
    "A1": block_a1, "A2": block_a2, "A3a": block_a3a, "A3b": block_a3b,
    "A4": block_a4, "A5": block_a5, "A6": block_a6,
    "B": block_b, "D1": block_d1, "D2": block_d2,
}


# ----------------------------------------------------------------------------------
# Driver
# ----------------------------------------------------------------------------------


class Context:
    """What every block needs, and the one place the common columns are filled in."""

    def __init__(self, args, client, cluster, all_files, files, out, csv_path):
        self.args, self.client, self.cluster = args, client, cluster
        self.all_files, self.files = all_files, files
        self.out, self.csv_path = out, csv_path
        self.tracker = WorkerTracker(client)
        self.rows, self.parquet_bytes = slice_size(files)
        self.records = []

    # `payload` and `repeats` are positional-only: everything else becomes a CSV column,
    # and a block that wants a column literally called "payload" (A3a and A3b do) must not
    # collide with the parameter carrying the job.
    def measure(self, payload, /, repeats=None, **columns):
        """Time one configuration, with the slice's size attached to every record.

        Rows and bytes travel with the measurement so the CSV is self-describing: a table
        of seconds alone cannot be turned into a throughput six weeks later, and the blocks
        that vary the volume would have no x axis at all.
        """
        defaults = {"block": self.args.block, "files": len(self.files),
                    "rows": self.rows, "parquet_bytes": self.parquet_bytes}
        self.records += measure(
            payload, self.client,
            repeats=self.args.repeats if repeats is None else repeats,
            out=self.csv_path, tracker=self.tracker, **{**defaults, **columns},
        )


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("block", choices=sorted(BLOCKS), help="which block to run")
    parser.add_argument("--input", default=DEFAULT_INPUT, help=f"silver/paragraphs (default {DEFAULT_INPUT})")
    parser.add_argument("--out", default=DEFAULT_OUT, help=f"output directory (default {DEFAULT_OUT})")
    parser.add_argument("--repeats", type=int, default=3, help="repetitions per configuration")
    parser.add_argument("--slice-files", type=int, help="files in the benchmark slice "
                                                        f"(default: {SLICE_FRACTION:.0%} of the corpus)")
    parser.add_argument("--label", help="a name for this cluster shape, recorded in block B")
    return parser.parse_args()


def main():
    args = parse_args()

    source = Path(args.input).expanduser()
    source = source if source.is_absolute() else REPO / source
    out = Path(args.out).expanduser()
    out = out if out.is_absolute() else REPO / out
    if not source.exists():
        raise SystemExit(f"Input non trovato: {source}")
    out.mkdir(parents=True, exist_ok=True)

    all_files = wc.paragraph_files(source)
    if not all_files:
        raise SystemExit(f"Nessun file .parquet in {source}")
    n_slice = args.slice_files or max(1, round(len(all_files) * SLICE_FRACTION))
    files = all_files[:min(n_slice, len(all_files))]

    client, cluster = get_client(repo_root=REPO)
    csv_path = out / f"{args.block}.csv"
    print(f"blocco    : {args.block}")
    print(f"input     : {source}  ({len(all_files)} file)")
    print(f"fetta     : {len(files)} file ({len(files) / len(all_files):.1%} del corpus)")
    print(f"ripetizioni: {args.repeats}")
    print(f"csv       : {csv_path}  (append)")

    context = Context(args, client, cluster, all_files, files, out, csv_path)
    print(f"paragrafi : {context.rows:,} righe, {context.parquet_bytes / 1e6:.0f} MB di Parquet")
    try:
        BLOCKS[args.block](context)
    finally:
        # The logs are the only place that says WHY a worker died, and they have to be
        # taken before the cluster goes away.
        dump_worker_logs(client, out / f"{args.block}_workers.log")
        client.close()
        if cluster is not None:
            cluster.close()

    done = [r for r in context.records if r.get("seconds") is not None]
    print(f"\nblocco {args.block}: {len(done)}/{len(context.records)} misure completate")
    print("written:", csv_path)


if __name__ == "__main__":
    main()
