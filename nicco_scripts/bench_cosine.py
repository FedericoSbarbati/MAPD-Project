import argparse
import csv
import sys
import time
from pathlib import Path

# Workers import the code from the repo, so the repo root has to be added.
REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(REPO / "Niccolo"))

import cosine as cs  # noqa: E402
from cluster import available_workers, get_client  # noqa: E402

# File sent to the workers: data is scattered, but not the code.
CODICE = REPO / "Niccolo" / "cosine.py"

DEFAULT_INPUT = cs.DEFAULT_INPUT
DEFAULT_OUT = "~/mapd-out/bench_2_3_4"

# Sweep on partitions. NO 1 e 2: the memory peak of a tile scales as
# (titles/k)^2, so at 100,000 titles k=2 would require ~30 GB per task. Low k
# values are UNFEASIBLE, and it's the same wall measured in 2.3.1 at k=32 — it's a memory problem.
BLOCCHI = (4, 8, 16, 32, 64)

# List of point of the thread curve. Explicit and not "how many corse the node has", bevause here the measured variable
# is the number of threads.
THREAD = (1, 2, 4)

CURVE = ("partizioni", "worker", "thread")

COLONNE = ["curva", "valore", "ripetizione", "secondi", "errore",
           "titoli", "partizioni", "worker", "thread"]

# The scheduler always comes up on port 8786 (cluster.py), so the next cluster
# finds it occupied if the previous one hasn't released it yet: "OSError: [Errno 98] Address
# already in use". It doesn't free up on its own - tested on the cluster on 2026-09-10.
PAUSA_FRA_CLUSTER = 10


def cronometra(client, X, k, top, bins):
    """How long the cluster takes to produce the two rankings and the histogram.

`build` is INSIDE the timer, unlike 2.3.2 where the graph was built
outside: here, building it means shipping the blocks to the workers, which is the actual work being measured.
"""
    inizio = time.perf_counter()
    client.compute(cs.build(client, X, k, top, bins), sync=True)
    return round(time.perf_counter() - inizio, 2)


def stato_cluster(client):
    """Worker and thread TRUE. `nthreads()` and not `scheduler_info()`, which under-count when
    more workers are on the same machine."""
    thread = client.nthreads()
    return {"worker": len(thread), "thread": sum(thread.values())}


def baseline_numpy(X, k, top, bins):
    """The same work on a single core, without Dask: the baseline, used as the reference
to verify the speedup obtained.

Same tiles, same k, same reduction, same histogram - just sequential instead
of parallel. Changing k would change the work itself, not just its distribution.
"""
    tagli = cs.split_rows(len(X), k)
    inizio = time.perf_counter()
    pezzi = [cs.top_block(X[tagli[i][0]:tagli[i][1]], X[tagli[j][0]:tagli[j][1]],
                          tagli[i][0], tagli[j][0], top, bins)
             for i, j in cs.block_pairs(len(tagli))]
    cs.merge(pezzi, top)
    return round(time.perf_counter() - inizio, 2)


def scrivi_riga(percorso, riga):
    """A CSV row for each measurement, in append: a interrupted campaign does not compromise its data."""
    nuovo = not percorso.exists()
    with open(percorso, "a", newline="") as fh:
        scrittore = csv.DictWriter(fh, fieldnames=COLONNE, extrasaction="ignore", restval="")
        if nuovo:
            scrittore.writeheader()
        scrittore.writerow(riga)


def misura(client, X, k, top, base):
    """Function to time the work, errors are captured as strings instead of interruptions
    of the campaign. A k that exceeds memory is data to be saved, not a path incident."""
    riga = dict(base, titoli=len(X), partizioni=k, secondi=None, errore="")
    riga.update(stato_cluster(client))
    try:
        riga["secondi"] = cronometra(client, X, k, top, cs.BINS)
    except Exception as errore:
        riga["errore"] = f"{type(errore).__name__}: {errore}"[:200]
    return riga


def campagna(disponibili, k_riferimento, thread, curve=None, quali_worker=None):
    """The FORMS OF CLUSTER to turn on and what to measure inside each.

    -> {(worker, thread): [(curva, k), ...]}

    A cluster turns on ONE time and we measure all its k inside. The curve on the
    partitions goes around the full cluster; that on the workers keeps k fixed at the reference and
    changes the number of processes; that on the threads keeps worker and k fixed and changes the
    threads of each process.

    The point (full cluster, reference k) belongs to all three, and is the pivot on
    which we read together: in the graphs the x-axis is read from the state columns
    (`partizioni`, `worker`, `thread`), never from the tag `curva`.
    """
    punti = [("partizioni", disponibili, thread, k) for k in BLOCCHI]
    punti += [("worker", w, thread, k_riferimento) for w in range(disponibili, 0, -1)]
    punti += [("thread", disponibili, t, k_riferimento) for t in THREAD]

    if curve:
        punti = [p for p in punti if p[0] in curve]
    if quali_worker:
        punti = [p for p in punti if p[1] in quali_worker]

    forme = {}
    for curva, worker, thread_, k in punti:
        forme.setdefault((worker, thread_), []).append((curva, k))
    return forme


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("input", nargs="?", default=DEFAULT_INPUT,
                        help=f"embeddings folder (default {DEFAULT_INPUT})")
    parser.add_argument("--out", default=DEFAULT_OUT, help=f"where to write (default {DEFAULT_OUT})")
    parser.add_argument("--titoli", type=int, default=cs.DEFAULT_TITLES, metavar="N",
                        help=f"how many titles (default {cs.DEFAULT_TITLES}). STAYS FIXED for "
                             "the whole campaign: it's the scale of the problem, not a curve. The "
                             "cost scales as its SQUARE")
    parser.add_argument("--ripetizioni", type=int, default=1, metavar="N",
                        help="how many times to repeat the WHOLE campaign (default 1). "
                             "Repetitions are passed as complete runs, not consecutive measurements "
                             "of the same point: the resulting spread also includes the "
                             "machine's variability over time")
    parser.add_argument("--k", type=int, metavar="N", default=cs.DEFAULT_BLOCKS,
                        help=f"the blocks for the worker and thread curves (default "
                             f"{cs.DEFAULT_BLOCKS}, i.e. the task's default). It's the "
                             "working point at which the speedup is measured")
    parser.add_argument("--thread", type=int, metavar="N",
                        help="threads per worker in the partition and worker curves "
                             "(default: however many cores the node has). The thread curve uses "
                             f"its own values: {THREAD}")
    parser.add_argument("--top", type=int, default=cs.TOP_N,
                        help=f"how many pairs per ranking (default {cs.TOP_N})")
    parser.add_argument("--seed", type=int, default=cs.SEED,
                        help=f"the sample seed (default {cs.SEED}): all measurements "
                             "must look at the same titles")
    parser.add_argument("--only", nargs="+", metavar="CURVA", choices=CURVE,
                        help=f"only rerun these curves: {' '.join(CURVE)} (default: all)")
    parser.add_argument("--worker", nargs="+", type=int, metavar="N",
                        help="only measure these worker counts, instead of the whole curve. "
                             "Used for constant-core comparisons")
    return parser.parse_args()


def main():
    args = parse_args()

    source = Path(args.input).expanduser()
    source = source if source.is_absolute() else REPO / source
    out = Path(args.out).expanduser()
    out = out if out.is_absolute() else REPO / out
    out.mkdir(parents=True, exist_ok=True)
    percorso_csv = out / "misure.csv"

    if not cs.embedding_files(source):
        raise SystemExit(f"Nessun file .parquet in {source}")

    disponibili = available_workers(REPO)
    forme = campagna(disponibili, args.k, args.thread, args.only, args.worker)
    if not forme:
        raise SystemExit("The Selection doesn't contain any measurement: check --only and --worker")

    # Fuori dal cronometro, e una volta per tutta la campagna: ogni misura deve guardare
    # gli stessi vettori, altrimenti confronta problemi diversi.
    uid, X = cs.load_vectors(source, args.titoli, args.seed)
    X = cs.normalize(X)
    coppie = len(X) * (len(X) - 1) // 2

    print(f"input   : {source}")
    print(f"csv     : {percorso_csv}  (in append)")
    print(f"titoli  : {len(X):,}  ({X.nbytes / 1e6:.0f} MB)  ->  {coppie:,} coppie")
    print(f"worker  : fino a {disponibili}")
    print(f"campagna: {len(forme)} cluster x {args.ripetizioni} passate")
    for (w, t), punti in forme.items():
        print(f"  worker={w} thread={t or 'default'} -> "
              f"{', '.join(f'{c}:{k}' for c, k in punti)}")

    inizio = time.perf_counter()

    for ripetizione in range(args.ripetizioni):
        secondi = baseline_numpy(X, args.k, args.top, cs.BINS)
        scrivi_riga(percorso_csv, {"curva": "numpy", "valore": 1, "ripetizione": ripetizione,
                                   "secondi": secondi, "errore": "", "titoli": len(X),
                                   "partizioni": args.k, "worker": 1, "thread": 1})
        print(f"\n[{ripetizione}] numpy, un core: {secondi} s")

        # One cluster per SHAPE (worker x thread): it turns on once and we measure all its k inside,
        for (worker, thread), punti in forme.items():
            client = cluster = None
            try:
                cs.single_thread_blas()         # it turns on before any worker starts
                client, cluster = get_client(repo_root=REPO, n_workers=worker,
                                             n_threads=thread)
                client.upload_file(str(CODICE))

                for curva, k in punti:
                    valore = k if curva == "partizioni" else (worker if curva == "worker"
                                                              else thread)
                    base = {"curva": curva, "valore": valore, "ripetizione": ripetizione}
                    riga = misura(client, X, k, args.top, base)
                    scrivi_riga(percorso_csv, riga)
                    print(f"[{ripetizione}] {curva}={riga['valore']:<5} "
                          f"worker={riga['worker']} thread={riga['thread']} "
                          f"partizioni={riga['partizioni']} -> "
                          f"{riga['secondi']} s {riga['errore']}")

            # One cluster that doesn't start shouldn't take the campaign down: its measurements
            # become rows with the error, and we move on to the next shape.
            except Exception as errore:
                detto = f"{type(errore).__name__}: {errore}"[:200]
                for curva, k in punti:
                    scrivi_riga(percorso_csv,
                                {"curva": curva, "valore": k, "ripetizione": ripetizione,
                                 "secondi": None, "errore": detto, "titoli": len(X),
                                 "partizioni": k, "worker": worker})
                print(f"[{ripetizione}] worker={worker}: CLUSTER FALLITO  {detto}")
            finally:
                if client is not None:
                    client.close()
                if cluster is not None:
                    cluster.close()
                time.sleep(PAUSA_FRA_CLUSTER)   # la 8786 has to be freed up for the next cluster 


    print(f"\ncampagna finita in {(time.perf_counter() - inizio) / 60:.1f} minuti")
    print("csv:", percorso_csv)


if __name__ == "__main__":
    main()
