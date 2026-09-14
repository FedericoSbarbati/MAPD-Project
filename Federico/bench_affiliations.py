"""Benchmark of task 2.3.2: runtime vs number of partitions and vs number of workers.

silver/authors is 34 MB and 2.9 million rows, and the same work in pandas on one core
takes fractions of a second: these curves measure the COST OF COORDINATION, not the
computation. Hence one cluster per worker count (and not one per measurement), and the
pandas single-core baseline written into the CSV as the curva="pandas" row.

    python Federico/bench_affiliations.py --out /tmp/bench-2_3_2            # rehearsal
    python Federico/bench_affiliations.py ~/mapd-data/silver/authors --ripetizioni 3
"""

import argparse
import csv
import sys
import time
from pathlib import Path

# Adding the repo root to syspath in order to make code executable on workers
REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(REPO / "Federico"))

import affiliations as af  # noqa: E402
from cluster import available_workers, get_client  # noqa: E402

# File sent to every worker to be executed
CODICE = REPO / "Federico" / "affiliations.py"

DEFAULT_INPUT = "data_sample/silver/authors"   # senza argomenti si prova sul campione
DEFAULT_OUT = "~/mapd-out/bench_2_3_2"         # fuori dalla repo

# Benchmark campaign parameters
PARTIZIONI = (1, 2, 4, 8, 16, 32, 64, 128, None)
CURVE = ("partizioni", "worker")
COLONNE = ["curva", "valore", "ripetizione", "secondi", "errore",
           "partizioni", "file", "worker", "thread"]

# Pause check: control that port 8786 is actually free
PAUSA_FRA_CLUSTER = 10


def lavoro(files, k):
    """
    Graph of the four works (lazy)
    """
    authors = af.read_groups(af.split_evenly(files, k))
    lazy = [serie for colonna in af.AFFILIAZIONI.values()
            for serie in af.ranking(authors, colonna)]
    return lazy, authors.npartitions


def cronometra(client, lazy):
    """
    Time measurement of the four graphs simultaneous compute
    """
    inizio = time.perf_counter()
    client.compute(lazy, sync=True)
    return round(time.perf_counter() - inizio, 2)


def stato_cluster(client):
    """ 
    Utility to get the total number of workers and the toal number of threads.
    It takes into account errors (workers stopping or dying or disconnected from the cluster)
    """
    thread = client.nthreads()
    return {"worker": len(thread), "thread": sum(thread.values())}


def baseline_pandas(files):
    """
    Run the affiliation analysis with pandas on a single core as a baseline.
    """
    inizio = time.perf_counter()
    tabella = af.load_group([str(f) for f in files])
    for colonna in af.AFFILIAZIONI.values():
        valide = tabella[["cord_uid", colonna]].dropna(subset=[colonna])
        valide[colonna].value_counts()
        (valide.assign(chiave=valide[colonna].map(af.chiave))[["cord_uid", "chiave"]]
               .drop_duplicates().chiave.value_counts())
    return round(time.perf_counter() - inizio, 2)


def scrivi_riga(percorso, riga):
    """
    Add the results of a single benchmark configuration to a csv file
    """
    nuovo = not percorso.exists()
    with open(percorso, "a", newline="") as fh:
        scrittore = csv.DictWriter(fh, fieldnames=COLONNE, extrasaction="ignore", restval="")
        if nuovo:
            scrittore.writeheader()
        scrittore.writerow(riga)


def misura(client, files, k, base):
    """
    Run and time one benchmark configuration, recording its results or any error.
    """
    riga = dict(base, file=len(files), partizioni=None, secondi=None, errore="")
    riga.update(stato_cluster(client))
    try:
        lazy, riga["partizioni"] = lavoro(files, k)
        riga["secondi"] = cronometra(client, lazy)
    except Exception as errore:
        riga["errore"] = f"{type(errore).__name__}: {errore}"[:200]
    return riga


def campagna(disponibili, k_riferimento, thread, curve=None, quali_worker=None):
    """Build and filter benchmark configurations grouped by worker and thread."""

    punti = [("partizioni", disponibili, thread, k) for k in PARTIZIONI]
    punti += [("worker", w, thread, k_riferimento) for w in range(disponibili, 0, -1)]

    if curve:
        punti = [p for p in punti if p[0] in curve]
    if quali_worker:
        punti = [p for p in punti if p[1] in quali_worker]

    forme = {}
    for curva, worker, thread_, k in punti:
        forme.setdefault((worker, thread_), []).append((curva, k))
    return forme



# Command-line arguments:
#   input        Input dataset path; defaults to the sample dataset.
#   --out        Output directory for benchmark results.
#   --ripetizioni Number of complete campaign repetitions.
#   --k          Reference number of partitions for the worker curve.
#   --thread     Number of threads per worker.
#   --only       Benchmark curves to run: partizioni or worker.
#   --worker     Worker counts to include in the benchmark.
def parse_args():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("input", nargs="?", default=DEFAULT_INPUT,
                        help=f"cartella silver/authors (default {DEFAULT_INPUT}, il campione)")
    parser.add_argument("--out", default=DEFAULT_OUT, help=f"dove scrivere (default {DEFAULT_OUT})")
    parser.add_argument("--ripetizioni", type=int, default=1, metavar="N",
                        help="quante volte ripetere TUTTA la campagna (default 1). Le "
                             "ripetizioni sono passate intere e non misure consecutive "
                             "dello stesso punto: la dispersione che ne esce comprende "
                             "anche la variabilita' della macchina nel tempo")
    parser.add_argument("--k", type=int, metavar="N", default=af.DEFAULT_PARTITIONS,
                        help=f"le partizioni della curva sui worker (default "
                             f"{af.DEFAULT_PARTITIONS}, cioe' il default del task). E' il "
                             "punto di lavoro a cui si misura lo speedup: misurarlo dove "
                             "il job e' fatto solo di coordinamento da una stima per difetto")
    parser.add_argument("--thread", type=int, metavar="N",
                        help="thread per worker (default: quanti core ha il nodo). Con "
                             "--thread 1 la stessa campagna misura i PROCESSI invece dei "
                             "thread: piu' thread nello stesso processo condividono il "
                             "GIL, piu' processi no")
    parser.add_argument("--only", nargs="+", metavar="CURVA", choices=CURVE,
                        help=f"rilancia solo queste curve: {' '.join(CURVE)} "
                             "(default: tutte)")
    parser.add_argument("--worker", nargs="+", type=int, metavar="N",
                        help="misura solo questi numeri di worker, invece di tutta la "
                             "curva. Serve ai confronti a core costanti, dove i punti "
                             "intermedi distribuirebbero i processi in modo sbilanciato "
                             "fra le macchine")
    return parser.parse_args()


def main():
    args = parse_args()

    # Extracting paths from the argument parser
    source = Path(args.input).expanduser()
    source = source if source.is_absolute() else REPO / source
    out = Path(args.out).expanduser()
    out = out if out.is_absolute() else REPO / out
    out.mkdir(parents=True, exist_ok=True)
    percorso_csv = out / "misure.csv"

    # Extracting and sorting the paths of the parquet files for authors
    files = af.author_files(source)
    if not files:
        raise SystemExit(f"Nessun file .parquet in {source}")

    disponibili = available_workers(REPO)

    # Creation of the benchmark campaign
    forme = campagna(disponibili, args.k, args.thread, args.only, args.worker)
    if not forme:
        raise SystemExit("La selezione non contiene nessuna misura: controlla --only e --worker")

    #TODO: Comment this better
    def etichetta(curva, k, worker):
        """Il valore che finisce in colonna `valore`: la k per la curva sulle partizioni,
        il numero di worker per quella sui worker."""
        return (k or len(files)) if curva == "partizioni" else worker

    print(f"input   : {source}  ({len(files)} file)")
    print(f"csv     : {percorso_csv}  (in append)")
    print(f"worker  : fino a {disponibili}")
    print(f"campagna: {len(forme)} cluster x {args.ripetizioni} passate")
    for (w, t), punti in forme.items():
        print(f"  worker={w} thread={t or 'default'} -> "
              f"{', '.join(f'{c}:{k or len(files)}' for c, k in punti)}")


    # Global cronometer for the whole benchmark campaign
    inizio = time.perf_counter()

    for ripetizione in range(args.ripetizioni):
        secondi = baseline_pandas(files)
        scrivi_riga(percorso_csv, {"curva": "pandas", "valore": 1, "ripetizione": ripetizione,
                                   "secondi": secondi, "errore": "", "partizioni": 1,
                                   "file": len(files), "worker": 1, "thread": 1})
        print(f"\n[{ripetizione}] pandas, un core: {secondi} s")

        # Un cluster per FORMA (worker x thread): si accende una volta e ci si misura
        # dentro tutto quello che quella forma deve dare.
        for (worker, thread), punti in forme.items():
            client = cluster = None
            try:
                client, cluster = get_client(repo_root=REPO, n_workers=worker,
                                             n_threads=thread)
                client.upload_file(str(CODICE))

                for curva, k in punti:
                    base = {"curva": curva, "valore": etichetta(curva, k, worker),
                            "ripetizione": ripetizione}
                    # Execution of the single benchmark inside the campaign
                    riga = misura(client, files, k or len(files), base)
                    scrivi_riga(percorso_csv, riga)
                    print(f"[{ripetizione}] {curva}={riga['valore']:<5} "
                          f"worker={riga['worker']} thread={riga['thread']} "
                          f"partizioni={riga['partizioni']} -> "
                          f"{riga['secondi']} s {riga['errore']}")

            # Exception handling: in case a cluster fail to start, save the error and keep the campaign going
            except Exception as errore:
                detto = f"{type(errore).__name__}: {errore}"[:200]
                for curva, k in punti:
                    scrivi_riga(percorso_csv,
                                {"curva": curva, "valore": etichetta(curva, k, worker),
                                 "ripetizione": ripetizione, "secondi": None,
                                 "errore": detto, "file": len(files), "worker": worker})
                print(f"[{ripetizione}] worker={worker}: CLUSTER FALLITO  {detto}")
            finally:
                if client is not None:
                    client.close()
                if cluster is not None:
                    cluster.close()
                time.sleep(PAUSA_FRA_CLUSTER)   # wait for port 8786 to be free again 

    print(f"\ncampagna finita in {(time.perf_counter() - inizio) / 60:.1f} minuti")
    print("csv:", percorso_csv)


if __name__ == "__main__":
    main()
