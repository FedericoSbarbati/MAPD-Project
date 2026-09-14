"""Benchmark of task 2.3.1: runtime vs partitions, vs workers, vs threads per worker.

One row of the CSV = one measurement = a brand new cluster, so that every point starts
from freshly born workers instead of workers worn out by the previous measurement.

    python Giulia/bench_word_count.py --out /tmp/bench-prova --timeout 300   # rehearsal
    python Giulia/bench_word_count.py ~/mapd-data/silver/paragraphs --thread 1 --ripetizioni 3
"""

import argparse
import csv
import os
import sys
import time
from pathlib import Path

# Adding the repo root to syspath in order to make code executable on workers
REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(REPO / "Giulia"))

from distributed import performance_report, wait  # noqa: E402

import word_count as wc  # noqa: E402
from cluster import available_workers, get_client  # noqa: E402

# File sent to every worker to be executed
CODICE = REPO / "Giulia" / "word_count.py"

DEFAULT_INPUT = "data_sample/silver/paragraphs"   # no arguments = try on a sample
DEFAULT_OUT = "~/mapd-out/bench"                  # outside of the repo: code usa e getta

# Benchmark campaign parameters

K_RIFERIMENTO = 256
PARTIZIONI = (512, 128, 1024, 64, 1979, 32, 16, 8, 4)
THREAD = (4, 2, 1)

# Pause check: control that port 8786 is actually free
PAUSA_FRA_CLUSTER = 10

# Columns for benchmark execution
COLONNE = ["curva", "valore", "ripetizione", "secondi", "errore",
           "partizioni", "split_out", "file", "worker", "thread",
           "picco_gb", "picco_medio_gb"]

def campagna(disponibili, thread=None, ripetizioni=1):
    """
    Creates a list of configuration for the benchmark.
    - disponibili: max number of avaliable workers
    - thread: number of threads per worker (none -> cluster will choose)
    - ripetizioni: how many time to repeat the whole campaign

    """
    def punto(curva, valore, **cambiato):
        '''
        Creates a dictionary with default value and changes with the one updated at the moment:
        - curva: name of the curve (riferimento, partizioni, worker, thread, foldby)
        - valore: value associated to the curve
        - ** cambiato: dictionary unpacking (adds the value to the returned dictionary)
        
        PS: For a dictionary the same kay cannot hold two values so **cambiato will overwrite the default 
            value into the configuration dictionary.
        '''
        return {"curva": curva, "valore": valore,
                "worker": disponibili, "thread": thread,
                "k": K_RIFERIMENTO, "split_out": wc.SPLIT_OUT,
                **cambiato}

    '''
    Explicit example of punti as a list of dictionaries:
        punti = [
            punto_riferimento,

            punto_partizioni_1, (It's *[punto("partizioni", k, k=k) )
            punto_partizioni_2,
            punto_partizioni_3,

            punto_worker_1, (The same thing: *[punto("worker",     w, worker=w))
            punto_worker_2,

            punto_thread_1, (*[punto("thread",     t, thread=t))
            punto_thread_2,

            punto_foldby,
        ]
    '''
    punti = [
        punto("riferimento", disponibili), # Creates a standard configuration dictionary
        *[punto("partizioni", k, k=k)      for k in PARTIZIONI], # List of configuration with a sweep over k values
        *[punto("worker",     w, worker=w) for w in range(disponibili - 1, 0, -1)], # List of configuration for workers
        *[punto("thread",     t, thread=t) for t in THREAD if t != thread],  # List of configuration for threads
        punto("foldby", 0, split_out=0), # Attempt with foldby instead of groupby with spli-out
    ]

    # Save every configuration with repetitions for multiple measurements
    return [dict(p, ripetizione=r) for r in range(ripetizioni) for p in punti]


# ----------------------------------------------------------------------------------
# Cronometro
# ----------------------------------------------------------------------------------


def lavoro(files, k, split_out, destinazione):
    """
    Creates the computational graph of the work as in word_count.py
    (Lazy, not executed).
    The logical flow is:

    input Parquet
    -> Bag of (cord_uid, testo)
    -> Map
    -> Reduce
    -> global_counts
    -> DataFrame
    -> write to Parquet
    """

    # Creates the Dask lazy collection (cord_uid,text)
    bag = wc.read_groups(wc.split_evenly(files, k))

    # Global counts = (word, count), Dask lazy collection
    _, global_counts = wc.word_count(bag, split_out=split_out)

    # Adding to the graph the task to convert global_counts from a Dask lazy collection to Dask Dataframe
    frame = global_counts.to_dataframe(meta={"word": "string", "count": "int64"})
    # Adding to the graph the task to write the results in Parquet (COMPUTE is FALSE)
    scrittura = frame.to_parquet(destinazione, write_index=False, compression="zstd",
                                 overwrite=True, compute=False)
    return [scrittura], bag.npartitions


def cronometra(client, collezioni, timeout):
    """
    Measure the time to compute a single configuration in the benchmark campaign
    """
    inizio = time.perf_counter()

    # We use a future cause it's easier to block with a timeout (loops happened)
    # Contains: Name of the task, State of the task (pending/finished/error)
    futures = client.compute(collezioni)
    wait(futures, timeout=timeout)

    # Loop that's executed just in case of failures (otherwise futures has len == 1 and future.result() = None)
    for future in futures:
        future.result()  # Store the result of the future into the variable (if it fails, it raises an exception)

    return round(time.perf_counter() - inizio, 1)


def picco_memoria(client):
    """
    Utility to measure the maximum amount of Ram used by every worker and it's mean value
    """
    def rss_massima():
        import resource
        import sys as _sys

        # For every worker get the maximum Resident Set Size (RSS, Ram used by the process)
        massimo = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss

        # Conversion to gygabite (ru_maxrss: Macos - byte, Linux - kilobyte)
        return massimo / 1e9 if _sys.platform == "darwin" else massimo / 1e6

    # Get maximum RSS from every worker in the cluster
    picchi = list(client.run(rss_massima).values())
    if not picchi:
        return {}
    return {"picco_gb": round(max(picchi), 2),
            "picco_medio_gb": round(sum(picchi) / len(picchi), 2)}


def stato_cluster(client):
    """
    Utility to get the total number of workers and the toal number of threads.
    It takes into account errors (workers stopping or dying or disconnected from the cluster)
    """
    thread = client.nthreads()
    return {"worker": len(thread), "thread": sum(thread.values())}


def misura(p, files, args):
    """
    Start a cluster and run benchmark for a single configuration.
    """

    client = cluster = None

    # Create a dictionary with the configuration parameters in p and the fields for the results
    riga = dict(p, file=len(files), partizioni=None, secondi=None, errore="")
    # Paths for the html report and the Parquet result of word counts
    percorso = args.out / f"report_{p['curva']}_{p['valore']}_{p['ripetizione']}.html"
    vocabolario = args.out / "vocabolario"


    try:

        # Create or connect to the cluster with the parameters of the configuration p
        client, cluster = get_client(repo_root=REPO,
                                     n_workers=p["worker"], n_threads=p["thread"])

        # Get the actual configuration of the cluster
        riga.update(stato_cluster(client))  

        # Copying word_count code into the worker (workers have the data but not the code) 
        # Solved: "Error during deserialization of the task graph ... different environments"
        # (The code is not into the snapshot used to start the VM and it's usually updated, surely there is a smarter way to solve the problem but this works...)
        client.upload_file(str(CODICE))

        # Creating the output directory into every worker (the results are stored into their local disk, not into the shared filesystem)
        client.run(os.makedirs, str(vocabolario), exist_ok=True)

        # Creation of the lazy computational graph
        collezioni, riga["partizioni"] = lavoro(files, p["k"], p["split_out"], vocabolario)

        # Execution of the graph inside the cluster and measuring performance
        with performance_report(filename=str(percorso), mode="inline"):
            riga["secondi"] = cronometra(client, collezioni, args.timeout)

        # Check if a workers died during the process and updates the configuration in case
        riga.update(stato_cluster(client))  
        riga.update(picco_memoria(client))  # Resources checkl

    # Error handling: Convert every error to string and keep the campaign going 
    except Exception as errore:
        riga["errore"] = f"{type(errore).__name__}: {errore}"[:200]
    finally:
        # Shut down the cluster and the client connection. Wait ten second for a fresh restart
        if client is not None:
            client.close()
        if cluster is not None:
            cluster.close()
        time.sleep(PAUSA_FRA_CLUSTER)
    return riga


def scrivi_riga(percorso, riga):
    """
    Add the results of a single benchmark configuration to a csv file
    """

    nuovo = not percorso.exists()
    with open(percorso, "a", newline="") as fh:
        scrittore = csv.DictWriter(fh, fieldnames=COLONNE, extrasaction="ignore", restval="") # Ignore missing key
        if nuovo:
            scrittore.writeheader()
        scrittore.writerow(riga)


# ----------------------------------------------------------------------------------

# Command-line arguments:
#   input       Optional input dataset path; defaults to the sample dataset.
#   --out       Output directory for CSV results, reports, and Parquet files.
#   --timeout   Maximum execution time in seconds for each benchmark point.
#   --only      Run only the selected benchmark curves.
#   --ripetizioni
#               Number of complete campaign repetitions.
#   --thread    Number of threads assigned to each worker.

def parse_args():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("input", nargs="?", default=DEFAULT_INPUT,
                        help=f"silver/paragraphs (default {DEFAULT_INPUT}, il campione)")
    parser.add_argument("--out", default=DEFAULT_OUT, help=f"dove scrivere (default {DEFAULT_OUT})")
    parser.add_argument("--timeout", type=int, default=3600,
                        help="tetto in secondi per una singola misura (default 3600)")
    parser.add_argument("--only", nargs="+", metavar="CURVA",
                        help="rilancia solo queste curve: riferimento worker partizioni "
                             "thread foldby (default: tutte, nell'ordine della campagna)")
    parser.add_argument("--ripetizioni", type=int, default=1, metavar="N",
                        help="quante volte ripetere TUTTA la campagna (default 1). Le "
                             "ripetizioni sono passate intere e non misure consecutive: "
                             "vedi `campagna`. Con N=3 la dispersione che ne esce e' una "
                             "barra d'errore vera, non due run attaccati")
    parser.add_argument("--thread", type=int, metavar="N",
                        help="thread per worker per TUTTA la campagna (default: quanti "
                             "core ha il nodo). Serve a rifare la curva sulle partizioni "
                             "con meno thread: un thread in meno e' un quarto di memoria "
                             "per worker, quindi il muro dei KilledWorker si sposta. "
                             "La curva `thread` ignora questa opzione, che e' la sua "
                             "variabile")
    return parser.parse_args()


def main():
    args = parse_args()

    # Extracting paths from the argument parser
    source = Path(args.input).expanduser()
    source = source if source.is_absolute() else REPO / source
    args.out = Path(args.out).expanduser()
    args.out = args.out if args.out.is_absolute() else REPO / args.out
    args.out.mkdir(parents=True, exist_ok=True)
    args.csv = args.out / "misure.csv"

    # Extracting and sorting the paths of the parquet files for paragraphs
    files = wc.paragraph_files(source)
    if not files:
        raise SystemExit(f"Nessun file .parquet in {source}")

    # Creation of the benchmark campaign
    lista = campagna(available_workers(REPO), thread=args.thread,
                     ripetizioni=args.ripetizioni)

    # Filter for testing selected curves
    if args.only:
        lista = [p for p in lista if p["curva"] in args.only]
    if not lista:
        raise SystemExit(f"--only {args.only} non seleziona nessuna misura")

    print(f"input   : {source}  ({len(files)} file)")
    print(f"output  : {args.out}")
    print(f"csv     : {args.csv}  (in append)")
    print(f"tetto   : {args.timeout} s per misura")


    print(f"\n{len(lista)} measurement,  {len(lista)} cluster, in this order:")
    for n, p in enumerate(lista, 1):
        print(f"  {n:>2}. {p['curva']:<11} valore={p['valore']:<5} worker={p['worker']} "
              f"thread={p['thread'] or 'default'} k={p['k']} split_out={p['split_out']} "
              f"rip={p['ripetizione']}")

    # Global cronometer for the whole benchmark campaign
    inizio = time.perf_counter()

    for n, p in enumerate(lista, 1):
        print(f"\n[{n}/{len(lista)}] {p['curva']}={p['valore']} rip={p['ripetizione']}")

        # Execution of the single benchmark inside the campaign
        riga = misura(p, files, args)
        scrivi_riga(args.csv, riga)
        print("   ->", f"{riga['secondi']} s  ({riga['partizioni']} partizioni, "
                       f"{riga['worker']} worker, {riga['thread']} thread in tutto, "
                       f"picco {riga.get('picco_gb', '?')} GB per worker)"
              if riga["secondi"] is not None else f"FALLITO  {riga['errore']}")

    print(f"\ncampagna finita in {(time.perf_counter() - inizio) / 60:.0f} minuti")
    print("csv:", args.csv)


if __name__ == "__main__":
    main()
