"""Benchmark del task 2.3.2. Un comando, e la campagna gira da sola.

    python Federico/bench_affiliations.py ~/mapd-data/silver/authors --ripetizioni 3

Le due curve obbligatorie: tempo vs numero di PARTIZIONI e tempo vs numero di WORKER
("at least the number of dataset partitions and the number of executors/processing
units", InstructionsAndGuidelines punto 5).

IL FATTO CHE DA' FORMA A TUTTO IL FILE: silver/authors e' 34 MB e 2,9 milioni di righe,
e lo stesso lavoro in pandas su un core dura frazioni di secondo. Qui non si misura il
calcolo, si misura il COSTO DI COORDINAMENTO. Da questo discendono due scelte:

  - UN CLUSTER PER NUMERO DI WORKER, non uno per misura come in bench_word_count.py.
    La' la regola serve perche' un worker che ha macinato milioni di stringhe trattiene
    RSS per frammentazione glibc; con 34 MB quel logoramento non puo' avvenire, mentre
    accendere un SSHCluster (~40 s) sarebbe la quasi totalita' della campagna.
  - IL BASELINE PANDAS SU UN CORE finisce nel CSV come una riga qualsiasi (curva
    "pandas"). E' il numero che trasforma "la curva e' piatta" in un rapporto.

Si cronometra il calcolo delle quattro classifiche, cioe' il lavoro distribuito.
La scrittura dei CSV e dei grafici e' pandas sul client, uguale in ogni punto: dentro il
cronometro sarebbe una costante additiva che schiaccia le curve.

Il punto "cluster pieno, una partizione per file" appartiene a TUTTE E DUE le curve:
nei grafici l'asse x si legge dalle colonne di stato (`partizioni`, `worker`), mai
dall'etichetta `curva`.

Prova generale sul campione, prima di occupare il cluster:

    python Federico/bench_affiliations.py --out /tmp/bench-2_3_2
"""

import argparse
import csv
import sys
import time
from pathlib import Path

# I worker devono poter importare il modulo del task: la radice della repo nel sys.path
REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(REPO / "Federico"))

import affiliations as af  # noqa: E402
from cluster import available_workers, get_client  # noqa: E402

# Il file spedito ai worker: i dati sono replicati su ogni macchina, il codice no
CODICE = REPO / "Federico" / "affiliations.py"

DEFAULT_INPUT = "data_sample/silver/authors"   # senza argomenti si prova sul campione
DEFAULT_OUT = "~/mapd-out/bench_2_3_2"         # fuori dalla repo

# I 192 file di silver/authors raggruppati in k partizioni. `None` = una per file, che e'
# il default del task e il punto di riferimento comune alle due curve.
PARTIZIONI = (1, 2, 4, 8, 16, 32, 64, 128, None)

COLONNE = ["curva", "valore", "ripetizione", "secondi", "errore",
           "partizioni", "file", "worker", "thread"]


def lavoro(files, k):
    """Il grafo delle quattro classifiche, identico a quello che il task consegna. Lazy."""
    authors = af.read_groups(af.split_evenly(files, k))
    lazy = [serie for colonna in af.AFFILIAZIONI.values()
            for serie in af.ranking(authors, colonna)]
    return lazy, authors.npartitions


def cronometra(client, lazy):
    """Quanto ci mette il cluster a produrre le quattro classifiche."""
    inizio = time.perf_counter()
    client.compute(lazy, sync=True)
    return round(time.perf_counter() - inizio, 2)


def stato_cluster(client):
    """Worker e thread VERI. `nthreads()` e non `scheduler_info()`, che sotto-conta
    quando piu' worker stanno sulla stessa macchina."""
    thread = client.nthreads()
    return {"worker": len(thread), "thread": sum(thread.values())}


def baseline_pandas(files):
    """Lo stesso lavoro su un core solo, senza Dask: il metro di paragone."""
    inizio = time.perf_counter()
    tabella = af.load_group([str(f) for f in files])
    for colonna in af.AFFILIAZIONI.values():
        valide = tabella[["cord_uid", colonna]].dropna(subset=[colonna])
        valide[colonna].value_counts()
        valide.drop_duplicates()[colonna].value_counts()
    return round(time.perf_counter() - inizio, 2)


def scrivi_riga(percorso, riga):
    """Una riga di CSV per misura, in append: una campagna interrotta lascia i suoi dati."""
    nuovo = not percorso.exists()
    with open(percorso, "a", newline="") as fh:
        scrittore = csv.DictWriter(fh, fieldnames=COLONNE, extrasaction="ignore", restval="")
        if nuovo:
            scrittore.writeheader()
        scrittore.writerow(riga)


def misura(client, files, k, base):
    """Una misura: il grafo, il cronometro, e gli errori come stringa invece che come
    interruzione della campagna."""
    riga = dict(base, file=len(files), partizioni=None, secondi=None, errore="")
    riga.update(stato_cluster(client))
    try:
        lazy, riga["partizioni"] = lavoro(files, k)
        riga["secondi"] = cronometra(client, lazy)
    except Exception as errore:
        riga["errore"] = f"{type(errore).__name__}: {errore}"[:200]
    return riga


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
    return parser.parse_args()


def main():
    args = parse_args()

    source = Path(args.input).expanduser()
    source = source if source.is_absolute() else REPO / source
    out = Path(args.out).expanduser()
    out = out if out.is_absolute() else REPO / out
    out.mkdir(parents=True, exist_ok=True)
    percorso_csv = out / "misure.csv"

    files = af.author_files(source)
    if not files:
        raise SystemExit(f"Nessun file .parquet in {source}")

    disponibili = available_workers(REPO)
    print(f"input   : {source}  ({len(files)} file)")
    print(f"csv     : {percorso_csv}  (in append)")
    print(f"worker  : fino a {disponibili}")
    print(f"campagna: {len(PARTIZIONI)} partizioni + {disponibili - 1} worker, "
          f"x{args.ripetizioni} passate")

    inizio = time.perf_counter()

    for ripetizione in range(args.ripetizioni):
        secondi = baseline_pandas(files)
        scrivi_riga(percorso_csv, {"curva": "pandas", "valore": 1, "ripetizione": ripetizione,
                                   "secondi": secondi, "errore": "", "partizioni": 1,
                                   "file": len(files), "worker": 1, "thread": 1})
        print(f"\n[{ripetizione}] pandas, un core: {secondi} s")

        # Un cluster per numero di worker. Sul cluster pieno gira anche la curva sulle
        # partizioni: il suo punto k=None e' il riferimento comune alle due curve.
        for worker in range(disponibili, 0, -1):
            client = cluster = None
            try:
                client, cluster = get_client(repo_root=REPO, n_workers=worker)
                client.upload_file(str(CODICE))

                if worker == disponibili:
                    punti = [("partizioni", k) for k in PARTIZIONI]
                else:
                    punti = [("worker", worker)]

                for curva, valore in punti:
                    k = valore if curva == "partizioni" else None
                    base = {"curva": curva, "valore": valore or len(files),
                            "ripetizione": ripetizione}
                    riga = misura(client, files, k or len(files), base)
                    scrivi_riga(percorso_csv, riga)
                    print(f"[{ripetizione}] {curva}={riga['valore']:<5} "
                          f"worker={riga['worker']} thread={riga['thread']} "
                          f"partizioni={riga['partizioni']} -> "
                          f"{riga['secondi']} s {riga['errore']}")
            finally:
                if client is not None:
                    client.close()
                if cluster is not None:
                    cluster.close()

    print(f"\ncampagna finita in {(time.perf_counter() - inizio) / 60:.1f} minuti")
    print("csv:", percorso_csv)


if __name__ == "__main__":
    main()
