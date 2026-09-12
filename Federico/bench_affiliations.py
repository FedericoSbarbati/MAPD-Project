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

Il punto "cluster pieno, k di riferimento" appartiene a TUTTE E DUE le curve: nei grafici
l'asse x si legge dalle colonne di stato (`partizioni`, `worker`, `thread`), mai
dall'etichetta `curva`.

PROCESSI CONTRO THREAD, senza toccare cluster.txt. `SSHCluster` accende un worker per
voce, quindi ripetere la lista degli host mette piu' processi sulla stessa macchina, e
`CORD19_HOSTS` scavalca `cluster.txt` per la durata di un comando:

    W=ip_worker1,ip_worker2,ip_worker3,ip_worker4
    CORD19_HOSTS="ip_scheduler,$W,$W" CORD19_WORKER_MEMORY_LIMIT=1.7GB \
    python Federico/bench_affiliations.py ~/mapd-data/silver/authors \
        --only worker --worker 8 --thread 1 --k 16 --ripetizioni 3

Si scrive "$W,$W" e non "w1,w1,w2,w2,...": cosi' i primi N host sono N macchine DIVERSE, e
i punti intermedi della curva restano leggibili.
`CORD19_WORKER_MEMORY_LIMIT` NON e' opzionale: il default e' una *frazione della RAM di
sistema per worker*, quindi due worker sulla stessa macchina si impegnerebbero il 170%
della sua memoria, e a fermarli sarebbe l'OOM killer del kernel - non la nanny di Dask,
che crede di avere tutta la macchina per se'.

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

CURVE = ("partizioni", "worker")

COLONNE = ["curva", "valore", "ripetizione", "secondi", "errore",
           "partizioni", "file", "worker", "thread"]

# Lo scheduler nasce sempre sulla porta 8786 (cluster.py), quindi il cluster successivo la
# trova occupata se il precedente non l'ha ancora rilasciata: "OSError: [Errno 98] Address
# already in use", e il cluster non nasce. Stessa pausa e stessa ragione di
# bench_word_count.py, dove 48 misure di fila l'hanno provata.
PAUSA_FRA_CLUSTER = 10


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
    """Lo stesso lavoro su un core solo, senza Dask: il metro di paragone.

    "Lo stesso" alla lettera, `chiave` compresa: se il baseline saltasse un pezzo, il
    rapporto Dask/pandas che finisce nel README misurerebbe due lavori diversi.
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


def campagna(disponibili, k_riferimento, thread, curve=None, quali_worker=None):
    """Le FORME DI CLUSTER da accendere, e cosa misurare dentro ciascuna.

    -> {(worker, thread): [(curva, k), ...]}

    Il raggruppamento e' il punto: un cluster si accende UNA volta e ci si misurano dentro
    tutte le sue k. La curva sulle partizioni gira sul cluster pieno; la curva sui worker
    tiene k fisso al riferimento e cambia il numero di processi.

    Il punto (cluster pieno, k di riferimento) appartiene a tutte e due le curve, ed e' il
    perno su cui si leggono insieme: nei grafici l'asse x va letto dalle colonne di stato
    (`partizioni`, `worker`, `thread`), mai dall'etichetta `curva`.
    """
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
    parser.add_argument("--k", type=int, metavar="N",
                        help="le partizioni della curva sui worker (default: una per "
                             "file). E' il punto di lavoro a cui si misura lo speedup: "
                             "misurarlo dove il job e' fatto solo di coordinamento da "
                             "una stima per difetto")
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
    forme = campagna(disponibili, args.k, args.thread, args.only, args.worker)
    if not forme:
        raise SystemExit("La selezione non contiene nessuna misura: controlla --only e --worker")

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
                    riga = misura(client, files, k or len(files), base)
                    scrivi_riga(percorso_csv, riga)
                    print(f"[{ripetizione}] {curva}={riga['valore']:<5} "
                          f"worker={riga['worker']} thread={riga['thread']} "
                          f"partizioni={riga['partizioni']} -> "
                          f"{riga['secondi']} s {riga['errore']}")

            # Un cluster che non nasce non deve portarsi via la campagna: le sue misure
            # diventano righe con l'errore, e si passa alla forma dopo.
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
                time.sleep(PAUSA_FRA_CLUSTER)   # la 8786 deve tornare libera

    print(f"\ncampagna finita in {(time.perf_counter() - inizio) / 60:.1f} minuti")
    print("csv:", percorso_csv)


if __name__ == "__main__":
    main()
