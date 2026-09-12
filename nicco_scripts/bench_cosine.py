"""Benchmark del task 2.3.4. Un comando, e la campagna gira da sola.

    python nicco_scripts/bench_cosine.py nicco_scripts/embeddings --ripetizioni 3

Le due curve obbligatorie - tempo vs numero di PARTIZIONI e tempo vs numero di WORKER
("at least the number of dataset partitions and the number of executors/processing units",
InstructionsAndGuidelines punto 5) - piu' una terza che qui NON e' un contorno.

L'IPOTESI, SCRITTA PRIMA DI MISURARE. Nel 2.3.1 e nel 2.3.2 il lavoro e' codice Python, il
GIL lo mette in fila, e i processi vincono: a parita' di core 8x1 batte 4x2 di 1,33x e il
secondo thread rende 1,01x, cioe' niente. QUI il lavoro non e' Python: e' una
moltiplicazione di matrici dentro BLAS, che e' C e RILASCIA il GIL. Quindi i thread
dovrebbero lavorare davvero in parallelo, e avere un vantaggio in piu' sui processi -
condividono la memoria, quindi i vettori esistono in una copia sola invece che in una per
processo. Se la misura lo conferma, il progetto ha tre task sullo stesso cluster di cui due
GIL-bound e uno no. Se la smentisce, e' comunque un risultato: ma va misurata.

PERCHE' BLAS VA MESSO A UN THREAD. BLAS si auto-parallelizza e di default prende tutti i
core: senza `cosine.single_thread_blas()` quattro worker che moltiplicano insieme sarebbero
sedici thread su quattro core, e questa campagna misurerebbe quel caos invece di Dask. Il
modulo se ne occupa da solo, all'import e prima di accendere il cluster.

SI CRONOMETRA il `scatter` dei blocchi piu' tutte le piastrelle piu' il reduce. Lo scatter
ci sta dentro apposta: e' lavoro distribuito e il suo costo cresce col numero di worker,
che e' esattamente cio' che la curva sui worker deve mostrare. Restano fuori lettura,
campionamento e normalizzazione - identici in ogni punto, quindi una costante additiva che
schiaccerebbe le curve - e la scrittura dei CSV, come nel 2.3.2.

IL BASELINE NUMPY SU UN CORE finisce nel CSV come una riga qualsiasi (curva "numpy"). Fa
lo STESSO lavoro, istogramma compreso: se saltasse un pezzo, il rapporto Dask/NumPy del
README confronterebbe due lavori diversi.

PROCESSI CONTRO THREAD, senza toccare cluster.txt. `SSHCluster` accende un worker per voce,
quindi ripetere la lista degli host mette piu' processi sulla stessa macchina, e
`CORD19_HOSTS` scavalca `cluster.txt` per la durata di un comando:

    W=ip_worker1,ip_worker2,ip_worker3,ip_worker4
    CORD19_HOSTS="ip_scheduler,$W,$W" CORD19_WORKER_MEMORY_LIMIT=3.5GB \
        python nicco_scripts/bench_cosine.py ~/mapd-data/embeddings \
            --only worker --worker 8 --thread 1 --k 32 --ripetizioni 3

`CORD19_WORKER_MEMORY_LIMIT` NON e' opzionale: il default e' una frazione della RAM di
SISTEMA per worker, quindi due worker sulla stessa macchina si impegnerebbero il 170% della
sua memoria, e a fermarli sarebbe l'OOM killer del kernel. E qui il tetto conta davvero:
una piastrella costa ~12-14 (titoli/k)^2 byte, per il numero di thread del worker.

Prova generale prima di occupare il cluster:

    python nicco_scripts/bench_cosine.py --titoli 5000 --out /tmp/bench-2_3_4
"""

import argparse
import csv
import sys
import time
from pathlib import Path

# I worker devono poter importare il modulo del task: la radice della repo nel sys.path
REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(REPO / "nicco_scripts"))

import cosine as cs  # noqa: E402
from cluster import available_workers, get_client  # noqa: E402

# Il file spedito ai worker: i dati si scatterano, il codice no
CODICE = REPO / "nicco_scripts" / "cosine.py"

DEFAULT_INPUT = cs.DEFAULT_INPUT
DEFAULT_OUT = "~/mapd-out/bench_2_3_4"

# Lo sweep sulle partizioni, ORDINATO DAL RIFERIMENTO VERSO I BORDI e non per valore
# crescente: dentro una forma di cluster le misure si susseguono, e un k che sfonda la
# memoria fa intervenire la nanny sul worker. Se quel k viene per primo, le misure dopo di
# lui girano su un cluster appena riavviato. I fragili in fondo, quindi, dove al massimo si
# portano via se stessi - stessa regola dell'ordine di campagna del 2.3.1.
# NIENTE 1 e 2: il picco di una piastrella va come (titoli/k)^2, quindi a 100.000 titoli
# k=2 chiederebbe ~30 GB per task. I k bassi non sono lenti, sono IRREALIZZABILI.
# Il 128 c'e' perche' su worker da 7,1 GB con 4 thread il 4 e l'8 sfondano: senza,
# la curva avrebbe due punti validi su quattro. A destra il muro non esiste (il picco
# va come 1/k^2) e si misura l'altro estremo, i task troppo piccoli.
#
# k=8 E k=4 SONO USCITI DALLO SWEEP, e sono comunque nel CSV: li ha misurati la
# calibrazione sul cluster del 2026-09-12, a 100.000 titoli su 4 worker x 4 thread.
# `k=8` -> KilledWorker su tutti e quattro i worker; `k=4` -> FutureCancelledError, e la
# riga registra `worker=3`, cioe' il cluster NON si era ripreso dal punto precedente. Il
# muro e' un fatto binario, non una misura con dispersione: ripeterlo a ogni passata
# costerebbe minuti e lascerebbe un cluster degradato ai punti dopo di lui. Per rimetterlo
# (altri titoli -> altro muro) basta aggiungerlo qui in fondo, dov'era.
BLOCCHI = (16, 32, 64, 128)

# I punti della curva sui thread. Espliciti e non "quanti core ha il nodo", perche' qui il
# numero di thread E' la variabile misurata.
THREAD = (1, 2, 4)

CURVE = ("partizioni", "worker", "thread")

COLONNE = ["curva", "valore", "ripetizione", "secondi", "errore",
           "titoli", "partizioni", "worker", "thread"]

# Lo scheduler nasce sempre sulla porta 8786 (cluster.py), quindi il cluster successivo la
# trova occupata se il precedente non l'ha ancora rilasciata: "OSError: [Errno 98] Address
# already in use". Non si libera da sola - provato sul cluster il 2026-09-10.
PAUSA_FRA_CLUSTER = 10


def cronometra(client, X, k, top, bins):
    """Quanto ci mette il cluster a produrre le due classifiche e l'istogramma.

    `build` sta DENTRO il cronometro, al contrario del 2.3.2 dove il grafo si costruiva
    fuori: qui costruirlo significa spedire i blocchi ai worker, che e' lavoro vero.
    """
    inizio = time.perf_counter()
    client.compute(cs.build(client, X, k, top, bins), sync=True)
    return round(time.perf_counter() - inizio, 2)


def stato_cluster(client):
    """Worker e thread VERI. `nthreads()` e non `scheduler_info()`, che sotto-conta quando
    piu' worker stanno sulla stessa macchina."""
    thread = client.nthreads()
    return {"worker": len(thread), "thread": sum(thread.values())}


def baseline_numpy(X, k, top, bins):
    """Lo stesso lavoro su un core solo, senza Dask: il metro di paragone.

    Stesse piastrelle, stesso k, stessa riduzione, stesso istogramma - solo in fila invece
    che in parallelo. Cambiare k cambierebbe il lavoro, non solo la sua distribuzione.
    """
    tagli = cs.split_rows(len(X), k)
    inizio = time.perf_counter()
    pezzi = [cs.top_block(X[tagli[i][0]:tagli[i][1]], X[tagli[j][0]:tagli[j][1]],
                          tagli[i][0], tagli[j][0], top, bins)
             for i, j in cs.block_pairs(len(tagli))]
    cs.merge(pezzi, top)
    return round(time.perf_counter() - inizio, 2)


def scrivi_riga(percorso, riga):
    """Una riga di CSV per misura, in append: una campagna interrotta lascia i suoi dati."""
    nuovo = not percorso.exists()
    with open(percorso, "a", newline="") as fh:
        scrittore = csv.DictWriter(fh, fieldnames=COLONNE, extrasaction="ignore", restval="")
        if nuovo:
            scrittore.writeheader()
        scrittore.writerow(riga)


def misura(client, X, k, top, base):
    """Una misura: il cronometro, e gli errori come stringa invece che come interruzione
    della campagna. Un k che sfonda la memoria e' un dato, non un incidente."""
    riga = dict(base, titoli=len(X), partizioni=k, secondi=None, errore="")
    riga.update(stato_cluster(client))
    try:
        riga["secondi"] = cronometra(client, X, k, top, cs.BINS)
    except Exception as errore:
        riga["errore"] = f"{type(errore).__name__}: {errore}"[:200]
    return riga


def campagna(disponibili, k_riferimento, thread, curve=None, quali_worker=None):
    """Le FORME DI CLUSTER da accendere, e cosa misurare dentro ciascuna.

    -> {(worker, thread): [(curva, k), ...]}

    Un cluster si accende UNA volta e ci si misurano dentro tutte le sue k. La curva sulle
    partizioni gira sul cluster pieno; quella sui worker tiene k fisso al riferimento e
    cambia il numero di processi; quella sui thread tiene worker e k fissi e cambia i
    thread di ogni processo.

    Il punto (cluster pieno, k di riferimento) appartiene a tutte e tre, ed e' il perno su
    cui si leggono insieme: nei grafici l'asse x va letto dalle colonne di stato
    (`partizioni`, `worker`, `thread`), mai dall'etichetta `curva`.
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
                        help=f"cartella degli embedding (default {DEFAULT_INPUT})")
    parser.add_argument("--out", default=DEFAULT_OUT, help=f"dove scrivere (default {DEFAULT_OUT})")
    parser.add_argument("--titoli", type=int, default=cs.DEFAULT_TITLES, metavar="N",
                        help=f"quanti titoli (default {cs.DEFAULT_TITLES}). RESTA FISSO per "
                             "tutta la campagna: e' la scala del problema, non una curva. Il "
                             "costo va come il suo QUADRATO")
    parser.add_argument("--ripetizioni", type=int, default=1, metavar="N",
                        help="quante volte ripetere TUTTA la campagna (default 1). Le "
                             "ripetizioni sono passate intere e non misure consecutive dello "
                             "stesso punto: la dispersione che ne esce comprende anche la "
                             "variabilita' della macchina nel tempo")
    parser.add_argument("--k", type=int, metavar="N", default=cs.DEFAULT_BLOCKS,
                        help=f"i blocchi delle curve su worker e thread (default "
                             f"{cs.DEFAULT_BLOCKS}, cioe' il default del task). E' il punto di "
                             "lavoro a cui si misura lo speedup")
    parser.add_argument("--thread", type=int, metavar="N",
                        help="thread per worker nelle curve su partizioni e worker "
                             "(default: quanti core ha il nodo). La curva sui thread usa i "
                             f"suoi valori: {THREAD}")
    parser.add_argument("--top", type=int, default=cs.TOP_N,
                        help=f"quante coppie per classifica (default {cs.TOP_N})")
    parser.add_argument("--seed", type=int, default=cs.SEED,
                        help=f"il seed del campione (default {cs.SEED}): tutte le misure "
                             "devono guardare gli stessi titoli")
    parser.add_argument("--only", nargs="+", metavar="CURVA", choices=CURVE,
                        help=f"rilancia solo queste curve: {' '.join(CURVE)} (default: tutte)")
    parser.add_argument("--worker", nargs="+", type=int, metavar="N",
                        help="misura solo questi numeri di worker, invece di tutta la curva. "
                             "Serve ai confronti a core costanti")
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
        raise SystemExit("La selezione non contiene nessuna misura: controlla --only e --worker")

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

        # Un cluster per FORMA (worker x thread): si accende una volta e ci si misura dentro
        # tutto quello che quella forma deve dare.
        for (worker, thread), punti in forme.items():
            client = cluster = None
            try:
                cs.single_thread_blas()         # prima che nasca qualunque worker
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

            # Un cluster che non nasce non deve portarsi via la campagna: le sue misure
            # diventano righe con l'errore, e si passa alla forma dopo.
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
                time.sleep(PAUSA_FRA_CLUSTER)   # la 8786 deve tornare libera

    print(f"\ncampagna finita in {(time.perf_counter() - inizio) / 60:.1f} minuti")
    print("csv:", percorso_csv)


if __name__ == "__main__":
    main()
