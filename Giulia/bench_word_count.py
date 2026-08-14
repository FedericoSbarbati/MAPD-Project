"""Benchmark del task 2.3.1. Un comando, e la campagna gira da sola tutta la notte.

    python Giulia/bench_word_count.py ~/mapd-data/silver/paragraphs

Il corso considera INCOMPLETA un'analisi senza benchmark, e chiede di studiare come il
tempo di esecuzione dipende dai parametri del cluster: "at least the number of dataset
partitions and the number of executors/processing units" (InstructionsAndGuidelines,
punto 5). Quel "processing units" si legge in due modi - le macchine e i thread - e qui
si misurano tutti e due.

LA REGOLA CHE REGGE TUTTO IL FILE: una riga del CSV = una misura = un cluster nuovo.
Costa un minuto a punto, e in cambio:
  - sparisce il concetto di "blocco", con tutta la contabilita' di quale cluster e'
    acceso e in che ordine si puo' scalare;
  - non serve cancellare niente dopo un fallimento: subito dopo il cluster viene
    distrutto comunque;
  - ogni punto parte da worker APPENA NATI. Non e' pignoleria: su questo cluster un
    worker che ha gia' macinato milioni di stringhe trattiene RSS per frammentazione
    glibc (PROJECT_CONTEXT.md §7, Atto 3). Riusando un cluster per nove punti, l'ultimo
    girerebbe su worker stanchi e la misura sommerebbe partizionamento e usura.

Il lavoro cronometrato e' ESATTAMENTE quello che `word_count.py` consegna, scrittura del
vocabolario compresa. Non un sottoinsieme piu' comodo da misurare: il crash dell'11
agosto stava proprio nella scrittura, cioe' nel pezzo che il vecchio benchmark saltava.

**Prova generale prima di lasciarla andare** - serve a scoprire un percorso sbagliato la
sera invece che alle sette del mattino. Stessa identica campagna, sul campione:

    python Giulia/bench_word_count.py --out /tmp/bench-prova --timeout 300

**Calibrazione sul cluster, prima della notte.** Tre misure dello stesso punto: danno la
dispersione (il rumore) e il numero da cui ricalcolare il budget di tutto il resto.

    python Giulia/bench_word_count.py ~/mapd-data/silver/paragraphs --only riferimento

**E poi, sul serio**, dentro `tmux` e con tutto l'output su un file:

    tmux new -s bench
    python Giulia/bench_word_count.py ~/mapd-data/silver/paragraphs 2>&1 | tee ~/bench.log
"""

import argparse
import csv
import os
import sys
import time
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(REPO / "Giulia"))

from distributed import performance_report, wait  # noqa: E402

import word_count as wc  # noqa: E402
from cluster import available_workers, get_client, serializza_per_valore  # noqa: E402

# OBBLIGATORIO, e non e' un dettaglio: qui `word_count` e' IMPORTATO, non definito. Senza
# questa riga cloudpickle mette nel grafo il *nome* delle sue funzioni invece del codice,
# e su `SSHCluster` lo scheduler - che nasce via SSH dalla home, senza il repo nel
# sys.path - non riesce a riaprire il grafo. Muore con "Error during deserialization of
# the task graph ... different environments", che manda a cercare nel posto sbagliato.
# `python Giulia/word_count.py` non ha il problema perche' li' le funzioni stanno in
# `__main__`, che cloudpickle spedisce gia' per valore. -> cluster.serializza_per_valore
serializza_per_valore(wc)

DEFAULT_INPUT = "data_sample/silver/paragraphs"   # senza argomenti si prova sul campione
DEFAULT_OUT = "~/mapd-out/bench"                  # fuori dalla repo: sul cluster e' usa-e-getta

# Il partizionamento del PUNTO DI RIFERIMENTO, che e' anche quello a cui girano la curva
# sui worker e la misura sui thread. Non e' un numero a caso: sul cluster deciso (4 worker
# da 4 thread) ci sono 16 slot di calcolo, e `split_out=16` mette uno shard del Reduce per
# slot. A dati fissi k=256 vuol dire ~19 MB di testo per partizione: abbastanza fine da
# lasciare margine al load balancing, abbastanza grosso da non annegare nello scheduling.
K_RIFERIMENTO = 256

# I punti della curva sulle partizioni, DAL CENTRO VERSO I BORDI (256 e' il riferimento,
# gia' misurato tre volte). L'ordine e' una scelta di progetto: i valori bassi sono i piu'
# lenti e i piu' fragili - k=4 sul corpus intero sono ~2,4 GB di testo in un task solo -
# quindi stanno in fondo. Se la notte si interrompe, quello che resta in mano e' il minimo
# della curva con i due rami vicini, cioe' la parte che risponde alla domanda.
PARTIZIONI = (512, 128, 1024, 64, 1979, 32, 16, 8, 4)

# La misura sui thread: stesso cluster, stesso taglio, cambia solo quanti thread ha ogni
# worker. Il 4 non c'e' perche' e' gia' il riferimento.
#
# IPOTESI, scritta prima di misurare: la fase Map e' `re.findall` + `Counter` in Python
# puro, che TIENE il GIL (solo pyarrow lo rilascia, e sta nella lettura). Previsione:
# T(4 thread)/T(1 thread) ~ 0,6, non 0,25. Se si conferma, l'unita' di calcolo utile su
# questo carico e' il processo e non il thread; se viene smentita, il tempo e' dominato
# da I/O e decompressione Arrow. In entrambi i casi lo si scopre con due run.
THREAD = (2, 1)

# Fra la chiusura di un cluster e l'apertura del successivo: la porta 8786 ha bisogno di
# un attimo per tornare libera. Mai verificato su SSH - la prima campagna lo dira'.
PAUSA_FRA_CLUSTER = 10

COLONNE = ["curva", "valore", "ripetizione", "secondi", "errore",
           "partizioni", "split_out", "file", "worker", "thread"]


# ----------------------------------------------------------------------------------
# La campagna: una tabella, non del codice
# ----------------------------------------------------------------------------------


def campagna(disponibili):
    """La notte intera, in una lista. Si legge dall'alto in basso ed e' l'ordine di esecuzione.

    Ogni punto e' IL RIFERIMENTO CON UNA MANOPOLA CAMBIATA - che e' il metodo
    sperimentale, ed e' il motivo per cui tutta la campagna sta in cinque righe.

    L'ordine dei blocchi non e' casuale:
      1. `riferimento` per primo: e' il punto in comune ai tre assi (il k=256 della curva
         partizioni, il "tutti i worker" della curva worker, il thread di default della
         misura sui thread) e calibra la stima dei tempi di tutto il resto;
      2. `worker` prima di `partizioni`: costo prevedibile, nessun punto che possa fallire;
      3. `partizioni` dal centro ai bordi, per il motivo scritto sopra la costante;
      4. `foldby` per ultimo, ed e' sacrificabile: riproduce sul cluster vero un confronto
         che oggi esiste solo dal Mac (1.128 s contro 274 s). Se muore per timeout non
         costa niente; se muore con KilledWorker, quello E' il risultato.
    """
    def punto(curva, valore, **cambiato):
        return {"curva": curva, "valore": valore,
                "worker": disponibili, "thread": None,
                "k": K_RIFERIMENTO, "split_out": wc.SPLIT_OUT, "ripetizioni": 1,
                **cambiato}

    punti = [
        punto("riferimento", disponibili, ripetizioni=3),
        *[punto("worker",     w, worker=w) for w in range(disponibili - 1, 0, -1)],
        *[punto("partizioni", k, k=k)      for k in PARTIZIONI],
        *[punto("thread",     t, thread=t) for t in THREAD],
        punto("foldby", 0, split_out=0),
    ]
    return [dict(p, ripetizione=r) for p in punti for r in range(p["ripetizioni"])]


# ----------------------------------------------------------------------------------
# Il cronometro
# ----------------------------------------------------------------------------------


def lavoro(files, k, split_out, destinazione):
    """Costruisce il lavoro da cronometrare. Non calcola niente: restituisce il grafo.

    Stesse funzioni di `word_count.main`, chiamate allo stesso modo: il benchmark misura
    quello che si consegna, non una versione piu' comoda da cronometrare.

    Cronometriamo la SCRITTURA del vocabolario e non anche la top-N. Non e' uno sconto:
    la scrittura obbliga a percorrere tutto il Map e tutto il Reduce, e la top-N e' una
    selezione su un risultato gia' calcolato. Misurato sul campione: 1,77 s la sola
    scrittura, 1,72 s la scrittura piu' la top-N su un risultato messo da parte. La
    stessa cosa.
    """
    bag = wc.read_groups(wc.split_evenly(files, k))
    _, global_counts = wc.word_count(bag, split_out=split_out)
    frame = global_counts.to_dataframe(meta={"word": "string", "count": "int64"})
    scrittura = frame.to_parquet(destinazione, write_index=False, compression="zstd",
                                 overwrite=True, compute=False)
    return [scrittura], bag.npartitions


def cronometra(client, collezioni, timeout):
    """Calcola `collezioni` col cronometro e un tetto di tempo. -> secondi.

    SOLLEVA se il calcolo fallisce o sfora: la riga del CSV la scrive chi chiama, che ha
    gia' un `except` per il cluster che non parte. Un fallimento e' un dato, non una
    catastrofe - "non ha completato" e' una riga della tabella, non un buco.

    Il tetto esiste per la notte: una configurazione che si impianta non deve prendersi
    le ore che restano. Non serve cancellare i future: fra un istante il cluster viene
    spento comunque.
    """
    inizio = time.perf_counter()
    futures = client.compute(collezioni)
    wait(futures, timeout=timeout)
    for future in futures:
        future.result()  # rilancia qui se il calcolo e' morto sul cluster
    return round(time.perf_counter() - inizio, 1)


def stato_cluster(client):
    """Quanti worker e quanti thread ci sono DAVVERO adesso.

    Non quanti ne avevi chiesti: se durante la misura ne e' morto uno, la riga del CSV
    deve dirlo, altrimenti fra sei settimane quella misura non e' piu' interpretabile.
    """
    workers = client.scheduler_info()["workers"].values()
    return {"worker": len(workers),
            "thread": sum(w.get("nthreads", 0) for w in workers)}


def misura(p, files, args):
    """Accende un cluster, cronometra UNA configurazione, lo spegne. -> la riga del CSV.

    Un solo `try/except`, e copre allo stesso modo un cluster che non parte e un calcolo
    che muore: da fuori sono la stessa cosa, cioe' un punto che non c'e'.

    `performance_report` avvolge il cronometro e non il contrario, cosi' la generazione
    dell'HTML - che avviene all'uscita dal `with` - resta fuori dalla misura. Ce n'e' uno
    per OGNI punto, non quattro scelti a priori: costa due secondi, evita di dover
    indovinare prima quale servira', e porta con se' la memoria di ogni worker nel tempo
    (scheda System), che e' il motivo per cui quel dato non sta nel CSV.

    `mode="inline"` incorpora BokehJS nella pagina. Senza, l'HTML se lo scarica da
    cdn.bokeh.org e resta BIANCO ovunque non ci sia internet - per esempio dopo averlo
    copiato giu' dal cluster, che e' l'unico momento in cui lo guarderai.
    """
    client = cluster = None
    riga = dict(p, file=len(files), partizioni=None, secondi=None, errore="")
    percorso = args.out / f"report_{p['curva']}_{p['valore']}_{p['ripetizione']}.html"
    vocabolario = args.out / "vocabolario"
    try:
        client, cluster = get_client(repo_root=REPO,
                                     n_workers=p["worker"], n_threads=p["thread"])
        riga.update(stato_cluster(client))   # appena acceso: la configurazione verificata

        # Il vocabolario lo scrivono I WORKER, ognuno sul proprio disco, e `to_parquet`
        # crea la cartella solo qui sul client. Senza questa riga il primo task di
        # scrittura muore con FileNotFoundError su ogni macchina che non ce l'ha: fsspec
        # apre in scrittura con `auto_mkdir=False` e non crea le cartelle mancanti.
        # E' anche il motivo per cui la cartella sulla macchina scheduler resta VUOTA a
        # fine run: li' non gira nessun worker.
        client.run(os.makedirs, str(vocabolario), exist_ok=True)

        collezioni, riga["partizioni"] = lavoro(files, p["k"], p["split_out"], vocabolario)
        with performance_report(filename=str(percorso), mode="inline"):
            riga["secondi"] = cronometra(client, collezioni, args.timeout)
        riga.update(stato_cluster(client))   # a misura finita: se un worker e' morto, si vede qui
    except Exception as errore:
        riga["errore"] = f"{type(errore).__name__}: {errore}"[:200]
    finally:
        if client is not None:
            client.close()
        if cluster is not None:
            cluster.close()
        time.sleep(PAUSA_FRA_CLUSTER)
    return riga


def scrivi_riga(percorso, riga):
    """Appende una misura al CSV. Subito, non alla fine.

    E' la differenza fra perdere una notte e perdere l'ultima misura.
    """
    nuovo = not percorso.exists()
    with open(percorso, "a", newline="") as fh:
        scrittore = csv.DictWriter(fh, fieldnames=COLONNE, extrasaction="ignore", restval="")
        if nuovo:
            scrittore.writeheader()
        scrittore.writerow(riga)


# ----------------------------------------------------------------------------------


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
    return parser.parse_args()


def main():
    args = parse_args()

    source = Path(args.input).expanduser()
    source = source if source.is_absolute() else REPO / source
    args.out = Path(args.out).expanduser()
    args.out = args.out if args.out.is_absolute() else REPO / args.out
    args.out.mkdir(parents=True, exist_ok=True)
    args.csv = args.out / "misure.csv"

    files = wc.paragraph_files(source)
    if not files:
        raise SystemExit(f"Nessun file .parquet in {source}")

    lista = campagna(available_workers(REPO))
    if args.only:
        lista = [p for p in lista if p["curva"] in args.only]
    if not lista:
        raise SystemExit(f"--only {args.only} non seleziona nessuna misura")

    print(f"input   : {source}  ({len(files)} file)")
    print(f"output  : {args.out}")
    print(f"csv     : {args.csv}  (in append)")
    print(f"tetto   : {args.timeout} s per misura")
    # La campagna si stampa PRIMA di eseguirla. E' la regola "niente fallback silenziosi"
    # applicata alla campagna stessa: quello che sta per succedere si legge in venti
    # secondi, e Ctrl-C e' li'.
    print(f"\n{len(lista)} misure, quindi {len(lista)} cluster, in quest'ordine:")
    for n, p in enumerate(lista, 1):
        print(f"  {n:>2}. {p['curva']:<11} valore={p['valore']:<5} worker={p['worker']} "
              f"thread={p['thread'] or 'default'} k={p['k']} split_out={p['split_out']} "
              f"rip={p['ripetizione']}")

    inizio = time.perf_counter()
    for n, p in enumerate(lista, 1):
        print(f"\n[{n}/{len(lista)}] {p['curva']}={p['valore']} rip={p['ripetizione']}")
        riga = misura(p, files, args)
        scrivi_riga(args.csv, riga)
        print("   ->", f"{riga['secondi']} s  ({riga['partizioni']} partizioni, "
                       f"{riga['worker']} worker, {riga['thread']} thread in tutto)"
              if riga["secondi"] is not None else f"FALLITO  {riga['errore']}")

    print(f"\ncampagna finita in {(time.perf_counter() - inizio) / 60:.0f} minuti")
    print("csv:", args.csv)


if __name__ == "__main__":
    main()
