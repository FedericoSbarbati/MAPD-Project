"""Benchmark del task 2.3.3. Un comando, e la campagna gira da sola.

    python daniele/bench_title_embeddings.py ~/mapd-data/silver/papers

Stesso metodo della campagna del word count (`Giulia/bench_word_count.py`), perche' i
risultati devono poter stare nella stessa relazione: **una riga del CSV = una misura =
un cluster nuovo**, e ogni punto e' IL RIFERIMENTO CON UNA MANOPOLA CAMBIATA.

Le manopole qui sono cinque, e le prime due sono le due larghezze del grafo:

    partizioni   in quante parti si tagliano i TITOLI          -> larghezza della Map
    blocksize    in che blocchi si legge il MODELLO da 4,5 GB  -> larghezza della lettura
    worker       quanti PROCESSI                               -> chi esegue
    thread       quanti task DENTRO lo stesso processo         -> chi esegue + la memoria
    split_out    in quante parti finisce il Reduce             -> la coda del grafo

piu' un punto singolo, `broadcast`, che confronta le due strategie di join (mandare il
modello a tutti contro rimescolare entrambi i lati). Sul word count la campagna aveva
mostrato che l'unita' di calcolo utile e' il PROCESSO e non il thread: qui la fase
pesante e' diversa (parsing di 600 M di float, che rilascia il GIL dentro pandas), quindi
la misura sui thread e' una domanda aperta e non una conferma.

**Prova generale prima di lasciarla andare**, sul campione e con un tetto basso:

    python daniele/bench_title_embeddings.py --out /tmp/bench-prova --timeout 300

**Calibrazione**, tre misure dello stesso punto: danno la dispersione e il budget:

    python daniele/bench_title_embeddings.py ~/mapd-data/silver/papers --only riferimento

**E poi, sul serio**, dentro tmux e con l'output su file:

    tmux new -s bench-emb
    python daniele/bench_title_embeddings.py ~/mapd-data/silver/papers \\
        --ripetizioni 3 2>&1 | tee ~/bench-emb.log

Ogni riga porta i secondi e il **picco di RAM del worker piu' carico** (`picco_gb`),
misurato con lo stesso strumento di `Giulia/misura_ram.py`.
"""

import argparse
import csv
import os
import sys
import time
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(REPO / "daniele"))

from distributed import performance_report, wait  # noqa: E402

import title_embeddings as te  # noqa: E402
from cluster import available_workers, get_client  # noqa: E402

# Il file da spedire a tutte le macchine prima di calcolare: vedi `misura`.
CODICE = REPO / "daniele" / "title_embeddings.py"

DEFAULT_INPUT = "data_sample/silver/papers"
DEFAULT_OUT = "~/mapd-out/bench-embeddings"

# Il punto di riferimento: e' anche il punto in comune a tutte le curve.
# 32 partizioni di titoli = ~30 k titoli l'una: abbastanza fini da bilanciare il carico
# su pochi worker, abbastanza grosse da non annegare nello scheduling.
K_RIFERIMENTO = 32
# 64 MB di modello per blocco = ~70 blocchi sul file da 4,5 GB. E' la stessa scala che il
# gruppo dell'anno scorso aveva trovato migliore sul loro modello (32-128 MB).
BLOCK_RIFERIMENTO = "64MB"
SPLIT_OUT_RIFERIMENTO = 8

# Dal centro verso i bordi: se la campagna si interrompe, quello che resta in mano e' il
# minimo della curva con i due rami vicini, cioe' la parte che risponde alla domanda.
PARTIZIONI = (16, 64, 8, 128, 4, 256)
BLOCCHI = ("32MB", "128MB", "16MB", "256MB")
THREAD = (2, 4, 1)
SPLIT_OUT = (16, 4, 1)

# Fra la chiusura di un cluster e l'apertura del successivo: la porta 8786 ha bisogno di
# un attimo per tornare libera.
PAUSA_FRA_CLUSTER = 10

COLONNE = ["curva", "valore", "ripetizione", "secondi", "errore",
           "partizioni", "blocksize", "split_out", "broadcast",
           "vocabolario", "worker", "thread", "picco_gb", "picco_medio_gb"]


# ----------------------------------------------------------------------------------
# La campagna: una tabella, non del codice
# ----------------------------------------------------------------------------------


def campagna(disponibili, thread=None, ripetizioni=1):
    """La giornata intera, in una lista. Si legge dall'alto in basso.

    L'ordine non e' casuale: il `riferimento` per primo perche' calibra la stima dei
    tempi di tutto il resto; poi le due larghezze del grafo; poi le due curve
    obbligatorie (worker e thread); in fondo i due punti sacrificabili, che se muoiono
    per timeout non costano niente - e se muoiono con KilledWorker, quello E' il
    risultato.

    LE RIPETIZIONI SONO PASSATE INTERE, non misure consecutive dello stesso punto: fra
    due ripetizioni dello stesso punto passano ore, quindi la dispersione comprende la
    variabilita' della macchina nella giornata e non solo quella di due run attaccati.
    """
    def punto(curva, valore, **cambiato):
        return {"curva": curva, "valore": valore,
                "worker": disponibili, "thread": thread,
                "partizioni": K_RIFERIMENTO, "blocksize": BLOCK_RIFERIMENTO,
                "split_out": SPLIT_OUT_RIFERIMENTO, "broadcast": True,
                **cambiato}

    punti = [
        punto("riferimento", disponibili),
        *[punto("partizioni", k, partizioni=k) for k in PARTIZIONI],
        *[punto("blocksize", b, blocksize=b) for b in BLOCCHI],
        *[punto("worker", w, worker=w) for w in range(disponibili - 1, 0, -1)],
        *[punto("thread", t, thread=t) for t in THREAD if t != thread],
        *[punto("split_out", s, split_out=s) for s in SPLIT_OUT],
        punto("broadcast", 0, broadcast=False),
    ]
    return [dict(p, ripetizione=r) for r in range(ripetizioni) for p in punti]


# ----------------------------------------------------------------------------------
# Il cronometro
# ----------------------------------------------------------------------------------


def picco_memoria(client):
    """Quanta RAM ha toccato al massimo OGNI worker durante questa misura. -> GB.

    `ru_maxrss` e' il massimo storico di RSS del processo, e siccome ogni misura accende
    un cluster NUOVO, il massimo storico di quel processo E' il picco di questa misura.
    La regola "un cluster per riga" si ripaga anche qui.

    L'unita' di `ru_maxrss` CAMBIA COL SISTEMA (byte su macOS, KB su Linux) e la
    conversione va fatta SUL WORKER: il cluster e' Linux e il portatile da cui si lancia
    potrebbe non esserlo.
    """
    def rss_massima():
        import resource
        import sys as _sys

        massimo = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        return massimo / 1e9 if _sys.platform == "darwin" else massimo / 1e6

    picchi = list(client.run(rss_massima).values())
    if not picchi:
        return {}
    return {"picco_gb": round(max(picchi), 2),
            "picco_medio_gb": round(sum(picchi) / len(picchi), 2)}


def stato_cluster(client):
    """Quanti worker e quanti thread ci sono DAVVERO adesso, non quanti ne hai chiesti.

    `client.nthreads()` e NON `client.scheduler_info()["workers"]`: il secondo SOTTO-CONTA
    quando piu' worker girano sulla stessa macchina, e ha gia' marchiato "5 worker" due
    campagne che ne avevano 8 e 16.
    """
    thread = client.nthreads()
    return {"worker": len(thread), "thread": sum(thread.values())}


def misura(p, source, model_path, args):
    """Accende un cluster, cronometra UNA configurazione, lo spegne. -> la riga del CSV.

    Un solo `try/except`, e copre allo stesso modo un cluster che non parte e un calcolo
    che muore: da fuori sono la stessa cosa, cioe' un punto che non c'e'.

    IL CRONOMETRO COMPRENDE ENTRAMBE LE FASI, vocabolario incluso. La pipeline non e'
    interamente pigra - il vocabolario deve esistere sul driver prima che il modello si
    possa filtrare - e misurare solo la seconda fase vorrebbe dire misurare qualcosa che
    non si consegna. Il `timeout` copre pero' solo la scrittura, che e' la fase lunga:
    e' l'unica su cui si puo' mettere un tetto senza cambiare il codice del task.
    """
    client = cluster = None
    riga = dict(p, secondi=None, errore="", vocabolario=None)
    report = args.out / f"report_{p['curva']}_{p['valore']}_{p['ripetizione']}.html"
    destinazione = args.out / "embeddings"
    try:
        client, cluster = get_client(repo_root=REPO,
                                     n_workers=p["worker"], n_threads=p["thread"])
        riga.update(stato_cluster(client))   # appena acceso: la configurazione verificata

        # Su questo cluster i dati sono replicati su ogni macchina, il codice no: nel
        # grafo le funzioni viaggiano PER NOME, quindi scheduler e worker devono poter
        # importare questo modulo. -> PROJECT_CONTEXT.md §8.12a
        client.upload_file(str(CODICE))
        # Gli embedding li scrivono I WORKER, ognuno sul proprio disco, mentre
        # `to_parquet` crea la cartella solo qui sul client. -> §8.12b
        client.run(os.makedirs, str(destinazione), exist_ok=True)

        with performance_report(filename=str(report), mode="inline"):
            inizio = time.perf_counter()
            embeddings, vocabolario, partizioni = te.build(
                source, model_path,
                partitions=p["partizioni"], blocksize=p["blocksize"],
                split_out=p["split_out"], broadcast=p["broadcast"])
            scrittura = embeddings.to_parquet(destinazione, write_index=False,
                                              compression="zstd", overwrite=True,
                                              compute=False)
            futures = client.compute(scrittura)
            wait(futures, timeout=args.timeout)
            futures.result()          # rilancia qui se il calcolo e' morto sul cluster
            riga["secondi"] = round(time.perf_counter() - inizio, 1)

        riga["vocabolario"] = vocabolario
        riga["partizioni"] = partizioni      # quelle VERE, non quelle chieste
        riga.update(stato_cluster(client))   # se un worker e' morto, si vede qui
        riga.update(picco_memoria(client))
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
                        help=f"silver/papers (default {DEFAULT_INPUT}, il campione)")
    parser.add_argument("--model", default=te.DEFAULT_MODEL,
                        help=f"file .vec del modello (default {te.DEFAULT_MODEL})")
    parser.add_argument("--out", default=DEFAULT_OUT, help=f"dove scrivere (default {DEFAULT_OUT})")
    parser.add_argument("--timeout", type=int, default=3600,
                        help="tetto in secondi per una singola misura (default 3600)")
    parser.add_argument("--thread", type=int, default=None,
                        help="thread per worker del punto di riferimento (default: quello di cluster.py)")
    parser.add_argument("--ripetizioni", type=int, default=1,
                        help="quante passate intere della campagna (default 1)")
    parser.add_argument("--only", default=None,
                        help="una sola curva: riferimento, partizioni, blocksize, worker, thread, split_out, broadcast")
    return parser.parse_args()


def main():
    args = parse_args()

    source = Path(args.input).expanduser()
    source = source if source.is_absolute() else REPO / source
    model_path = Path(args.model).expanduser()
    args.out = Path(args.out).expanduser()
    args.out = args.out if args.out.is_absolute() else REPO / args.out
    args.out.mkdir(parents=True, exist_ok=True)

    if not source.exists():
        raise SystemExit(f"Input non trovato: {source}")
    if not model_path.exists():
        raise SystemExit(
            f"Modello non trovato: {model_path}\n"
            "Deve stare ALLO STESSO PATH SU OGNI WORKER: non c'e' piu' un file system\n"
            "condiviso, ogni macchina lo legge dal proprio disco."
        )

    disponibili = available_workers(REPO)
    punti = campagna(disponibili, args.thread, args.ripetizioni)
    if args.only:
        punti = [p for p in punti if p["curva"] == args.only]
        if not punti:
            raise SystemExit(f"Nessuna curva di nome '{args.only}'")

    destinazione = args.out / "misure.csv"
    print(f"input   : {source}")
    print(f"modello : {model_path}")
    print(f"csv     : {destinazione}")
    print(f"misure  : {len(punti)} (worker disponibili: {disponibili})")
    print(f"tetto   : {args.timeout} s per misura\n")

    for i, p in enumerate(punti, 1):
        print(f"[{i}/{len(punti)}] {p['curva']}={p['valore']} "
              f"(rip. {p['ripetizione']}) ...", flush=True)
        riga = misura(p, source, model_path, args)
        scrivi_riga(destinazione, riga)
        esito = riga["errore"] or f"{riga['secondi']} s, picco {riga.get('picco_gb', '?')} GB"
        print(f"          -> {esito}\n", flush=True)

    print("scritto:", destinazione)


if __name__ == "__main__":
    main()
