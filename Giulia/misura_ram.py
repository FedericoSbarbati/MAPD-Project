"""Quanta RAM occupa UN task del word count, e in quale passo se ne va.

    python Giulia/misura_ram.py ~/mapd-data/silver/paragraphs

**Non serve nessun cluster**: gira in un processo solo, sequenziale. E' apposta - vogliamo
il costo di UN task, senza altri task attorno che sporcano la misura.

A cosa serve. Meta' della curva sulle partizioni (§9.1) non completa: sotto k=64 i worker
muoiono con `KilledWorker`. Questo script dice **perche'**, con dei numeri invece che con
un'ipotesi: esegue le stesse identiche funzioni del task che muore e misura la RSS del
processo passo per passo.

Misurato sul corpus completo (1979 file, worker da 7,1 GB):

    k    testo    coppie Map   picco 1 task   x4 thread   esito sul cluster
    512  0,03 GB    834 k        0,63 GB        2,0 GB    ok,  489,8 s
    256  0,06 GB   1,81 M        1,11 GB        3,9 GB    ok,  497,6 s
    128  0,12 GB   3,75 M        2,10 GB        7,9 GB    ok,  508,8 s (sul filo)
     64  0,29 GB   7,93 M        4,21 GB       16,3 GB    KilledWorker

Il muro cade dove la misura lo mette. Due numeri da ricordare:

  * il picco di un task e' circa **15 volte** il testo che sta elaborando;
  * la fase Map da sola vale il 44% del picco, ed e' **227 byte per ogni coppia**
    ((cord_uid, parola), conteggio) - il costo dell'oggetto Python, non del dato.

Da cui: i thread non sono solo unita' di calcolo, sono **moltiplicatori di memoria**.
Quattro thread nello stesso worker vogliono quattro volte quel picco.

**Ogni k gira in un processo NUOVO**, per lo stesso motivo per cui il benchmark accende un
cluster nuovo per ogni misura: la RSS non torna indietro quando Python libera, quindi la
seconda misura dentro lo stesso processo partirebbe da un valore gonfiato e il suo picco
sarebbe quello della prima.

Lo script si ferma da solo quando la partizione successiva non entrerebbe nella RAM
libera: e' pensato per girare anche sulla VM scheduler, che di GB ne ha 8.
"""

import argparse
import concurrent.futures as futures
import gc
import csv
import multiprocessing
import resource
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(REPO / "Giulia"))

import word_count as wc  # noqa: E402

DEFAULT_INPUT = "data_sample/silver/paragraphs"
DEFAULT_OUT = "~/mapd-out/bench"
# Dal piu' fine al piu' grosso: cosi' le partizioni crescono e ci si ferma quando non
# entrano piu', invece di far fuori la macchina al primo colpo.
DEFAULT_K = (512, 256, 128, 64, 32)

COLONNE = ["k", "file", "paragrafi", "byte_testo", "coppie_map", "picco_gb",
           "gb_lettura", "gb_map", "gb_proiezione", "gb_dataframe", "gb_groupby"]


def rss_gb():
    import psutil

    return psutil.Process().memory_info().rss / 1e9


def picco_gb():
    """Il massimo storico di RSS del processo. Su macOS e' in byte, su Linux in KB."""
    massimo = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return massimo / 1e9 if sys.platform == "darwin" else massimo / 1e6


def libera_gb():
    import psutil

    return psutil.virtual_memory().available / 1e9


def misura(percorsi):
    """Esegue UNA partizione passo per passo. -> la riga di risultati.

    GIRA IN UN PROCESSO TUTTO SUO (vedi `in_un_processo_nuovo`), quindi prende dei
    percorsi e non oggetti gia' costruiti.

    I passi sono esattamente quelli del task che muore sul cluster, il cui nome nel grafo
    e' `document_counts-from-bag-to_dataframe-chunk-reset_index-operation`: leggere,
    contare per documento, proiettare sulla parola, diventare DataFrame, sommare.
    """
    import pandas as pd

    gruppo = [Path(p) for p in percorsi]

    def tappa(nome, precedente):
        gc.collect()
        adesso = rss_gb()
        print(f"    {nome:<36} RSS {adesso:>6.2f} GB   (+{adesso - precedente:>5.2f})")
        return adesso, adesso - precedente

    partenza = rss_gb()
    riga = {"file": len(gruppo)}

    coppie = wc.load_group([[str(f) for f in gruppo]])
    riga["paragrafi"] = len(coppie)
    riga["byte_testo"] = sum(len(testo) for _, testo in coppie)
    dopo, riga["gb_lettura"] = tappa("load_group (coppie uid,testo)", partenza)

    mappa = wc.document_counts(coppie)
    riga["coppie_map"] = len(mappa)
    dopo, riga["gb_map"] = tappa("document_counts (fase Map)", dopo)

    righe = [wc.word_and_count(x) for x in mappa]
    dopo, riga["gb_proiezione"] = tappa("map(word_and_count)", dopo)

    frame = pd.DataFrame(righe, columns=["word", "count"])
    dopo, riga["gb_dataframe"] = tappa("to_dataframe (chunk)", dopo)

    sommato = frame.groupby("word")["count"].sum().reset_index()
    dopo, riga["gb_groupby"] = tappa("groupby + reset_index", dopo)

    riga["picco_gb"] = picco_gb() - partenza
    print(f"    -> {riga['paragrafi']:,} paragrafi, {riga['byte_testo'] / 1e9:.2f} GB di "
          f"testo, {riga['coppie_map']:,} coppie, {len(sommato):,} parole distinte")
    return riga


def in_un_processo_nuovo(percorsi):
    """Lancia `misura` in un processo appena nato e ne riporta indietro la riga.

    `spawn` e non `fork`: il figlio riparte da zero invece di ereditare la memoria del
    padre, che e' esattamente cio' che rende la misura pulita.
    """
    contesto = multiprocessing.get_context("spawn")
    with futures.ProcessPoolExecutor(max_workers=1, mp_context=contesto) as pool:
        return pool.submit(misura, percorsi).result()


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("input", nargs="?", default=DEFAULT_INPUT,
                        help=f"silver/paragraphs (default {DEFAULT_INPUT}, il campione)")
    parser.add_argument("--out", default=DEFAULT_OUT, help=f"dove scrivere (default {DEFAULT_OUT})")
    parser.add_argument("--k", type=int, nargs="+", default=list(DEFAULT_K),
                        help="i partizionamenti da misurare, dal piu' fine al piu' grosso")
    return parser.parse_args()


def main():
    args = parse_args()

    source = Path(args.input).expanduser()
    source = source if source.is_absolute() else REPO / source
    out = Path(args.out).expanduser()
    out = out if out.is_absolute() else REPO / out
    out.mkdir(parents=True, exist_ok=True)
    destinazione = out / "memoria.csv"

    files = wc.paragraph_files(source)
    if not files:
        raise SystemExit(f"Nessun file .parquet in {source}")

    print(f"input : {source}  ({len(files)} file)")
    print(f"csv   : {destinazione}")
    print(f"RAM   : {libera_gb():.1f} GB libera adesso\n")

    misurate = []
    # Quanto e' costato un byte di Parquet, dall'ultima misura: serve a prevedere se la
    # prossima partizione ci sta. Prima di avere una misura si parte prudenti.
    gb_per_byte = 50 / 1e9

    for k in args.k:
        gruppo = wc.split_evenly(files, k)[0]
        su_disco = sum(f.stat().st_size for f in gruppo)
        previsto = su_disco * gb_per_byte
        if previsto > 0.8 * libera_gb():
            print(f"k={k}: SALTATO - servirebbero ~{previsto:.1f} GB e ne sono libera "
                  f"{libera_gb():.1f}. E' il limite di questa macchina, non un errore.")
            continue

        print(f"k={k}: una partizione = {len(gruppo)} file, {su_disco / 1e6:.0f} MB su disco")
        riga = in_un_processo_nuovo([str(f) for f in gruppo])
        riga["k"] = k
        misurate.append(riga)
        gb_per_byte = riga["picco_gb"] / su_disco
        print()

    with open(destinazione, "w", newline="") as fh:
        scrittore = csv.DictWriter(fh, fieldnames=COLONNE, extrasaction="ignore")
        scrittore.writeheader()
        scrittore.writerows(misurate)

    print(f"{'k':>6} {'testo':>9} {'coppie Map':>12} {'picco':>9} {'byte/coppia':>12} {'espansione':>11}")
    for riga in misurate:
        print(f"{riga['k']:>6} {riga['byte_testo'] / 1e9:>8.2f}G {riga['coppie_map']:>12,} "
              f"{riga['picco_gb']:>8.2f}G "
              f"{riga['gb_map'] * 1e9 / max(riga['coppie_map'], 1):>11.0f}B "
              f"{riga['picco_gb'] * 1e9 / max(riga['byte_testo'], 1):>10.1f}x")
    print(f"\nscritto: {destinazione}")
    print("Per sapere se un k regge sul cluster: picco x (thread per worker) contro il")
    print("tetto di memoria di UN worker, che lo script del benchmark stampa all'avvio.")


if __name__ == "__main__":
    main()
