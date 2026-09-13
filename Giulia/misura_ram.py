import argparse
import concurrent.futures as futures
import gc
import csv
import multiprocessing
import resource
import sys
from pathlib import Path

# Add the repository and the Giulia folder to the the available python modules to import
REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(REPO / "Giulia"))

# The engine of the task is imported as a module: the steps measured here are ITS
# functions, not a copy of them. If word_count.py changes, this measures the new version.
import word_count as wc  # noqa: E402

# Input and output paths
DEFAULT_INPUT = "data_sample/silver/paragraphs"
DEFAULT_OUT = "~/mapd-out/bench"

# The partitionings to measure, from the finest to the biggest: in this way the partitions
# grow one after the other and we stop when they don't enter anymore, instead of killing
# the machine.
DEFAULT_K = (512, 256, 128, 64, 32)

# Columns of the output csv. One row = one k = one process:
# picco_gb is the peak of the whole task, the gb_* are the single steps
COLONNE = ["k", "file", "paragrafi", "byte_testo", "coppie_map", "picco_gb",
           "gb_lettura", "gb_map", "gb_proiezione", "gb_dataframe", "gb_groupby"]


def rss_gb():
    """
    RSS of this process in GB.
    RSS is the memory that the operating system is really giving to the process: it is the
    one that makes a worker die, not the one that Python believes it is using.
    """
    import psutil

    return psutil.Process().memory_info().rss / 1e9


def picco_gb():
    """
    The historical maximum of RSS touched by the process, that is the number we are
    looking for: the peak, not the memory occupied at the end.
    PS: macOS returns it in byte and Linux in KB, so the division is not the same.
    """
    massimo = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return massimo / 1e9 if sys.platform == "darwin" else massimo / 1e6


def libera_gb():
    """
    Free RAM of the machine in GB. Serves to decide if the next k can be measured or if
    this machine is too small for it.
    """
    import psutil

    return psutil.virtual_memory().available / 1e9


def misura(percorsi):
    """
    Execute ONE partition step by step and return the row of the results.

    IT RUNS IN A PROCESS ALL OF ITS OWN (see `in_un_processo_nuovo`), so it takes the paths
    of the files and not objects already built, that a new process could not receive.

    The steps are exactly the ones of the task that dies on the cluster, whose name in the
    graph is `document_counts-from-bag-to_dataframe-chunk-reset_index-operation`:
    1- read the parquet files of the group
    2- count the words document by document (the Map phase)
    3- project on the word, dropping the cord_uid
    4- become a Pandas DataFrame
    5- groupby on the word and sum
    """
    import pandas as pd

    gruppo = [Path(p) for p in percorsi]

    # Print the RSS after a step and return it together with the increment from the step
    # before. gc.collect() first, otherwise we are measuring also the garbage that Python
    # has not collected yet.
    def tappa(nome, precedente):
        gc.collect()
        adesso = rss_gb()
        print(f"    {nome:<36} RSS {adesso:>6.2f} GB   (+{adesso - precedente:>5.2f})")
        return adesso, adesso - precedente

    partenza = rss_gb()        # The baseline: every step is measured starting from here
    riga = {"file": len(gruppo)}

    # Step 1: read one group of files -> list of (cord_uid, text)
    coppie = wc.load_group([[str(f) for f in gruppo]])
    riga["paragrafi"] = len(coppie)
    riga["byte_testo"] = sum(len(testo) for _, testo in coppie)
    dopo, riga["gb_lettura"] = tappa("load_group (coppie uid,testo)", partenza)

    # Step 2: the Map phase, (cord_uid, text) -> ((cord_uid, word), cp)
    mappa = wc.document_counts(coppie)
    riga["coppie_map"] = len(mappa)
    dopo, riga["gb_map"] = tappa("document_counts (fase Map)", dopo)

    # Step 3: drop the cord_uid, ((cord_uid, word), cp) -> (word, cp)
    righe = [wc.word_and_count(x) for x in mappa]
    dopo, riga["gb_proiezione"] = tappa("map(word_and_count)", dopo)

    # Step 4: this is what to_dataframe does to ONE partition of the Bag
    frame = pd.DataFrame(righe, columns=["word", "count"])
    dopo, riga["gb_dataframe"] = tappa("to_dataframe (chunk)", dopo)

    # Step 5: the piece of the Reduce that every partition does by itself, before the shuffle
    sommato = frame.groupby("word")["count"].sum().reset_index()
    dopo, riga["gb_groupby"] = tappa("groupby + reset_index", dopo)

    # The peak of the whole task, cleaned from the memory that the process had at the start
    riga["picco_gb"] = picco_gb() - partenza
    print(f"    -> {riga['paragrafi']:,} paragrafi, {riga['byte_testo'] / 1e9:.2f} GB di "
          f"testo, {riga['coppie_map']:,} coppie, {len(sommato):,} parole distinte")
    return riga


def in_un_processo_nuovo(percorsi):
    """
    Launch `misura` in a process just born and bring its row back here.

    `spawn` and not `fork`: the son restarts from zero instead of inheriting the memory of
    the father, and this is exactly what makes the measure clean.
    """
    contesto = multiprocessing.get_context("spawn")
    with futures.ProcessPoolExecutor(max_workers=1, mp_context=contesto) as pool:
        return pool.submit(misura, percorsi).result()


# ----------------------------------------------------------------------------------

# Command-line arguments:
#   input       Optional paragraphs dataset path; defaults to the sample dataset.
#   --out       Output directory of the csv with the measures.
#   --k         The partitionings to measure, from the finest to the biggest.

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

    # Adjusting input and output paths passed from the user to match the repo structure
    source = Path(args.input).expanduser()
    source = source if source.is_absolute() else REPO / source
    out = Path(args.out).expanduser()
    out = out if out.is_absolute() else REPO / out
    out.mkdir(parents=True, exist_ok=True)
    destinazione = out / "memoria.csv"

    # Debug
    files = wc.paragraph_files(source)
    if not files:
        raise SystemExit(f"Nessun file .parquet in {source}")

    print(f"input : {source}  ({len(files)} file)")
    print(f"csv   : {destinazione}")
    print(f"RAM   : {libera_gb():.1f} GB libera adesso\n")

    misurate = []
    # How much a byte of Parquet costed in RAM, from the last measure: it serves to foresee
    # if the next partition enters or not. Before having a measure we start prudent.
    gb_per_byte = 50 / 1e9

    for k in args.k:
        # One partition is enough: split_evenly makes them of the same size, so the first
        # one is representative of all the others
        gruppo = wc.split_evenly(files, k)[0]
        su_disco = sum(f.stat().st_size for f in gruppo)
        previsto = su_disco * gb_per_byte

        # Stop condition: if the prevision eats more than the 80% of the free RAM we don't
        # even try. The 80% leaves the space for the operating system and for the peak that
        # arrives a little bit over the prevision.
        if previsto > 0.8 * libera_gb():
            print(f"k={k}: SALTATO - servirebbero ~{previsto:.1f} GB e ne sono libera "
                  f"{libera_gb():.1f}. E' il limite di questa macchina, non un errore.")
            continue

        print(f"k={k}: una partizione = {len(gruppo)} file, {su_disco / 1e6:.0f} MB su disco")
        riga = in_un_processo_nuovo([str(f) for f in gruppo])
        riga["k"] = k
        misurate.append(riga)

        # Update the prevision with the measure just done: the next k is bigger, and now we
        # know how much a byte of this corpus costs
        gb_per_byte = riga["picco_gb"] / su_disco
        print()

    # Storing results: one row per k, the skipped ones are simply not there
    with open(destinazione, "w", newline="") as fh:
        scrittore = csv.DictWriter(fh, fieldnames=COLONNE, extrasaction="ignore")
        scrittore.writeheader()
        scrittore.writerows(misurate)

    # Final table: byte per pair and expansion (peak / text) are the two numbers of the NOTES
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
