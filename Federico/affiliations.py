"""Task 2.3.2 - i paesi e gli istituti piu' e meno rappresentati nella ricerca.

Il testo chiede di classificare paesi e istituti a partire dalle AFFILIAZIONI DEGLI
AUTORI, e suggerisce (senza obbligare) di passare dal Bag del 2.3.1 al DataFrame: qui si
usa il DataFrame di Dask.

    silver/authors            una riga per (paper, autore)
      |  dropna               un autore senza affiliazione non vota
      |  chiave               le grafie della stessa entita' cadono sulla stessa chiave
      |  drop_duplicates      (paper, chiave): un paper conta UNA volta per entita'
      v  value_counts         l'unico shuffle del job
    classifica  entita' -> paper, e la stessa cosa senza il dedup -> autori

Il conteggio per PAPER e' la metrica primaria: senza il dedup un articolo con quaranta
co-autori italiani varrebbe quaranta volte uno con un autore solo. Il conteggio per
AUTORE resta in tabella accanto, cosi' l'inflazione da co-autori si legge come numero.

Lo stesso file gira invariato sul Mac e su Cloud Veneto: dove gira lo decide cluster.txt,
mai il codice (vedi cluster.py).

    python Federico/affiliations.py                        # campione, cluster locale
    python Federico/affiliations.py data/silver/authors    # corpus completo
"""

import argparse
import re
import sys
import time
import unicodedata
from pathlib import Path

import dask
import dask.dataframe as dd

DEFAULT_INPUT = "data_sample/silver/authors"
DEFAULT_OUTPUT = "~/mapd-out/2_3_2"   # fuori dalla repo: sul cluster la repo si ricancella
TOP_N = 20

# Le sole colonne che servono. Il Parquet e' colonnare: leggerne tre invece di nove
# significa leggere meno byte dal disco, non filtrarli dopo.
COLUMNS = ["cord_uid", "country", "institution_norm"]

# Le due classifiche chieste dal testo: nome dell'output -> colonna di silver/authors.
AFFILIAZIONI = {"country": "country", "institution": "institution_norm"}


# ----------------------------------------------------------------------------------
# La chiave di raggruppamento
#
# Il silver normalizza gli istituti in modo DICHIARATAMENTE leggero (NFKC, spazi
# collassati, punteggiatura tolta ai bordi) e lascia la disambiguazione ai task. Quello
# che resta e' la stessa istituzione scritta in piu' modi, e non e' cosmetico: misurato
# sul corpus, "The University of Hong Kong" e' spezzata in SEI grafie e con il
# raggruppamento corretto passa dal 17o al 14o posto. Ogni regola qui sotto e' stata
# aggiunta dopo averne misurato l'effetto (README).
# ----------------------------------------------------------------------------------

# I marcatori di nota che il PDF attacca al nome dell'affiliazione: ‡ † △ ✉ e simili
BORDI = re.compile(r"^[^\w]+|[^\w]+$", re.UNICODE)
ARTICOLO = re.compile(r"^(the|la|le|el|il)\s+", re.IGNORECASE)


def chiave(nome):
    """Nome di affiliazione -> chiave di raggruppamento.

    Grafie diverse della stessa entita' devono cadere sulla stessa chiave:
    1- via i caratteri non alfanumerici ai bordi (i marcatori di nota)
    2- via l'articolo iniziale ('The University of X' = 'University of X')
    3- minuscolo
    4- accenti piegati: l'estrazione dal PDF perde gli accenti a intermittenza, e
       'aix marseille universite' e' lo stesso posto di 'aix marseille universite'
       con l'accento. NFKD separa la lettera dal segno, e i segni si scartano.
    """
    nome = ARTICOLO.sub("", BORDI.sub("", nome)).lower()
    nome = unicodedata.normalize("NFKD", nome)
    return "".join(c for c in nome if not unicodedata.combining(c))


# ----------------------------------------------------------------------------------
# Lettura: k partizioni si ottengono RAGGRUPPANDO I FILE, non con un repartition a valle
# (che rileggerebbe alla stessa granularita' e metterebbe la ricucitura nel cronometro).
# k e' la variabile della curva obbligatoria "tempo vs numero di partizioni".
# ----------------------------------------------------------------------------------


def author_files(path):
    """I file 'part.<n>.parquet' della cartella, ordinati per <n> e non alfabeticamente."""

    def part_number(file):
        pieces = file.stem.split(".")          # "part.137" -> ["part", "137"]
        return int(pieces[-1]) if pieces[-1].isdigit() else -1

    return sorted(Path(path).glob("*.parquet"), key=lambda f: (part_number(f), f.name))


def split_evenly(files, k):
    """La lista dei file divisa in k gruppi di lunghezza quasi uguale."""
    k = max(1, min(int(k), len(files)))        # k oltre il numero di file non ha senso
    return [files[i * len(files) // k:(i + 1) * len(files) // k] for i in range(k)]


def load_group(group):
    """Un gruppo di file -> un DataFrame pandas, cioe' UNA partizione."""
    import pyarrow.parquet as pq

    return pq.read_table(list(group), columns=COLUMNS).to_pandas()


def read_groups(groups):
    """k gruppi di file -> un DataFrame Dask a k partizioni. Lazy: qui non legge niente.

    `meta` e' lo scheletro vuoto (nomi e tipi delle colonne) che Dask usa per sapere la
    forma del risultato senza eseguire: si prende dallo schema di un file, che non costa
    una lettura.
    """
    import pyarrow.parquet as pq

    groups = [[str(file) for file in group] for group in groups]
    meta = pq.read_schema(groups[0][0]).empty_table().select(COLUMNS).to_pandas()
    return dd.from_map(load_group, groups, meta=meta)


# ----------------------------------------------------------------------------------
# Il calcolo
# ----------------------------------------------------------------------------------


def ranking(authors, column):
    """Le due classifiche di una colonna di affiliazione. Lazy.

    Restituisce (per_paper, per_autore):
        per_paper    indicizzata sulla CHIAVE   quanti paper per entita'
        per_autore   indicizzata sulla GRAFIA   quante righe-autore per grafia

    Le due indicizzazioni sono diverse apposta: la chiave serve a raggruppare, la grafia
    a ricavare l'etichetta leggibile. Cosi' l'etichetta non costa uno shuffle in piu' -
    si ricuce sul client, dove sono 10^5 righe (vedi `classifica`).

    value_counts riduce in UNA partizione, e qui e' la scelta giusta: le entita' distinte
    sono 206 paesi e ~10^5 istituti, non i 6 milioni di parole del 2.3.1 che avevano
    costretto a spezzare il reduce.
    """
    valid = authors[["cord_uid", column]].dropna(subset=[column])
    per_autore = valid[column].value_counts()
    chiavi = valid.assign(chiave=valid[column].map(chiave, meta=(column, "string")))
    per_paper = chiavi[["cord_uid", "chiave"]].drop_duplicates().chiave.value_counts()
    return per_paper, per_autore


def classifica(per_paper, per_autore):
    """Le due Serie gia' calcolate -> una tabella pandas ordinata per numero di paper.

    L'etichetta consegnata e' la GRAFIA PIU' FREQUENTE della chiave, non la chiave, che
    e' minuscola e senza accenti: 'The University of Hong Kong' resta scritto bene, e
    vale la somma delle sue sei grafie.
    """
    import pandas as pd

    grafie = per_autore.rename_axis("entity").reset_index(name="authors")
    grafie["chiave"] = grafie["entity"].map(chiave)
    grafie = grafie.sort_values("authors", ascending=False)   # la prima e' la piu' frequente
    frame = grafie.groupby("chiave").agg(entity=("entity", "first"),
                                         authors=("authors", "sum"))
    frame["papers"] = per_paper
    return (frame.sort_values(["papers", "entity"], ascending=[False, True])
                 .reset_index(drop=True)[["entity", "papers", "authors"]])


def barplot(frame, path, title):
    """Il grafico che il testo chiede, per la cima e per il fondo della classifica."""
    import matplotlib

    matplotlib.use("Agg")                      # nessuno schermo sulla VM
    import matplotlib.pyplot as plt

    righe = frame.iloc[::-1]                   # il primo in classifica finisce in alto
    fig, ax = plt.subplots(figsize=(9, max(4, 0.32 * len(righe))))
    ax.barh(righe["entity"].astype(str), righe["papers"], color="#2f6f73")
    ax.set_title(title)
    ax.set_xlabel("paper")
    ax.grid(axis="x", alpha=0.25)
    fig.tight_layout()
    fig.savefig(path, dpi=150)
    plt.close(fig)


# ----------------------------------------------------------------------------------


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("input", nargs="?", default=DEFAULT_INPUT,
                        help=f"cartella silver/authors (default {DEFAULT_INPUT}, il campione)")
    parser.add_argument("--out", default=DEFAULT_OUTPUT,
                        help=f"dove scrivere i risultati (default {DEFAULT_OUTPUT})")
    parser.add_argument("--top", type=int, default=TOP_N,
                        help=f"quante entita' in cima e in fondo alla classifica (default {TOP_N})")
    parser.add_argument("--partitions", type=int,
                        help="raggruppa i file in N partizioni (default: una per file). "
                             "E' il pomello della curva obbligatoria sulle partizioni")
    return parser.parse_args()


def main():
    args = parse_args()

    # I percorsi relativi si risolvono sulla radice della repo, non sulla cartella da cui lanci
    repo = Path(__file__).resolve().parent.parent
    sys.path.insert(0, str(repo))
    from cluster import get_client

    source = Path(args.input).expanduser()
    source = source if source.is_absolute() else repo / source
    out = Path(args.out).expanduser()
    out = out if out.is_absolute() else repo / out

    files = author_files(source)
    if not files:
        raise SystemExit(f"Nessun file .parquet in {source}")
    out.mkdir(parents=True, exist_ok=True)

    client, cluster = get_client(repo_root=repo)
    print("input     :", source, f"({len(files)} file)")
    print("output    :", out)

    authors = read_groups(split_evenly(files, args.partitions or len(files)))
    print("partizioni:", authors.npartitions)

    # Un solo grafo per tutte e quattro le Serie: cosi' i file si leggono UNA volta.
    # Calcolarle una per una rileggerebbe silver/authors da capo a ogni compute.
    lazy = [serie for column in AFFILIAZIONI.values() for serie in ranking(authors, column)]

    started = time.perf_counter()
    try:
        risultati = dask.compute(*lazy)
        elapsed = time.perf_counter() - started
    finally:
        client.close()
        if cluster is not None:
            cluster.close()

    for posizione, (nome, column) in enumerate(AFFILIAZIONI.items()):
        per_paper, per_autore = risultati[2 * posizione], risultati[2 * posizione + 1]
        frame = classifica(per_paper, per_autore)

        frame.to_csv(out / f"{nome}_ranking.csv", index=False)
        barplot(frame.head(args.top), out / f"{nome}_top.png",
                f"{nome}: le {args.top} entità con più paper")
        barplot(frame.tail(args.top), out / f"{nome}_bottom.png",
                f"{nome}: le {args.top} entità con meno paper")

        soli = int((frame["papers"] == 1).sum())
        print(f"\n=== {nome} ({column}) ===")
        print(f"entita' distinte: {len(frame):,}  |  con UN solo paper: {soli:,} "
              f"({100 * soli / len(frame):.1f}%)")
        print(f"coppie (paper, entita'): {int(frame['papers'].sum()):,}")
        print(frame.head(args.top).to_string(index=False))
        print("...")
        print(frame.tail(args.top).to_string(index=False))

    print(f"\nelapsed: {elapsed:.1f} s")
    print("scritti :", out)


if __name__ == "__main__":
    main()
