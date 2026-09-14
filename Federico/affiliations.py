"""Task 2.3.2 - the most and least represented countries and institutions.

Countries and institutions are ranked from the AUTHORS' AFFILIATIONS, with the Dask
DataFrame instead of the Bag of task 2.3.1.

    silver/authors            one row per (paper, author)
      |  dropna               an author with no affiliation does not vote
      |  chiave               the spellings of one entity fall on the same key
      |  drop_duplicates      (paper, key): a paper counts ONCE per entity
      v  value_counts         the only shuffle of the job

Counting per PAPER is the primary metric: without the dedup an article with forty
Italian co-authors would weigh forty times one with a single author. The per-AUTHOR
count stays in the table next to it, so co-author inflation can be read as a number.

    python Federico/affiliations.py                       # sample, local cluster
    python Federico/affiliations.py data/silver/authors   # full corpus
"""

import argparse
import re
import sys
import time
import unicodedata
from pathlib import Path

import dask
import dask.dataframe as dd

# Input and output paths
DEFAULT_INPUT = "data_sample/silver/authors"
DEFAULT_OUTPUT = "~/mapd-out/2_3_2"  
TOP_N = 20

# Default number (overwritten with args in benchmark script)
DEFAULT_PARTITIONS = 8


COLUMNS = ["cord_uid", "country", "institution_norm"]
AFFILIAZIONI = {"country": "country", "institution": "institution_norm"}

# SANIFICATION from artifacts in pdf/pmc parsing using regexp
# ^[^\w]+ : At the beginning of the string search for one or more (+) non alphanumeric characters or _
# Pipe (|) here works as a OR
# [^\w]+$ : At the end of the string search for one or more (+) non alphanumeric characters or _
# Unicode helps for accents interpretation
# PS: It was full of entities like: †University of Padua‡

BORDI = re.compile(r"^[^\w]+|[^\w]+$", re.UNICODE)

# Improved standardization removing articles.
# ^(the|la|le|el|il) : Search for these at the beginning of the string
# \s+ : Followed by one or more blank spaces (case independent)
ARTICOLO = re.compile(r"^(the|la|le|el|il)\s+", re.IGNORECASE)


def chiave(nome):
    """
    Sanitization and standardization of the names.
    Applies BORDI and ARTICOLO regular expression and sobstitutes the match with null space.
    Then separates accents from characters using NFKD and in the end remove the accent.
    """
    # Apply regexp on a name and convert to lower case
    nome = ARTICOLO.sub("", BORDI.sub("", nome)).lower()
    # NFKD standardization works like this on accents: "é" → "e" + "´" 
    nome = unicodedata.normalize("NFKD", nome)

    # Removing the accents previously separated in the return
    # Example: "université de padoue" → "universite de padoue"
    return "".join(c for c in nome if not unicodedata.combining(c))


def author_files(path):
    """
    Sort the parquet files (part.<n>.parquet) and sort by increasing n instead of alphabetical order
    """

    def part_number(file):
        pieces = file.stem.split(".")          # "part.137" -> ["part", "137"]
        return int(pieces[-1]) if pieces[-1].isdigit() else -1

    return sorted(Path(path).glob("*.parquet"), key=lambda f: (part_number(f), f.name))


def split_evenly(files, k):
    """
    Given all the path to the files return a list of group files with the desired number of partitions.
    PS: an element of the returned list is a group of path. One of this element is something like:
        [
        "part.0.parquet",
        "part.1.parquet",
        "part.2.parquet"
        ]

    """
    k = max(1, min(int(k), len(files)))        # Check: Don't exceed the total number of files
    return [files[i * len(files) // k:(i + 1) * len(files) // k] for i in range(k)]


# ----------------------------------------------------------------------------------
# ----------------------------------------------------------------------------------


def load_group(group):
    """
    Load a group of files and select just the selected columns.
    Converts the pyarrow table to Pandas DataFrame.
    """

    import pyarrow.parquet as pq

    return pq.read_table(list(group), columns=COLUMNS).to_pandas()


def read_groups(groups):
    """
    Create one lazy DaskDataframe with one partition per group.
    meta is an empty Pandas Dataframe used to infer the schema from the first element and select the desired columns.
    Then load_group is mapped to each group, returning for each call a Pandas DataFrame which becomes one DaskPartition
    """
    import pyarrow.parquet as pq

    # Converts Path objects to strings while preserving groups structure
    groups = [[str(file) for file in group] for group in groups]

    # Read only the schema of the first file and build the metadata DataFrame.
    meta = pq.read_schema(groups[0][0]).empty_table().select(COLUMNS).to_pandas()

    # Build one lazy Dask DataFrame with one partition per group (Pandas DF).
    return dd.from_map(load_group, groups, meta=meta)


# ----------------------------------------------------------------------------------
# ----------------------------------------------------------------------------------


def ranking(authors, column):
    """
    Takes authors dask DataFrame and a column (country or institution_norm) and returns:

    per_autore : Dask DF containing frequencies with non-sanitized affiliations
    per_paper  : Dask DF containing frequencies with sanitized affiliations

    per_paper  : (key,key_counts)
    per_author : (country or institution, count) with the non-sanitized entities

    Performs lazy operation
    """

    # Remove rows with missing country or institute affiliation (is a Dask DataFrame)
    valid = authors[["cord_uid", column]].dropna(subset=[column])

    # Get unique values from the selected column and the corresponding frequency
    per_autore = valid[column].value_counts()

    # Create a new Dask DF from valid with a new column called Chiave
    # The new column is filled with the chiave function applied to the Dask Series valid[column] that is partitioned
    # meta is used to deduce sctructure and type without executing (lazy)
    chiavi = valid.assign(chiave=valid[column].map(chiave, meta=(column, "string")))

    # Now we select the id column and key, dedup duplicates (same paper listed multiple times) and count frequencies
    per_paper = chiavi[["cord_uid", "chiave"]].drop_duplicates().chiave.value_counts()

    return per_paper, per_autore


def classifica(per_paper, per_autore):
    """
    Takes the computed series and returns a pandas DF with the results of the classification
    """
    import pandas as pd

    # Link every entity to the number of authors connected to that entity
    grafie = per_autore.rename_axis("entity").reset_index(name="authors")
    # Link every entity to the corresponding sanitized key
    grafie["chiave"] = grafie["entity"].map(chiave)

    # For every key take the most frequent entity
    grafie = grafie.sort_values("authors", ascending=False)   

    # Aggregate for every key the most frequent entity with the sum of the number of authors
    # EXAMPLE:    
    #       chiave                    | entity                      | authors
    #       university of hong kong   | The University of Hong Kong | 100
    frame = grafie.groupby("chiave").agg(entity=("entity", "first"),
                                         authors=("authors", "sum"))
    # Add the column with the number of papers associated with the key
    frame["papers"] = per_paper
    # Now sort for descending n. of papers or alphabetical for a spare and drop the key column
    return (frame.sort_values(["papers", "entity"], ascending=[False, True])
                 .reset_index(drop=True)[["entity", "papers", "authors"]])


def barplot(frame, path, title):
    import matplotlib

    matplotlib.use("Agg")                     
    import matplotlib.pyplot as plt

    righe = frame.iloc[::-1]                  
    fig, ax = plt.subplots(figsize=(9, max(4, 0.32 * len(righe))))
    ax.barh(righe["entity"].astype(str), righe["papers"], color="#2f6f73")
    ax.set_title(title)
    ax.set_xlabel("paper")
    ax.grid(axis="x", alpha=0.25)
    fig.tight_layout()
    fig.savefig(path, dpi=150)
    plt.close(fig)


# ----------------------------------------------------------------------------------

# Command-line arguments:
#   input       Optional authors dataset path; defaults to the sample dataset.
#   --out       Output directory for ranking CSV files and plots.
#   --top       Number of entities shown in the top and bottom rankings.
#   --partitions Number of Dask partitions used to process the dataset.

def parse_args():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("input", nargs="?", default=DEFAULT_INPUT,
                        help=f"cartella silver/authors (default {DEFAULT_INPUT}, il campione)")
    parser.add_argument("--out", default=DEFAULT_OUTPUT,
                        help=f"dove scrivere i risultati (default {DEFAULT_OUTPUT})")
    parser.add_argument("--top", type=int, default=TOP_N,
                        help=f"quante entita' in cima e in fondo alla classifica (default {TOP_N})")
    parser.add_argument("--partitions", type=int, default=DEFAULT_PARTITIONS,
                        help=f"raggruppa i file in N partizioni (default {DEFAULT_PARTITIONS}, "
                             "misurato). E' il pomello della curva obbligatoria sulle partizioni")
    return parser.parse_args()


def main():
    args = parse_args()

    # Add the repository to the the available python modules to import
    repo = Path(__file__).resolve().parent.parent
    sys.path.insert(0, str(repo))

    from cluster import get_client

    # Input and output path
    source = Path(args.input).expanduser()
    source = source if source.is_absolute() else repo / source
    out = Path(args.out).expanduser()
    out = out if out.is_absolute() else repo / out

    # Debug
    files = author_files(source)
    if not files:
        raise SystemExit(f"Nessun file .parquet in {source}")
    out.mkdir(parents=True, exist_ok=True)

    # Connect to the scheduler and cluster
    client, cluster = get_client(repo_root=repo)
    print("input     :", source, f"({len(files)} file)")
    print("output    :", out)

    # Create Dask Lazy collection distributed in k partitions with columns: "cord_uid", "country", "institution_norm" for every author 
    authors = read_groups(split_evenly(files, args.partitions))
    print("partizioni:", authors.npartitions)


    # Preparing the four Dask graph (lazy):
    #   [
    #       per_paper_country,
    #       per_autore_country,
    #       per_paper_institution,
    #       per_autore_institution
    #   ]
    # The idea is to compute the four graphs simoultaneously to share common operations and read the files just one time
    lazy = [serie for column in AFFILIAZIONI.values() for serie in ranking(authors, column)]

    started = time.perf_counter()
    try:
        # Computing simoultaneosly all graphs in lazy list
        risultati = dask.compute(*lazy)
        elapsed = time.perf_counter() - started
    finally:
        # Cluster shut dowon
        client.close()
        if cluster is not None:
            cluster.close()


    for posizione, (nome, column) in enumerate(AFFILIAZIONI.items()):
        per_paper, per_autore = risultati[2 * posizione], risultati[2 * posizione + 1]

        # Use Pandas to create the ranking
        frame = classifica(per_paper, per_autore)

        # Graphs
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
