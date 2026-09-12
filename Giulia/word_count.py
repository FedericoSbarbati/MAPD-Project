"""Task 2.3.1 - distributed word count over the CORD-19 body text.

v4: the algorithm of v1-v3, now on a real cluster and writing its results.

    Map phase     for each document D, emit the pairs (w, cp(w)),
                  where cp(w) is the number of occurrences of w inside D
    Reduce phase  for each word w, sum cp(w) over all documents -> c(w)

The same file runs unchanged on the Mac and on Cloud Veneto: where the computation
happens is decided by cluster.txt / env vars, never by editing the code (see cluster.py).

    python Giulia/word_count.py                          # sample data, local cluster
    python Giulia/word_count.py data/silver/paragraphs    # full corpus

Sanitization rules are all measured, not customary - see NOTES.md.
"""

import argparse
import operator
import os
import re
import sys
import time
import unicodedata
from collections import Counter, defaultdict
from pathlib import Path

import dask
import dask.bag as db

# Global variables
DEFAULT_INPUT = "data_sample/silver/paragraphs" # Default input path for the paragraphs dataset
DEFAULT_OUTPUT = "~/mapd-out/word_count" # outside the repo: on the cluster the repo is disposable
TOP_N = 20 # Number of top words to display in order of occurence

# Numbers of partition of the final part of Reduce task. If 0 it's a plain foldby
SPLIT_OUT = 16


# Non ASCII characthers are converted to plain ASCII ones using this susbstitution table
PUNCTUATION = str.maketrans({
    **dict.fromkeys("‐‑‒–—―−", "-"),   # hyphen, non-breaking hyphen, dashes, minus
    **dict.fromkeys("‘’‚‛", "'"),      # curly apostrophes
    **dict.fromkeys("“”„", '"'),       # curly quotes
})

# Defining valid characters inside a word:
# From a-z minuscole
# From à-ö in latin alphabet minuscole
# ø-þ other lating characters
# α-ω greek letters minuscole
LETTER = "a-zà-öø-þα-ω"

# Regex experession used to define the form of the word searched by the code using the characters in letter
# [{LETTER}] first word must be a letter
# [{LETTER}0-9]* after the first word search for a letter or a number, * means search zero or plus
# (?: ... ) non capturing group: match the sequence without creating a separate group
# Into the non capturing group we search for the characters - or /
# [{LETTER}0-9]+ search a word or number, + means search for one or more
# (?: ... )* the * means that the whole non capturing group can appear zero or more times

TOKEN = re.compile(f"[{LETTER}][{LETTER}0-9]*(?:[-/][{LETTER}0-9]+)*")

MIN_LENGTH = 2 # drops the single letters left behind by formulas and initials

# NOTE: PubMed Central stores every inline formula as MathML with a LaTeX fallback, and that
# fallback repeats a full preamble each time:
#     \documentclass[12pt]{minimal} \usepackage{amsmath} ... \begin{document} X \end{document}
# Removing the whole block kills the entire family of junk words
# (documentclass, amsmath, wasysym, upgreek, setlength, oddsidemargin, pt, ...) instead of
# chasing them one by one in the stopword list. Only `pmc` paragraphs are affected.

# Regular expression to remove latex leftover from pmc parsing pipeline:
# \\documentclass.*? search for document class and ? stops the search at the first
# occurrence of \end{document}
# re.DOTALL accounts for formulas distributed among more than one lines otherwise
# it would stop at the first endline \n
# The whole identified block is sobstituted by a blank space

LATEX_FORMULA = re.compile(r"\\documentclass.*?\\end\{document\}", re.DOTALL)

# Creates an immutable set of elements for noise brought from the paper structure
STOPWORDS = frozenset("""
a about above across after again against all also am among an and any are as at be
because been before being below besides between both but by can could despite did do
does doing down during each either every few for from further had has have having he
her here hers herself him himself his how however i if in into is it its itself just may
many me might more most much must my myself neither no nor not now of off on once one
only or other our ours ourselves out over own per several shall she should since so some
such than that the their theirs them themselves then there therefore these they this
those three through throughout thus to too toward towards two under until up upon us
very via was we were what when where whether which while who whom why will with within
without would you your yours yourself yourselves
""".split())

ARTIFACTS = frozenset("""
et al fig figs figure figures table tables eq eqn respectively
""".split())

# Union of the two sets
IGNORED = STOPWORDS | ARTIFACTS


def sanitize(text):
    """
    Sanitize paragraph before splitting into words.
    1- Applies the latex formula regex and sobstitute with a blank space
    2- Normalize the text to unicode NFKC standard for a common semantic representation
    3- Convert to lower case
    4- Apply PUNCTUATION sobstitution table
    """
    text = LATEX_FORMULA.sub(" ", text)
    return unicodedata.normalize("NFKC", text).lower().translate(PUNCTUATION)


def words(text):
    """
    Text-> Sanitized text + Filtering -> remaining words.

    Search for all sequences respecting the regex defined in TOKEN over the sanitized text.
    Returns a list of words that are longer than the min lenght and are not in the ignored sets
    
    """
    return [
        token
        for token in TOKEN.findall(sanitize(text))
        if len(token) >= MIN_LENGTH and token not in IGNORED
    ]


def document_counts(partition):
    """
    Map phase, one partition at a time: paragraphs -> ((cord_uid, word), cp).

    Takes as input a partition that contains pair of elements with this shape: (cord_uid, text)
    where cord_uid identifies a document.
    
    defaultdict(Counter) creates a dictionary containing a COUNTER for every document.
    The COUNTER associated to a document contains a dictionary containing every word and it's number of 
    occurrences.
    The dictionaries are then converted to a list of pairs of shape: ((cord_uid,word),count)

    """
    per_document = defaultdict(Counter)
    for cord_uid, text in partition:
        per_document[cord_uid].update(words(text))
    return [
        ((cord_uid, word), count)
        for cord_uid, counts in per_document.items()
        for word, count in counts.items()
    ]


# ----------------------------------------------------------------------------------

# There are 1979 parquet files in silver/paragraphs.
# Using dd.read_parquet use a number of partitions automatically choose by dask.
# We want to group this 1979 files in a number k of partitions of our choice since it's a variable
# of our benchmarks.

# ----------------------------------------------------------------------------------


def paragraph_files(path):
    """
    Parquet files are stored into silver/paragraphs with names 'part.<n>.parquet' where n is the 
    number identifying the partition.

    part_number(file): Extract <n> from every filename that as been found using glob() to retrieve 
    all .parquet files.

    The function returns all the paths of the parquet files that are sorted by the ascendent number
    of partitions <n> instead of alphabetical order.

    """


    def part_number(file):
        pieces = file.stem.split(".")  # "part.137" -> ["part", "137"]
        return int(pieces[-1]) if pieces[-1].isdigit() else -1

    return sorted(Path(path).glob("*.parquet"), key=lambda f: (part_number(f), f.name))


def split_evenly(files, k):
    """
    Split a file list into `k` groups of (almost) equal length.
    """

    # Check for edge case: non int k or k bigger than the number of available files
    k = max(1, min(int(k), len(files))) 

    # Returns a list of k groups containing the assigned files
    return [files[i * len(files) // k: (i + 1) * len(files) // k] for i in range(k)]


def load_group(partition):
    """
    Read a Bag partition holding one group of file paths and read the columns 'cord_uid'
    and 'text'. Return a python list with (cord_uid,text) of the partition.
    """

    import pyarrow.parquet as pq

    pairs = []
    for group in partition:  # partition_size=1 below: exactly one group per partition

        # PyArrow table with the desired columns
        table = pq.read_table(list(group), columns=["cord_uid", "text"])
        # Converting the PyArrow tables in Python lists and add them to partition list of (cord_uid,text)
        pairs.extend(zip(table["cord_uid"].to_pylist(), table["text"].to_pylist()))

    return pairs


def read_groups(groups):
    """
    Takes all groups of parquet files and for every group create a partition.
    To every partition is applied the load_group function that creates the bag of (cord_uid,text).
    It's a lazy method. No compute() or persist() is called here.
    """

    # Converts Paths to string without altering the group structure
    groups = [[str(file) for file in group] for group in groups]
    # Creates a dask bag from the groups sequence where every partition contains exactly 1 group of files
    # map_partitions(load_group) apply load_group to every partition.
    return db.from_sequence(groups, partition_size=1).map_partitions(load_group)


def word_of(doc_count):
    """
    ((cord_uid, word), cp) -> word. The grouping key of the Reduce phase.
    """

    (_, word), _ = doc_count

    return word


def add_count(total, doc_count):
    """
    Reduce step: accumulate one document's cp(w) into the running total.
    doc_count is ((cord_uid, word), cp)
    """
    return total + doc_count[1] # Total + cp


def word_and_count(doc_count):
    """
    ((cord_uid, word), cp) -> (word, cp). Drops the document id before the Reduce.
    """

    (_, word), count = doc_count
    return word, count


def word_count(paragraphs, split_out=SPLIT_OUT):
    """
    Bag of (cord_uid, text) -> the two Bags of the assignment, still lazy ((cord_uid,word),counts).

    Returns (doc_counts, global_counts):
        doc_counts     ((cord_uid, word), cp)   the Map phase output
        global_counts  (word, c)                the Reduce phase output

    NOTES:
        7.0 GB/worker, foldby            1128 s, 0 workers killed
        3.4 GB/worker, foldby            1762 s, 3 workers killed
        3.4 GB/worker, split_out=16       274 s, 0 workers killed

    Keep `split_out=0` when you want to reproduce that comparison; otherwise leave it.
    """

    # Map phase: apply document_counts to every partition performing the operation:
    # (cord_uid,text) -> ((cord_uid, word), cp)
    # paragraphs is a Dask Bag of pairs (cord_uid,text)
    doc_counts = paragraphs.map_partitions(document_counts)

    if split_out:

        # Sharded reduce phase: allows to divide the final results in different partitions avoiding
        # exceeding single worker memory.


        # Passing to dask the explicit method to shuffle data across different worker.
        # Forcing to use 'task' as shuffle method forces dask to create explicit task for sharing
        # data between workers (necessary because we convert a Bag to DataFrame)
        dask.config.set({"dataframe.shuffle.method": "tasks"})


        # Remove cord_uid: ((cord_uid, word), cp) -> (word,cp)
        frame = doc_counts.map(word_and_count).to_dataframe(
            meta={"word": "string", "count": "int64"}
        )
        # Group by word and sum counts. The results are splitted into split_out partitions
        summed = frame.groupby("word")["count"].sum(split_out=split_out).reset_index()

        # doc_counts: ((cord_uid,word),counts)
        # global_counts = (word, count)
        return doc_counts, summed.to_bag(format="tuple")

    # Reduce phase. Group the per-document pairs by word and sum their counts.
    # Foldby groups by the same word

    # binop is the function that combines the counts for each word, starting from an initial count of 0
    # combine is the function that combines the results from different partitions, also starting from an initial count of 0
    
    # Both `foldby` and `frequencies` collapse a Bag into ONE output partition, so the
    # whole key space has to fit in a single task. Here the keys are the vocabulary,
    # which saturates ram as the corpus grows. 

    global_counts = doc_counts.foldby(
        key=word_of, binop=add_count, initial=0, combine=operator.add
    )
    return doc_counts, global_counts


def barplot(top, path, title):
    """The barplot of the most frequent words, as the assignment asks for."""
    import matplotlib

    matplotlib.use("Agg")  # no display on the VM
    import matplotlib.pyplot as plt

    labels = [word for word, _ in reversed(top)]
    values = [count for _, count in reversed(top)]

    fig, ax = plt.subplots(figsize=(9, max(4, 0.32 * len(top))))
    ax.barh(labels, values, color="#2f6f73")
    ax.set_title(title)
    ax.set_xlabel("occurrences")
    ax.grid(axis="x", alpha=0.25)
    fig.tight_layout()
    fig.savefig(path, dpi=150)
    plt.close(fig)

# Parser degli argomenti:
# input: cartella dei paragrafi Parquet
# --out: cartella di destinazione dei risultati
# --top: numero di parole più frequenti da esportare
# --partitions: numero di partizioni di input da usare
# --split-out: numero di partizioni del Reduce finale; 0 usa foldby
def parse_args():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("input", nargs="?", default=DEFAULT_INPUT, help="silver/paragraphs directory")
    parser.add_argument("--out", default=DEFAULT_OUTPUT, help=f"output directory (default {DEFAULT_OUTPUT})")
    parser.add_argument("--top", type=int, default=TOP_N, help="how many words to export and plot")
    parser.add_argument("--partitions", type=int,
                        help="cut the data into N partitions (default: one per Parquet "
                             "file). This is the knob the mandatory benchmark sweeps")
    parser.add_argument("--split-out", type=int, default=SPLIT_OUT,
                        help=f"shard the final reduce into N parts (default {SPLIT_OUT}); "
                             "0 selects the plain Bag foldby, which is slower and needs a "
                             "big worker - useful to reproduce the comparison")
    return parser.parse_args()


def main():
    args = parse_args()

    # Goest to repository root to import cluster.py
    repo = Path(__file__).resolve().parent.parent

    # Add the repository to the the available python modules to import
    sys.path.insert(0, str(repo))
    from cluster import get_client

    # Adjusting input and output paths passed from the user to match the repo structure
    source = Path(args.input).expanduser()
    source = source if source.is_absolute() else repo / source
    out = Path(args.out).expanduser()
    out = out if out.is_absolute() else repo / out

    # Debugging tools
    if not source.exists():
        raise SystemExit(
            f"Input non trovato: {source}\n"
            "Use the example silver path:\n"
            "  python Giulia/word_count.py ~/mapd-data/silver/paragraphs"
        )
    out.mkdir(parents=True, exist_ok=True)

    # Other debugging tools
    files = paragraph_files(source)
    if not files:
        raise SystemExit(f"Nessun file .parquet in {source}")

    # Connect to the scheduler and cluster
    client, cluster = get_client(repo_root=repo)
    print("input     :", source, f"({len(files)} file)")
    print("output    :", out)

    # Create locally on every worker the folder out/word_counts to save the results
    client.run(os.makedirs, str(out / "word_counts"), exist_ok=True)

    # Creates the Dask lazy collection (cord_uid,text)
    paragraphs = read_groups(split_evenly(files, args.partitions or len(files)))


    print("partitions:", paragraphs.npartitions)
    print("reduce    :", f"DataFrame groupby, split_out={args.split_out}"
          if args.split_out else "Bag foldby (una sola partizione in uscita)")

    # Global counts = (word, count), Dask lazy collection
    _, global_counts = word_count(paragraphs, split_out=args.split_out)

    started = time.perf_counter()
    try:

        # Trigger the action and store results into worker memory
        global_counts = global_counts.persist()
        # topk.compute would execute again map and reduce phase, with persist they are in memory.
        top = global_counts.topk(args.top, key=1).compute()

        # The full vocabulary goes to Parquet: too big to print, and it is what any
        # further analysis (or a different top-N) should be recomputed from.

        # Converts results dask bag to dataframe with fixed tipes for columns
        counts_frame = global_counts.to_dataframe(meta={"word": "string", "count": "int64"})
        # Write the vocabulary to parquet extension.
        counts_frame.to_parquet(out / "word_counts", write_index=False, compression="zstd", overwrite=True)
        elapsed = time.perf_counter() - started
    finally:
        client.close()
        if cluster is not None:
            cluster.close()

    # Storing results: create csv and bar plot 
    with open(out / "top_words.csv", "w") as fh:
        fh.write("word,count\n")
        for word, count in top:
            fh.write(f"{word},{count}\n")
    barplot(top, out / "top_words.png", f"Top {len(top)} words in the CORD-19 body text")

    for word, count in top:
        print(f"{count:>12,}  {word}")
    print(f"\nelapsed: {elapsed:.1f} s")
    print("written:", out / "top_words.csv", out / "top_words.png", out / "word_counts")


if __name__ == "__main__":
    main()
