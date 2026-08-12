"""Task 2.3.1 - distributed word count over the CORD-19 body text.

v4: the algorithm of v1-v3, now on a real cluster and writing its results.

    Map phase     for each document D, emit the pairs (w, cp(w)),
                  where cp(w) is the number of occurrences of w inside D
    Reduce phase  for each word w, sum cp(w) over all documents -> c(w)

The same file runs unchanged on the Mac and on Cloud Veneto: where the computation
happens is decided by cluster.txt / env vars, never by editing the code (see bench.py).

    python Giulia/word_count.py                          # sample data, local cluster
    python Giulia/word_count.py data/silver/paragraphs    # full corpus

Sanitization rules are all measured, not customary - see NOTES.md.
"""

import argparse
import operator
import re
import sys
import time
import unicodedata
from collections import Counter, defaultdict
from pathlib import Path

import dask
import dask.bag as db
import dask.dataframe as dd

# The shared cluster/benchmark helpers live at the repository root.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from bench import get_client  # noqa: E402

# Global variables
DEFAULT_INPUT = "data_sample/silver/paragraphs" # Default input path for the paragraphs dataset
DEFAULT_OUTPUT = "reports/word_count" # git-ignored, and on the VM it sits on the data volume
TOP_N = 20 # Number of top words to display in order of occurence

# How many parts the final Reduce is split into. 0 = the plain `Bag.foldby`, which ends
# in ONE task holding the whole vocabulary: a memory ceiling and a serial tail. See
# `word_count` for the measurements that made this the default rather than the fallback.
SPLIT_OUT = 16

# Characters the corpus uses instead of the plain ASCII ones. U+2013 EN DASH is the
# single most frequent non-ASCII character in the whole corpus (286,687 hits in 1.1M
# paragraphs), so "covid–19" is a common spelling: without this table it would not
# match the hyphen branch of TOKEN below.
PUNCTUATION = str.maketrans({
    **dict.fromkeys("‐‑‒–—―−", "-"),   # hyphen, non-breaking hyphen, dashes, minus
    **dict.fromkeys("‘’‚‛", "'"),      # curly apostrophes
    **dict.fromkeys("“”„", '"'),       # curly quotes
})

# Letters a word may be made of. Latin plus accented Latin plus Greek, because 10.1% of
# the paragraphs contain Greek letters (tnf-α, ifn-β, α-helix) and 7.7% contain accented
# Latin: with a bare [a-z], "müller" becomes the two junk tokens "m" and "ller".
# Non-Latin scripts are deliberately left out: CJK is 0.03% of paragraphs and, having no
# spaces, would need a segmenter rather than a regex (NOTES.md, "da vedere in futuro").
LETTER = "a-zà-öø-þα-ω"

# A word starts with a letter, may contain digits, and may be joined by - or / to further
# pieces: covid-19, sars-cov-2, μg/ml. Starting with a letter also means bare numbers
# ("2020", "95") are dropped for free - they are not words.
TOKEN = re.compile(f"[{LETTER}][{LETTER}0-9]*(?:[-/][{LETTER}0-9]+)*")

MIN_LENGTH = 2 # drops the single letters left behind by formulas and initials

# PubMed Central stores every inline formula as MathML with a LaTeX fallback, and that
# fallback repeats a full preamble each time:
#     \documentclass[12pt]{minimal} \usepackage{amsmath} ... \begin{document} X \end{document}
# It survived into silver/paragraphs and it is not a rare glitch: 1.00% of all characters
# in the corpus, and without this the token `usepackage` alone ranks 10th overall with
# 255,743 hits. Removing the whole block kills the entire family of junk words
# (documentclass, amsmath, wasysym, upgreek, setlength, oddsidemargin, pt, ...) instead of
# chasing them one by one in the stopword list. Only `pmc` paragraphs are affected.
LATEX_FORMULA = re.compile(r"\\documentclass.*?\\end\{document\}", re.DOTALL)

# Plain English function words: they always win a raw word count and say nothing about
# the corpus.
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

# Not English, but noise the corpus produces: citations ("et al.", ranks 3 and 4 with
# 347,632 and 343,098 hits) and figure/table cross-references. Kept in a separate set so
# the distinction stays visible: STOPWORDS is a property of the language, ARTIFACTS is a
# property of *this* corpus and is the part that would need rethinking on another dataset.
ARTIFACTS = frozenset("""
et al fig figs figure figures table tables eq eqn respectively
""".split())

IGNORED = STOPWORDS | ARTIFACTS


def sanitize(text):
    """Normalize a paragraph before splitting it into words.

    NFKC first: it merges characters the corpus writes in two ways, most importantly
    U+00B5 MICRO SIGN and U+03BC GREEK SMALL LETTER MU (40,964 + 74,308 hits) which are
    the same letter to a reader. Then lowercase, then fold the typographic punctuation.
    """
    text = LATEX_FORMULA.sub(" ", text)
    return unicodedata.normalize("NFKC", text).lower().translate(PUNCTUATION)


def words(text):
    """Sanitized text -> the words we keep."""
    return [
        token
        for token in TOKEN.findall(sanitize(text))
        if len(token) >= MIN_LENGTH and token not in IGNORED
    ]


def document_counts(partition):
    """Map phase, one partition at a time: paragraphs -> ((cord_uid, word), cp).

    This is the assignment's Map phase, and it is also the MapReduce *combiner*: the
    counting happens locally, inside the partition, so what leaves the worker is one
    entry per (document, word) instead of one per occurrence.

    Doing it here rather than with a global `frequencies()` over (cord_uid, word) pairs
    is what makes the job possible at all - see NOTES.md, step v4. A document whose
    paragraphs straddle a partition boundary contributes more than one partial entry;
    the Reduce phase adds them up, so the result is unchanged.
    """
    per_document = defaultdict(Counter)
    for cord_uid, text in partition:
        per_document[cord_uid].update(words(text))
    return [
        ((cord_uid, word), count)
        for cord_uid, counts in per_document.items()
        for word, count in counts.items()
    ]


# The document id `partition_counts` puts in the key it no longer has. Nothing reads it -
# `word_of` and `word_and_count` both throw the first element away - but keeping the shape
# `((doc, word), count)` identical across the three combiners means the Reduce phase, and
# every benchmark that drives it, is literally the same code for all of them.
NO_DOCUMENT = None


def occurrence_counts(partition):
    """Map phase WITHOUT the combiner: one entry per OCCURRENCE. Benchmark only.

    This is the formulation you write first and the one that cannot work: it sends one
    record per word occurrence into the shuffle (785 M of them on the full corpus) instead
    of one per (document, word). It is here to be measured, not to be used - see
    `bench_word_count.py`, block A4, where it is run against growing slices to find the
    point at which it stops completing.
    """
    return [
        ((cord_uid, word), 1)
        for cord_uid, text in partition
        for word in words(text)
    ]


def partition_counts(partition):
    """Map phase with the combiner pushed one level FURTHER: one entry per (partition, word).

    `document_counts` counts per document because §2.3.1 asks for the pairs (w, cp(w)) of
    each document D. That granularity has a price: a word appearing in 500 documents of the
    same partition leaves the worker 500 times instead of once. This variant collapses the
    documents inside the partition, which is the least the Reduce phase could possibly be
    given, and therefore the lower bound the assignment's own formulation is measured
    against. It computes the same c(w) and it does NOT produce the assignment's Map output.
    """
    counts = Counter()
    for _, text in partition:
        counts.update(words(text))
    return [((NO_DOCUMENT, word), count) for word, count in counts.items()]


# The three rungs of the combiner ladder, from no combiner to the most aggressive one.
# "document" is the assignment's own formulation and stays the default everywhere.
COMBINERS = {
    "none": occurrence_counts,
    "document": document_counts,
    "partition": partition_counts,
}


def read_paragraphs(path, drop_reference_like=True, npartitions=None):
    """silver/paragraphs -> a Bag of (cord_uid, text) tuples.

    `is_reference_like` flags author-contribution, conflict-of-interest and funding
    statements (1.24% of the paragraphs, and short ones: 477 characters against 867 on
    average). They are boilerplate, not body content, and their vocabulary is measurably
    its own - `declare`, `conflict`, `manuscript`, `financial`. Dropping them moves
    nothing in the top 30 (four swaps between neighbours, no word enters or leaves), so
    this is a matter of definition rather than of results, and it is free.

    The filter runs on the DataFrame, before `to_bag`: a vectorized mask over a boolean
    column is much cheaper than testing every element once we are down in the Bag.

    `npartitions` truncates the input, for smoke tests on a slice of the corpus.
    """
    paragraphs = dd.read_parquet(path, columns=["cord_uid", "text", "is_reference_like"])
    if npartitions:
        paragraphs = paragraphs.partitions[:npartitions]
    if drop_reference_like:
        paragraphs = paragraphs[~paragraphs.is_reference_like]
    return paragraphs[["cord_uid", "text"]].to_bag(format="tuple")


# ----------------------------------------------------------------------------------
# The benchmark reader. `read_paragraphs` above stays the production path; this one
# exists because the mandatory "time vs number of partitions" sweep cannot be measured
# honestly through it.
# ----------------------------------------------------------------------------------


def paragraph_files(path):
    """The Parquet files of a silver/paragraphs directory, in `part.<n>` NUMERIC order.

    Numeric and not lexicographic on purpose: sorted as strings, `part.10` lands between
    `part.1` and `part.2`. The order has to be stable *and* meaningful, because the
    benchmarks measure a fixed slice `files[:N]` and that slice was verified representative
    of the whole corpus in exactly this order: on the first 400 files, 75.7% of the
    paragraphs come from `pmc` against 73.5% over the whole corpus, and 838 bytes of text
    per row against 778. The source matters - the LaTeX preamble only exists in `pmc`.
    """
    def part_number(file):
        pieces = file.stem.split(".")  # "part.137" -> ["part", "137"]
        return int(pieces[-1]) if pieces[-1].isdigit() else -1

    return sorted(Path(path).glob("*.parquet"), key=lambda f: (part_number(f), f.name))


def split_evenly(files, k):
    """Split a file list into `k` groups of (almost) equal length.

    `k` is clamped to the number of files: one file is the finest grain this reader can
    produce, because a silver/paragraphs Parquet holds a single row group and a row group
    cannot be split. To go finer, `Bag.repartition` on the result - and say so in the
    record, since that is a different mechanism.
    """
    k = max(1, min(int(k), len(files)))
    return [files[i * len(files) // k: (i + 1) * len(files) // k] for i in range(k)]


def load_group(partition, drop_reference_like):
    """One Bag partition, holding one group of file paths -> its (cord_uid, text) pairs."""
    import pyarrow.compute as pc
    import pyarrow.parquet as pq

    pairs = []
    for group in partition:  # partition_size=1 below: exactly one group per partition
        table = pq.read_table(list(group), columns=["cord_uid", "text", "is_reference_like"])
        if drop_reference_like:
            # Same rule as the DataFrame path. Measured on the corpus: no nulls in this
            # column, so `== False` and `~` select exactly the same rows.
            table = table.filter(pc.equal(table["is_reference_like"], False))
        pairs.extend(zip(table["cord_uid"].to_pylist(), table["text"].to_pylist()))
    return pairs


def read_groups(groups, drop_reference_like=True):
    """Groups of Parquet files -> a Bag of (cord_uid, text), ONE GROUP PER PARTITION.

    Why this exists rather than `read_paragraphs(...).repartition(k)`:

    - through `dd.read_parquet` the partition count is not a knob you set, it is a
      property you discover. `blocksize` does not move it (measured, 4 MB to 64 MB, with
      and without a distributed Client: always 1979, because each silver file holds a
      single row group and nothing aggregates across files) - and then `to_bag` forces
      `optimize()`, and dask-expr COALESCES the small partitions. Measured on the corpus:
      `dd.read_parquet(...).npartitions` says 1979, and what actually runs is **990**.
      That is the 990 the VM printed for 1979 files, and the number nobody asked for;
    - repartitioning afterwards means the file reading always happens at that granularity
      anyway, and a reshuffle whose cost depends on k is then billed to the stopwatch. The
      sweep would be measuring the repartition, not the partitioning.

    Grouping the FILES instead gives exactly k, no shuffle at all, and a read cost that
    scales with partition size the way it should. Verified end to end: for k requested,
    the executed graph really contains k Map tasks - a Bag is not a dask-expr collection,
    so no optimizer coalesces it behind us. The work items travelling in the graph are
    file names only (PROJECT_CONTEXT.md, rule 8.7).
    """
    groups = [[str(file) for file in group] for group in groups]
    return db.from_sequence(groups, partition_size=1).map_partitions(
        load_group, drop_reference_like
    )


def word_of(doc_count):
    """((cord_uid, word), cp) -> word. The grouping key of the Reduce phase."""
    (_, word), _ = doc_count
    return word


def add_count(total, doc_count):
    """Reduce step: accumulate one document's cp(w) into the running total.
       doc_count is ((cord_uid, word), cp)
    """
    return total + doc_count[1]


def word_and_count(doc_count):
    """((cord_uid, word), cp) -> (word, cp). Drops the document id before the Reduce."""
    (_, word), count = doc_count
    return word, count


def word_count(paragraphs, split_out=SPLIT_OUT, combiner="document"):
    """Bag of (cord_uid, text) -> the two Bags of the assignment, still lazy.

    Returns (doc_counts, global_counts):
        doc_counts     ((cord_uid, word), cp)   the Map phase output
        global_counts  (word, c)                the Reduce phase output

    `combiner` selects the granularity of the Map phase and only ever moves for a
    benchmark: "document" is what §2.3.1 asks for and the default. See `COMBINERS`.

    `split_out` chooses how the Reduce is executed. Both paths compute exactly the same
    thing - verified on the full corpus, 6,037,808 words and 785,753,529 occurrences, every
    count equal - but `split_out=0` (a plain `Bag.foldby`) ends in a single task, which is
    both a memory ceiling and a serial tail. Measured on the whole corpus:

        7.0 GB/worker, foldby            1128 s, 0 workers killed
        3.4 GB/worker, foldby            1762 s, 3 workers killed
        3.4 GB/worker, split_out=16       274 s, 0 workers killed

    Keep `split_out=0` when you want to reproduce that comparison; otherwise leave it.
    """
    # Map phase. Purely local: no data crosses the network here, and the number of
    # partitions is unchanged.
    doc_counts = paragraphs.map_partitions(COMBINERS[combiner])

    if split_out:
        # Sharded Reduce, for when the vocabulary does not fit in one worker.
        # `foldby` cannot split its output, but a DataFrame groupby can: `split_out`
        # hashes the words into that many partitions, so each final task holds roughly
        # vocabulary/split_out entries instead of all of it. We come straight back to a
        # Bag of (word, count) so everything downstream is unchanged.
        # The assignment explicitly allows moving between the two structures.
        #
        # The task-based shuffle is not a preference: the P2P one cannot rebuild its
        # spec on the scheduler for a graph that started life as a Bag, and the run dies
        # with "P2P <id> failed during transfer phase" (a KeyError on the shuffle id).
        dask.config.set({"dataframe.shuffle.method": "tasks"})
        frame = doc_counts.map(word_and_count).to_dataframe(
            meta={"word": "string", "count": "int64"}
        )
        summed = frame.groupby("word")["count"].sum(split_out=split_out).reset_index()
        return doc_counts, summed.to_bag(format="tuple")

    # Reduce phase. Group the per-document pairs by word and sum their counts.
    # Foldby groups by the same word
    # binop is the function that combines the counts for each word, starting from an initial count of 0
    # combine is the function that combines the results from different partitions, also starting from an initial count of 0
    #
    # Both `foldby` and `frequencies` collapse a Bag into ONE output partition, so the
    # whole key space has to fit in a single task. Here the keys are the vocabulary,
    # which saturates as the corpus grows (Heaps' law) - that is affordable. Keying on
    # (cord_uid, word) instead would not be: that key space grows with the number of
    # documents and blew a 7 GB worker on 10% of the corpus (NOTES.md, step v4).
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


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("input", nargs="?", default=DEFAULT_INPUT, help="silver/paragraphs directory")
    parser.add_argument("--out", default=DEFAULT_OUTPUT, help=f"output directory (default {DEFAULT_OUTPUT})")
    parser.add_argument("--top", type=int, default=TOP_N, help="how many words to export and plot")
    parser.add_argument("--partitions", type=int, help="use only the first N partitions (smoke test)")
    parser.add_argument("--split-out", type=int, default=SPLIT_OUT,
                        help=f"shard the final reduce into N parts (default {SPLIT_OUT}); "
                             "0 selects the plain Bag foldby, which is slower and needs a "
                             "big worker - useful to reproduce the comparison")
    parser.add_argument("--keep-references", action="store_true", help="do not drop reference-like paragraphs")
    parser.add_argument("--check", action="store_true", help="also verify the Map/Reduce invariant (doubles the runtime)")
    return parser.parse_args()


def main():
    args = parse_args()

    # Relative paths are resolved against the REPOSITORY, not the current directory:
    # `python Giulia/word_count.py` and `cd Giulia && python word_count.py` must mean
    # the same thing, otherwise the defaults point at Giulia/data_sample/... and the
    # results land in Giulia/reports/.
    repo = Path(__file__).resolve().parent.parent
    source = Path(args.input)
    source = source if source.is_absolute() else repo / source
    out = Path(args.out)
    out = out if out.is_absolute() else repo / out

    if not source.exists():
        raise SystemExit(
            f"Input non trovato: {source}\n"
            "Passa il path del silver, per esempio:\n"
            "  python Giulia/word_count.py ~/mapd-data/silver/paragraphs"
        )
    out.mkdir(parents=True, exist_ok=True)

    client, cluster = get_client(repo_root=repo)
    print("input     :", source)
    print("output    :", out)

    paragraphs = read_paragraphs(
        source,
        drop_reference_like=not args.keep_references,
        npartitions=args.partitions,
    )
    print("partitions:", paragraphs.npartitions)
    print("reduce    :", f"DataFrame groupby, split_out={args.split_out}"
          if args.split_out else "Bag foldby (una sola partizione in uscita)")
    doc_counts, global_counts = word_count(paragraphs, split_out=args.split_out)

    started = time.perf_counter()
    try:
        # `topk` alone can prune early: each partition only forwards its own top N.
        # The invariant cannot - it has to add up every per-document count, which
        # materializes the whole intermediate table and doubles the runtime (measured:
        # 4.7 s -> 9.5 s on the sample). That is why it is opt-in and not always on.
        if args.check:
            # One compute(): the Map phase is shared instead of being run three times.
            # key=1 means "rank by the second element of the tuple", the count.
            top, after_map, after_reduce = dask.compute(
                global_counts.topk(args.top, key=1),
                doc_counts.pluck(1).sum(),
                global_counts.pluck(1).sum(),
            )
            # Structural invariant: the Reduce phase only regroups, it never loses or
            # invents occurrences. No hard-coded corpus totals - the local dump and the
            # VM corpus are different datasets (PROJECT_CONTEXT.md, rule 8.2).
            assert after_map == after_reduce, (after_map, after_reduce)
        else:
            top = global_counts.topk(args.top, key=1).compute()
            after_map = None

        # The full vocabulary goes to Parquet: too big to print, and it is what any
        # further analysis (or a different top-N) should be recomputed from.
        counts_frame = global_counts.to_dataframe(meta={"word": "string", "count": "int64"})
        counts_frame.to_parquet(out / "word_counts", write_index=False, compression="zstd", overwrite=True)
        elapsed = time.perf_counter() - started
    finally:
        client.close()
        if cluster is not None:
            cluster.close()

    with open(out / "top_words.csv", "w") as fh:
        fh.write("word,count\n")
        for word, count in top:
            fh.write(f"{word},{count}\n")
    barplot(top, out / "top_words.png", f"Top {len(top)} words in the CORD-19 body text")

    for word, count in top:
        print(f"{count:>12,}  {word}")
    print(f"\nelapsed: {elapsed:.1f} s")
    if after_map is not None:
        print(f"invariant ok: {after_map:,} occurrences before and after the reduce")
    print("written:", out / "top_words.csv", out / "top_words.png", out / "word_counts")


if __name__ == "__main__":
    main()
