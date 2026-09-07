"""Task 2.3.3 - title embeddings with a pre-trained fastText model.

    Map phase     every title becomes a list of words; every word that the model
                  knows becomes a 300-dimensional vector
    Reduce phase  for every paper, average the vectors of its words -> ONE vector
                  per title (mean pooling), which is what task 2.3.4 consumes

The same file runs unchanged on the laptop and on Cloud Veneto: where the computation
happens is decided by cluster.txt / env vars, never by editing the code (see cluster.py).

    python daniele/title_embeddings.py                        # sample data, local cluster
    python daniele/title_embeddings.py ~/mapd-data/silver/papers

The model is NEVER loaded in memory. It is a 4.5 GB text file (`word v1 ... v300`, one
word per line) and it is read like any other dataset: `dd.read_csv` cuts it into blocks,
each worker parses its own blocks and immediately throws away the words that no title
uses. Only the surviving slice (~10^5 words out of 2*10^6) stays in the cluster.
"""

import argparse
import os
import re
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import dask.dataframe as dd

# NOTHING outside the standard library, Dask and the array libraries is imported here,
# and that is a requirement rather than a coincidence: this module has to be IMPORTABLE
# ON THE SCHEDULER AND ON THE WORKERS, which receive it as a single uploaded file and
# know nothing about this repository. `from cluster import get_client` would break that,
# so it lives inside `main()`, the only place that needs it.
# -> PROJECT_CONTEXT.md §8.12a

DEFAULT_INPUT = "data_sample/silver/papers"          # no arguments -> try on the sample
DEFAULT_MODEL = "~/mapd-model/crawl-300d-2M-subword.vec"
DEFAULT_OUTPUT = "~/mapd-out/title_embeddings"       # outside the repo: it is disposable

N_DIM = 300                  # fastText vector size, fixed by the model
VECTOR_COLUMNS = [f"v{i}" for i in range(N_DIM)]

# How wide the Map is: how many partitions the titles are cut into. 0 = leave the
# partitioning that Parquet gives (one per file).
PARTITIONS = 0

# How the model file is cut. It is the second width knob, and the one specific to this
# task: a block is what ONE task reads, parses and filters.
BLOCKSIZE = "64MB"

# How many parts the final Reduce is split into. 0 = a single output partition, i.e. one
# task holding every embedding: a memory ceiling and a serial tail, exactly like the
# `foldby` of the word count.
SPLIT_OUT = 8

# Words that carry no meaning and would drag every average towards the same point.
# Kept short and explicit on purpose: no extra dependency, and every entry is defensible.
STOPWORDS = frozenset("""
the of and in to a an is are was were be for on with by as at from that this these those
it its or not no but if then than so such can could may might will would shall should
has have had do does did we our you your they their he she his her i my me us am been
being into over under between during through about
""".split())

# A title word: at least two letters, no digits and no punctuation. The model is indexed
# by lowercase words, and `title_norm` is already lowercase.
WORD = re.compile(r"[a-z]{2,}")


# ----------------------------------------------------------------------------------
# Map: titles -> (paper, word)
# ----------------------------------------------------------------------------------


def read_titles(source, partitions=PARTITIONS):
    """The titles worth embedding, as a distributed DataFrame.

    Only two columns are read out of the twenty-three of `silver/papers`: Parquet is
    columnar, so the rest is never touched on disk. `title_ok` is the flag the silver
    step already computed (title present and at least three characters) - the task does
    not re-invent its own definition of "usable title".
    """
    papers = dd.read_parquet(source, columns=["cord_uid", "title_norm", "title_ok"])
    papers = papers[papers["title_ok"]]
    if partitions:
        papers = papers.repartition(npartitions=partitions)
    return papers[["cord_uid", "title_norm"]]


def tokenize(papers):
    """One row per (paper, word). -> DataFrame[cord_uid, word]

    `findall` + `explode` is the DataFrame way of writing a flatMap: the first gives a
    list of words per title, the second turns each element of the list into its own row.
    Both are narrow operations - no data moves between machines here.
    """
    tokens = papers.assign(word=papers["title_norm"].str.findall(WORD.pattern))
    tokens = tokens[["cord_uid", "word"]].explode("word")
    tokens = tokens.dropna(subset=["word"])
    tokens = tokens[~tokens["word"].isin(STOPWORDS)]
    # The join key is declared on BOTH sides (see `read_model`): `explode` returns an
    # object column while the CSV reader may return a string one, and joining two
    # different dtypes is what Dask warns about with "Cast dtypes explicitly to avoid
    # unexpected results" - the kind of warning that becomes an empty result.
    return tokens.astype({"word": "string"})


# ----------------------------------------------------------------------------------
# The model: read it in blocks, keep the words the titles use
# ----------------------------------------------------------------------------------


def read_model(path, blocksize=BLOCKSIZE):
    """The fastText `.vec` file as a distributed DataFrame of 1 + 300 columns.

    Four options that are not decoration:
      * `sep=" "`      the format is space separated, not comma separated;
      * `QUOTE_NONE`   some fastText words ARE quote characters (`"`), and would
                       otherwise swallow the rest of the file;
      * `keep_default_na=False` + `na_values=[""]`  the words "nan", "null" and "NA" are
                       real entries of the model and must stay strings; only a genuinely
                       empty field counts as missing (see the header row below);
      * `float32`      halves the memory of the 300 numeric columns.

    The first line of the file is the header `2000000 300`: two fields where the parser
    expects 301, so the other 299 arrive empty. We do NOT use `skiprows`, which under
    `blocksize` is a known way to silently drop the first line of EVERY block; with
    `na_values=[""]` those empty fields simply become NaN and the row survives as one
    junk record whose word is "2000000". `keep_vocabulary_words` then drops it for free,
    because "2000000" cannot be in a vocabulary made of `[a-z]{2,}` tokens.
    """
    import csv as csv_module

    return dd.read_csv(
        path,
        sep=" ",
        header=None,
        names=["word"] + VECTOR_COLUMNS,
        dtype={**{c: "float32" for c in VECTOR_COLUMNS}, "word": "string"},
        quoting=csv_module.QUOTE_NONE,
        keep_default_na=False,
        na_values=[""],
        encoding="utf-8",
        blocksize=blocksize,
    )


def keep_vocabulary_words(block, vocabulary):
    """Keep the rows of one model block whose word appears in some title.

    `vocabulary` is a `pyarrow.Array` built ONCE on the driver, and the test is the
    vectorized Arrow kernel `pc.is_in`. Written the obvious way - `block["word"].isin(
    python_set)` - pandas rebuilds that set in Arrow format ON EVERY BLOCK: same result,
    but seconds of GIL-bound work per task and a pile of short-lived objects that the
    allocator then refuses to give back. That is the whole story of
    `docs/MEMORY_LEAK_REPORT.md`, and this task has exactly the shape that triggered it.
    """
    import pyarrow as _pa
    import pyarrow.compute as pc

    words = _pa.array(block["word"], from_pandas=True)
    mask = pc.is_in(words, value_set=vocabulary).to_numpy(zero_copy_only=False)
    return block[mask]


def vocabulary_of(tokens):
    """The distinct title words, as a `pyarrow.Array` ready to be sent to the workers.

    This is the one collection that comes back to the driver, and it is allowed by the
    "no pandas on the driver" rule for the reason the rule gives: it is small (~10^5
    strings, a few MB) and there is no other way to filter the model without it.
    """
    words = tokens["word"].unique().compute()
    return pa.array(sorted(words), type=pa.string())


# ----------------------------------------------------------------------------------
# Reduce: one vector per paper
# ----------------------------------------------------------------------------------


def _to_mean(block):
    """sum / count, inside one partition of the already-aggregated result.

    Built with a single `concat` and not column by column: 300 successive insertions
    fragment the DataFrame and pandas rightly complains about it.
    """
    columns = [c for c in block.columns if c.startswith("v")]
    means = block[columns].div(block["n"], axis=0).astype("float32")
    head = pd.DataFrame({"cord_uid": block["cord_uid"],
                         "n_words": block["n"].astype("int32")})
    return pd.concat([head, means], axis=1)


def mean_pooling(tokens, model, split_out=SPLIT_OUT, broadcast=True):
    """Average the word vectors of each title. -> DataFrame[cord_uid, n_words, v0..v299]

    The join is inner: a word the model does not know simply disappears, which is what
    the assignment asks for.

    WHY `broadcast`. Joining on `word` normally means shuffling BOTH sides by word - and
    the left side, after the join, carries 300 floats per (paper, word) row: on the full
    corpus that is several GB crossing the network, and it also scatters the words of a
    paper over many partitions, so the groupby that follows has nothing left to reduce
    locally. Broadcasting sends the (small) filtered model to every worker instead: the
    join becomes local, the titles keep their original partitioning, and the `groupby`
    can sum a paper's words inside the partition BEFORE anything moves. Same trick, and
    the same reason, as the prefer-pmc join of the conversion step
    (PROJECT_CONTEXT.md §7, Atto 1). `broadcast=False` is kept as a knob because the
    difference between the two is worth measuring.

    The average is ONE `groupby.sum` plus a division, not a collected list of vectors:
    summing is associative, so Dask reduces inside each partition and then combines the
    partial sums (a tree reduction - the same shape as `foldby` on a Bag).

    `split_out` keeps the tail parallel: with a single output partition the last task
    holds every embedding at once.
    """
    merged = tokens.merge(model, on="word", how="inner", broadcast=broadcast)
    merged = merged.assign(n=np.float32(1.0))

    sums = merged.groupby("cord_uid")[VECTOR_COLUMNS + ["n"]].sum(
        split_out=split_out if split_out else 1)
    sums = sums.reset_index()

    # meta derived by running the same code on the empty frame: it cannot disagree with
    # what the function actually returns.
    return sums.map_partitions(_to_mean, meta=_to_mean(sums._meta))


def build(source, model_path, partitions=PARTITIONS, blocksize=BLOCKSIZE,
          split_out=SPLIT_OUT, broadcast=True):
    """The whole pipeline, up to the collection that still has to be computed.

    Returns (embeddings, vocabulary_size, n_partitions). NOTE that this is not fully
    lazy: the vocabulary has to exist on the driver before the model can be filtered, so
    computing it is a real, unavoidable first pass over the titles. It is the reason the
    task has two phases and not one.
    """
    papers = read_titles(source, partitions)
    tokens = tokenize(papers).persist()      # read twice: vocabulary, then the join

    vocabulary = vocabulary_of(tokens)

    model = read_model(model_path, blocksize)
    model = model.map_partitions(keep_vocabulary_words, vocabulary, meta=model._meta)

    embeddings = mean_pooling(tokens, model, split_out, broadcast)
    return embeddings, len(vocabulary), tokens.npartitions


# ----------------------------------------------------------------------------------


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("input", nargs="?", default=DEFAULT_INPUT,
                        help=f"silver/papers directory (default {DEFAULT_INPUT})")
    parser.add_argument("--model", default=DEFAULT_MODEL,
                        help=f"fastText .vec file (default {DEFAULT_MODEL})")
    parser.add_argument("--out", default=DEFAULT_OUTPUT,
                        help=f"output directory (default {DEFAULT_OUTPUT})")
    parser.add_argument("--partitions", type=int, default=PARTITIONS,
                        help="how many partitions to cut the titles into (0 = one per file)")
    parser.add_argument("--blocksize", default=BLOCKSIZE,
                        help=f"size of one model block (default {BLOCKSIZE})")
    parser.add_argument("--split-out", type=int, default=SPLIT_OUT,
                        help=f"output partitions of the Reduce (default {SPLIT_OUT}, 0 = one)")
    parser.add_argument("--no-broadcast", action="store_true",
                        help="join by shuffling both sides instead of broadcasting the model")
    return parser.parse_args()


def main():
    args = parse_args()

    # Relative paths are resolved against the REPOSITORY, not the current directory, so
    # that `python daniele/title_embeddings.py` and `cd daniele && python
    # title_embeddings.py` mean the same thing.
    repo = Path(__file__).resolve().parent.parent
    sys.path.insert(0, str(repo))
    from cluster import get_client   # imported here: see the note next to the imports

    source = Path(args.input).expanduser()
    source = source if source.is_absolute() else repo / source
    model_path = Path(args.model).expanduser()
    out = Path(args.out).expanduser()
    out = out if out.is_absolute() else repo / out

    if not source.exists():
        raise SystemExit(
            f"Input not found: {source}\n"
            "Pass the path of the silver dataset, for example:\n"
            "  python daniele/title_embeddings.py ~/mapd-data/silver/papers"
        )
    if not model_path.exists():
        raise SystemExit(
            f"Model not found: {model_path}\n"
            "The .vec file must exist AT THE SAME PATH ON EVERY WORKER: there is no\n"
            "shared file system any more, each machine reads it from its own disk."
        )
    out.mkdir(parents=True, exist_ok=True)

    client, cluster = get_client(repo_root=repo)
    print("input     :", source)
    print("model     :", model_path)
    print("output    :", out)

    # The workers must be able to import THIS module: in the graph we ship, functions
    # travel by name. -> PROJECT_CONTEXT.md §8.12a
    client.upload_file(__file__)
    # The embeddings are written by THE WORKERS, each on its own disk, while
    # `to_parquet` creates the directory only here on the client. -> §8.12b
    client.run(os.makedirs, str(out / "embeddings"), exist_ok=True)

    started = time.perf_counter()
    try:
        embeddings, n_vocabulary, n_partitions = build(
            source, model_path, args.partitions, args.blocksize, args.split_out,
            broadcast=not args.no_broadcast)
        print("partitions:", n_partitions)
        print("vocabulary:", f"{n_vocabulary:,} distinct title words")
        print("join      :", "shuffle" if args.no_broadcast else "broadcast of the model")
        print("reduce    :", f"groupby, split_out={args.split_out}"
              if args.split_out else "groupby into a SINGLE output partition")

        embeddings.to_parquet(out / "embeddings", write_index=False,
                              compression="zstd", overwrite=True)
        elapsed = time.perf_counter() - started

        # Read back one small column to say how much of the corpus got an embedding.
        # Cheap (columnar) and it checks the output actually exists.
        written = dd.read_parquet(out / "embeddings", columns=["n_words"])
        n_titles = len(written)
        n_matched = int(written["n_words"].sum().compute())
    finally:
        client.close()
        if cluster is not None:
            cluster.close()

    print(f"\ntitles embedded : {n_titles:,}")
    print(f"words matched   : {n_matched:,}")
    print(f"elapsed         : {elapsed:.1f} s")
    print("written:", out / "embeddings")


if __name__ == "__main__":
    main()
