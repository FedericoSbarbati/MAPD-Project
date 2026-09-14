"""Task 2.3.3 - title embeddings with a pre-trained fastText model.

    Map phase     every title becomes a list of words; every word that the model
                  knows becomes a 300-dimensional vector
    Reduce phase  for every paper, average the vectors of its words -> ONE vector
                  per title (mean pooling)

Where the computation happens is decided by cluster.txt / env vars, never by editing the code
(see cluster.py).

    python daniele/title_embeddings.py                        # sample data, local cluster
    python daniele/title_embeddings.py ~/mapd-data/silver/papers

The model is NEVER loaded in memory. It is a 4.5 GB text file (`word v1 ... v300`)
and it is read like any other dataset: `dd.read_csv` cuts it into blocks,
each worker parses its own blocks and immediately throws away the words that no title
uses. Only that small surviving slice stays in the cluster, which is what lets workers
with less RAM than the file use it.
"""

import argparse
from ast import Add
import os
import re
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import dask
import dask.dataframe as dd

# NOTHING outside the standard library, Dask and the array libraries is imported here:
# this module has to be IMPORTABLE
# ON THE SCHEDULER AND ON THE WORKERS, which know nothing about this repository.
# `from cluster import get_client` would break that,
# so it lives inside `main()`.

DEFAULT_INPUT = "data_sample/silver/papers"          # no arguments -> try on the sample
DEFAULT_MODEL = "~/mapd-model/crawl-300d-2M-subword.vec"
DEFAULT_OUTPUT = "~/mapd-out/title_embeddings"       # outside the repo: it is disposable

N_DIM = 300                  # fastText vector size, fixed by the model
VECTOR_COLUMNS = [f"v{i}" for i in range(N_DIM)]

# The dtype of the join key, declared once and used on BOTH sides. Saying "string" is not
# enough: pandas has two storages behind that name (python and pyarrow), and
# joining one against the other makes Dask warn about a dtype mismatch
WORD_DTYPE = "string[pyarrow]"

# How wide the Map is: how many partitions the titles are cut into. 0 = leave whatever
# the Parquet reader gives.
#
# 64 by default, and the reason is memory. The join adds 300 float32 to every row
# (1.2 kB), so the ~7.8 M (paper, word) pairs of the full corpus weigh ~9.3 GB in total.
# The peak of ONE task is that total divided by the number of partitions, and every
# thread of a worker holds one task at a time.
#
# Measured on the cluster (3 workers x 2 threads, 3.5 GB each): leaving the reader's own
# partitioning gave THREE partitions - 3.1 GB per task, 6.2 GB per worker - and every
# worker was killed. At 64 it is 0.15 GB per task.
PARTITIONS = 64

# How the model file is cut. Second width knob, and the one specific to this
# task: a block is what ONE task reads, parses and filters.
BLOCKSIZE = "64MB"

# How many parts the final Reduce is split into. 0 = a single output partition, i.e. one
# task holding every embedding
SPLIT_OUT = 8

# Words that carry no meaning and would drag every average towards the same point.
STOPWORDS = frozenset("""
the of and in to a an is are was were be for on with by as at from that this these those
it its or not no but if then than so such can could may might will would shall should
has have had do does did we our you your they their he she his her i my me us am been
being into over under between during through about
""".split())


# WORD is a compiled regular expression object in Python (created using the `re` module).
# Uused to search for and find text strings containing sequences of
# at least two consecutive lowercase letters (from 'a' to 'z').
# Will be usefull to isolate single words in titles
WORD = re.compile(r"[a-z]{2,}")


# ----------------------------------------------------------------------------------
# Map: titles -> (paper, word)
# ----------------------------------------------------------------------------------


def read_titles(source, partitions=PARTITIONS):
    """
    Only two columns are read out of the twenty-three of `silver/papers`: Parquet is
    columnar, so the rest is never touched on disk. `title_ok` is the flag the silver
    step already computed (title present and at least three characters) - the task does
    not re-invent its own definition of "usable title".
    """
    papers = dd.read_parquet(source, columns=["cord_uid", "title_norm", "title_ok"])
    """
    Reads the Parquet file in a distributed manner
    Does not read the data yet, it only creates a computational graph (a "plan of what to do")
    Returns a dask.dataframe.DataFrame
    """
    papers = papers[papers["title_ok"]]
    if partitions:
        papers = papers.repartition(npartitions=partitions) #Add a repartitioning step: "when executing,
                                                            # split the result into 64 partitions."
                                                            #still lazy
    return papers[["cord_uid", "title_norm"]] #filter the columns of interest


def tokenize(papers):
    """One row per (paper, word). -> DataFrame[cord_uid, word]

    `findall` + `explode` is the DataFrame way of writing a flatMap: the first gives a
    list of words per title, the second turns each element of the list into its own row.
    Both are narrow operations - no data moves between machines here.
    """
    tokens = papers.assign(word=papers["title_norm"].str.findall(WORD.pattern)) #select all single words
                                                                                #inside papers[title_norm]
                                                                                #still lazy, add a
                                                                                #column to papers
                                                                                #with that list
    tokens = tokens[["cord_uid", "word"]].explode("word")     #select columns separated by words:
                                                              #(paper1, ["clinical", "features"])
                                                              #output: (paper1, "clinical"), (paper1, "features")
    tokens = tokens.dropna(subset=["word"])     #removes rows where word is NaN
    tokens = tokens[~tokens["word"].isin(STOPWORDS)]        #is this word in STOPWORDS? filter
    return tokens.astype({"word": WORD_DTYPE})          #put every word WORD_DTYPE, returns still a graph


# ----------------------------------------------------------------------------------
# The model: read it in blocks, keep the words the titles use
# ----------------------------------------------------------------------------------


def read_model(path, blocksize=BLOCKSIZE):
    """The fastText `.vec` file as a distributed DataFrame of 1 + 300 columns.

    Four options that are not decoration:
      * `sep=" "`      space separated, not comma separated;
      * `QUOTE_NONE`   some fastText words ARE quote characters (`"`);
      * `keep_default_na=False` + `na_values=[""]`  the words "nan", "null" and "NA" are
                       real entries of the model and must stay strings; only a genuinely
                       empty field counts as missing (see the header row below);
      * `float32`      halves the memory of the 300 numeric columns.
      * `names` = assign names: ["word", "v0", "v1", ..., "v299"] — 301 total columns
      * `dtypes` = specifies type of every columns
      * `blocksize` = Dask sends every block to a different worker, so the parsing is done in parallel

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
        dtype={**{c: "float32" for c in VECTOR_COLUMNS}, "word": WORD_DTYPE},
        quoting=csv_module.QUOTE_NONE,
        keep_default_na=False,
        na_values=[""],
        encoding="utf-8",
        blocksize=blocksize,
    )


def keep_vocabulary_words(block, vocabulary):
    """Keep only the rows of this block whose word appears in the title vocabulary.

    The vocabulary is a PyArrow Array built ONCE on the driver (before this function
    is called). We pass it to every worker as an argument. This function runs on each
    block independently.

    Why this specific approach?
    Naive way - block["word"].isin(python_set) - rebuilds that set FROM SCRATCH on
    every single block, in Python (GIL-locked), creating temporary objects that the
    allocator refuses to free. This approach uses a vectorized Arrow kernel instead,
    which is C++ code and releases memory correctly. set lives in the driver, sent to
    the workers when needed and then discarded correctly.
    """
    import pyarrow as _pa
    import pyarrow.compute as pc

    words = _pa.array(block["word"], from_pandas=True)
    mask = pc.is_in(words, value_set=vocabulary).to_numpy(zero_copy_only=False)
    return block[mask] #Note that this functions is not lazy:operations are done immediately
                       #the argumentsin this functions are real Pandas datframes, not Dask DataFrames


def vocabulary_of(tokens):
    """Build the distinct title words as a PyArrow Array (vocabulary of above function)


    Execution: when this function is called, it immediately runs the entire graph:
    - Dask sends the graph to all workers
    - Each worker finds the unique words in its partitions
    - Results come back to the driver
    - This function returns a PyArrow Array

    Result stays on the driver, in RAM. It is small (~10 MB, about 161,901 strings).
    How is it used? The vocabulary is passed to keep_vocabulary_words() as an argument
    via map_partitions(). Dask copies this PyArrow Array to every worker, and each
    worker uses it to filter its own block of the model.
    """
    words = tokens["word"].unique().compute()
    return pa.array(sorted(words), type=pa.string())


# ----------------------------------------------------------------------------------
# Reduce: one vector per paper
# ----------------------------------------------------------------------------------


def _to_mean(block):
    """Turn sums into means: divide each vector sum by its word count.

    Input: one partition of grouped-and-summed data. One row per paper.
    - cord_uid: paper ID
    - v0..v299: SUM of word vectors for that paper (not yet divided)
    - n: how many words this paper had (came from summing column of 1.0s)

    Output: same row, but v0..v299 now contain the MEAN vector.
    """
    # Get the 300 vector columns (all columns starting with "v").
    columns = [c for c in block.columns if c.startswith("v")]

    # Divide each vector sum by the word count to get the mean.
    # axis=0 means each row is divided by its own n value.
    means = block[columns].div(block["n"], axis=0).astype("float32")

    # Prepare the metadata: cord_uid and the word count (renamed, cast to int).
    head = pd.DataFrame({"cord_uid": block["cord_uid"],
                         "n_words": block["n"].astype("int32")})

    # Glue metadata and means side by side into one DataFrame.
    # It's all Pandas now, no Dask, so this is immediate.
    return pd.concat([head, means], axis=1)


def _join_block(block, table):
    """Attach the 300-dimensional word vector to each (paper, word) pair.

    Input:
    - block: one partition of tokens. Rows are (cord_uid, word).
    - table: the filtered fastText model. Rows are (word, v0..v299).
      Already loaded in this worker's memory via broadcast.

    Output: rows are (cord_uid, word, v0..v299).
    Keeps only words the model knows (inner join drops the rest).

    No data moves between workers: both inputs are already here.
    It's just a local pandas merge.
    """
    # Join on the word column: keep rows where word exists in the model.
    return block.merge(table, on="word", how="inner")


def join_vectors(tokens, model, broadcast=True, client=None):
    """Attach the model vectors to every (paper, word) pair. -> DataFrame[cord_uid, word, v0..v299]

    `_join_block` is the helper that does the actual merge locally, on one worker, between
    a partition of tokens and the model table. This function decides the STRATEGY for
    getting the model to the workers in the first place. Two ways, picked with `broadcast`.

    broadcast=True (default). The filtered model is small - 87,774 words, 0.11 GB. So we:
    1. Calculate it ONCE on the driver: model.compute()
    2. Send a copy of the WHOLE table to EVERY worker: client.scatter(..., broadcast=True)
    3. Every partition joins locally with _join_block: no shuffle, no data moves between workers
    The groupby that comes next can sum a paper's words inside its own partition.

    We deliberately do NOT use merge(..., broadcast=True), which lets Dask decide. Measured
    on the cluster: Dask made every output partition depend on the whole model side and
    refused to start with "8.04 GiB worth of input dependencies" against a 3.25 GiB worker
    - about thirty times the real size. Doing the broadcast ourselves makes the cost known:
    one copy of 0.11 GB per worker, once.

    broadcast=False is the textbook alternative, kept as a benchmark knob: a real
    distributed join. It shuffles BOTH sides by "word" - that means rearranging all data
    so every worker gets ALL rows with word="patient", ALL rows with word="study", etc.
    The model is calculated later, as part of the final compute(), not now. The shuffle
    moves several GB across the network, which is why the broadcast version is faster.
    """
    if not broadcast:
        return tokens.merge(model, on="word", how="inner")

    table = model.compute()          # small and motivated: it is what the filter is for
    print(f"model kept : {len(table):,} words, "
          f"{table.memory_usage(deep=True).sum() / 1e9:.2f} GB to every worker")
    handle = client.scatter(table, broadcast=True) if client is not None else table
    return tokens.map_partitions(_join_block, handle,
                                 meta=_join_block(tokens._meta, model._meta))


def mean_pooling(merged, split_out=SPLIT_OUT):
    """Average the word vectors of each title. -> DataFrame[cord_uid, n_words, v0..v299]

    This NEVER collects a list of vectors into memory. Instead:
    1. Sum all word vectors for each paper (associative, can be split across partitions)
    2. Count the words for each paper (add a column of 1.0, then sum it)
    3. Divide at the end, in _to_mean (because mean is NOT associative)

    Why this way? If you collected [v1, v2, v3] into a list before averaging, the list
    sits in one worker's memory. For a paper with 100 words, that's 100 × 300 floats.
    With 969k papers, some have many words — this would blow up memory. Summing instead
    lets Dask reduce INSIDE each partition, then combine the partial sums (tree reduction),
    never materializing the full list.

    The column `n` carries the word count: assign(n=1.0) adds 1.0 to every row, so when
    you sum it, you get the count for free. The count travels IN THE SAME groupby as the
    vectors, so one reduction instead of sum + count + join.

    `split_out` is how many partitions the result comes out in. With 1, a single task
    holds every embedding at once (memory ceiling). With 8 (default), 8 tasks work in
    parallel on different papers.
    """
    # Add a column of 1.0 to every row. When we sum this column, it counts the words.
    merged = merged.assign(n=np.float32(1.0))

    # Group by paper, sum all 300 vectors PLUS the count column, in split_out partitions.
    # This is a tree reduction: Dask sums inside each partition first, then combines
    # the partial sums. The mean (division) waits until _to_mean.
    sums = merged.groupby("cord_uid")[VECTOR_COLUMNS + ["n"]].sum(
        split_out=split_out if split_out else 1)
    sums = sums.reset_index()

    # Derive the schema of the output by running _to_mean on an empty DataFrame.
    # This way the schema cannot disagree with what the function actually returns.
    # The result is still lazy: map_partitions does not execute yet.
    return sums.map_partitions(_to_mean, meta=_to_mean(sums._meta))


def build(source, model_path, partitions=PARTITIONS, blocksize=BLOCKSIZE,
          split_out=SPLIT_OUT, broadcast=True, client=None):
    """Assemble the whole pipeline. -> (embeddings, vocabulary_size, n_partitions)

    `embeddings` is still a graph: nothing has been written yet, the caller decides when
    to compute it.

    The pipeline is NOT fully lazy, and this function is where that shows. Two things
    have to exist for real before the graph can be finished:
      1. the vocabulary, because the model cannot be filtered without it;
      2. with broadcast=True, the filtered model itself, because it has to be handed to
         the workers.
    Each of them is a real pass over the data. That is why `tokens` is persisted: it is
    read twice, once for the vocabulary and once for the join.
    """
    papers = read_titles(source, partitions)
    tokens = tokenize(papers).persist()      # read twice: vocabulary, then the join

    vocabulary = vocabulary_of(tokens)

    model = read_model(model_path, blocksize)
    model = model.map_partitions(keep_vocabulary_words, vocabulary, meta=model._meta)

    merged = join_vectors(tokens, model, broadcast, client)
    embeddings = mean_pooling(merged, split_out)
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
    from cluster import get_client   # not at the top: see the note next to the imports

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
    # travel BY NAME, and the code only exists on the machine we launch from.
    client.upload_file(__file__)
    # The output directory has to exist ON EVERY MACHINE: `to_parquet` creates it here on
    # the client, but the workers are the ones writing, each on its own disk.
    client.run(os.makedirs, str(out / "embeddings"), exist_ok=True)

    started = time.perf_counter()
    try:
        embeddings, n_vocabulary, n_partitions = build(
            source, model_path, args.partitions, args.blocksize, args.split_out,
            broadcast=not args.no_broadcast, client=client)
        print("partitions:", n_partitions)
        print("vocabulary:", f"{n_vocabulary:,} distinct title words")
        print("join      :", "shuffle" if args.no_broadcast else "broadcast of the model")
        print("reduce    :", f"groupby, split_out={args.split_out}"
              if args.split_out else "groupby into a SINGLE output partition")

        # The write and the two coverage numbers are asked for TOGETHER, in one
        # `dask.compute`: it merges the graphs, so the pipeline runs once and both
        # consumers read the same result.
        #
        # The obvious alternative - write, then read the output back - CANNOT work here:
        # the workers wrote their parts on their own disks, and the folder on this machine
        # stays empty because no worker runs here. Asking the driver to read it gives
        # "No files satisfy the parquet_file_extension criteria" AFTER a perfectly good
        # run. Measured: 8 parts, 1.07 GB, spread over the three workers.
        write = embeddings.to_parquet(out / "embeddings", write_index=False,
                                      compression="zstd", overwrite=True, compute=False)
        _, n_titles, n_matched = dask.compute(write,
                                              embeddings["n_words"].count(),
                                              embeddings["n_words"].sum())
        elapsed = time.perf_counter() - started
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
