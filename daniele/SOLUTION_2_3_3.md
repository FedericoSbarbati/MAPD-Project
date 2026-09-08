# Task 2.3.3 — Title embeddings

Solution of point **2.3.3 "Obtaining Embeddings for Paper Titles"**: the title of every
paper becomes a numerical vector, using the pre-trained fastText model
`crawl-300d-2M-subword.vec`.

| File | What it is |
|---|---|
| `title_embeddings.py` | **The source of truth**: the functions and the `main()`. This is what runs |
| `bench_scaling.py` · `bench_threads.py` · `bench_knobs.py` | The three benchmark campaigns, one per experiment. Instructions in `BENCHMARKS.md` |
| `bench_common.py` | The engine shared by the three campaigns (not run on its own) |
| `task_2_3_3_title_embeddings.ipynb` | The notebook: it **imports** the `.py`, explains, and draws the figures |
| `requirements.txt` | The versions used — all already present in `~/pyvenv` on the VMs |

Repository convention (`docs/DECISIONI.md`): **one `.py` with the functions, one notebook
that imports it**, never duplicated code between the two. On the cluster the `.py` is
launched from the terminal.

---

## 1 · How to run it

```bash
# on the scheduler, from the repository cloned in ~/MAPD-Project
source ~/pyvenv/bin/activate

# the task
python daniele/title_embeddings.py ~/mapd-data/silver/papers

# FIRST the calibration: three measures of one point, to learn what a measure costs
python daniele/bench_scaling.py ~/mapd-data/silver/papers --worker 3 --k 64 --repetitions 3

# then the three campaigns, one at a time, inside tmux (options in BENCHMARKS.md)
tmux new -s bench
python daniele/bench_scaling.py ~/mapd-data/silver/papers 2>&1 | tee ~/bench-scaling.log
python daniele/bench_threads.py ~/mapd-data/silver/papers 2>&1 | tee ~/bench-threads.log
python daniele/bench_knobs.py   ~/mapd-data/silver/papers 2>&1 | tee ~/bench-knobs.log
```

**Prerequisites on the machines** (they are not in the repository because they cannot be):

1. `cluster.txt` in the root of the repository, on the scheduler — it is git-ignored, so
   it is written there:
   ```
   10.67.22.144,10.67.22.162,10.67.22.225,10.67.22.237
   ```
   The **first host acts as scheduler only**, the other three are workers.
2. The model in `~/mapd-model/crawl-300d-2M-subword.vec` **on every machine that acts as
   a worker** (see section 3: there is no shared file system any more).

No other file of the repository needs to be modified.

## 2 · Structure and logical flow

```
Phase 1  silver/papers ──► title_ok filter ──► findall + explode ──► (cord_uid, word)
         (2 columns of 23)                     + stop-words                 │  [persist]
                                                                            │
                                        distinct words = VOCABULARY ◄───────┘
                                                    │ (~10⁵ strings, on the driver)
                                                    ▼
Phase 2  model.vec (4.5 GB) ──► dd.read_csv in blocks ──► keep only the vocabulary
                                                                            │
                        (cord_uid, word) ⋈ (word, v0..v299)   model broadcast
                                                                            │
                        groupby(cord_uid).sum() ──► ÷ n_words = MEAN ──► Parquet
```

The functions of the `.py` follow this order: `read_titles` → `tokenize` →
`vocabulary_of` → `read_model` → `keep_vocabulary_words` → `join_vectors` →
`mean_pooling`, and `build` puts them in a row.

**Why the mean (mean pooling).** The assignment allows both the list of vectors and
"aggregated into a single vector". The mean gives a **fixed-size** vector (300) per title,
which is the shape task 2.3.4 needs: the cosine similarity is computed between two
vectors, not between two lists of different length.

**What the output contains.** `cord_uid | n_words | v0..v299`, in zstd Parquet.
`n_words` is how many words of the title found a vector (the others are skipped, as the
assignment suggests). The **title is not there**: it lives in `silver/papers`, one join
away, and carrying it through the pipeline would have added a shuffle to every measured
run for no computational reason. Task 2.3.4 gets it back with
`dd.read_parquet(papers, columns=["cord_uid", "title", "is_title_unique"])` — a columnar
read, which costs little. It really does need `is_title_unique`: 122 thousand papers have
a duplicate title, and pairs with similarity 1 would be an artefact.

## 3 · The important question: how can workers with 3.8 GB use a 4.5 GB model?

**The model is never loaded.** The change of perspective is the whole point: it is not an
object to load into memory, it is **a dataset to read in pieces**, exactly like the data.

1. The `.vec` file is **text**: 2 million lines of `word v1 v2 ... v300`.
2. `dd.read_csv(..., blocksize="64MB")` cuts it into ~70 blocks. Each block is a task: the
   worker running it reads **only that block** from its own disk, parses it and — before
   the next one arrives — **filters** it, keeping only the rows whose word appears in some
   title. The peak per task is the size of one block, not of the file.
3. Of the 2 000 000 vectors only those of the title vocabulary survive (~10⁵): that slice
   (a few hundred MB, distributed) is the only thing that stays in memory.

The 7.2 GB `.bin` file **is not used**: that one really would have to be loaded whole into
RAM by the `fasttext` library, which is impossible on our workers. The `.vec`, being text
line by line, lends itself to distributed reading.

> ⚠️ **It is needed on every worker, at the same path.** With the current architecture
> there is no volume and no NFS any more: every machine reads from its own disk. If the
> model is only on the scheduler, the workers fail with `FileNotFoundError` — which is why
> the `.py` checks that the file exists and says so explicitly.

### The filter, and the link with MEMORY_LEAK_REPORT

The filter asks a question repeated ~70 times: *"is this word in the vocabulary?"*,
against a set of ~10⁵ strings. Written the obvious way —
`block["word"].isin(python_set)` — pandas **rebuilds the set in Arrow format on every
block**: same result, but seconds of GIL-bound work per task and a pile of temporary
objects that the allocator then keeps. It is **exactly** the hotspot root-caused in
`docs/MEMORY_LEAK_REPORT.md` (where the cure was worth ~170×).

The cure, adopted here: the vocabulary is converted **once, on the driver**, into a
`pyarrow.Array`, and every block is filtered with the vectorized kernel `pc.is_in`.

The rest of the recipe also comes from that report, but it already lives in `cluster.py`
and applies to every task: `MALLOC_TRIM_THRESHOLD_=0` and `MALLOC_ARENA_MAX=2` set
**before** any worker is born.

### The other memory decision: broadcasting the join

After the join every `(paper, word)` row carries **300 floats**. A normal join shuffles
both sides by the key `word`: on the full corpus that is several GB crossing the network,
and it also scatters the words of one paper across different partitions, so the `groupby`
that follows has nothing left to reduce locally.

Handing the filtered (small) model to every worker instead makes the join local, the
titles keep the partitioning they were read with, and the `groupby` can sum a paper's
words **inside its partition** before anything moves. It is the same trick, for the same
reason, as the prefer-pmc join of the conversion step (`PROJECT_CONTEXT.md` section 7,
Act 1, where a `merge` was rewritten as a broadcast). `--no-broadcast` remains as a knob
because the difference between the two is worth measuring.

## 4 · Benchmarks

Same method as the word count campaign, so that the results can sit in the same report:
**one row of the CSV = one measure = a brand new cluster**, and every point is the
reference with **one single knob turned**. Every measure starts from freshly born workers,
because on this cluster a worker that has already chewed through millions of strings keeps
RSS by fragmentation: reusing one would add wear to the partitioning being measured.

There are five knobs — the first two are the two **widths** of the graph:

| knob | what it changes | the boundary |
|---|---|---|
| `partitions` | how many parts the **titles** are cut into | few partitions = big tasks = memory peak |
| `blocksize` | in what blocks the **model** is read | the same, on the model side |
| `worker` | how many **processes** | speedup and efficiency |
| `thread` | how many tasks **inside** the same process | GIL + memory shared within the worker |
| `split_out` | how wide the **tail** of the Reduce is | `split_out=1` = one task holds everything |

plus a single point, `broadcast`, comparing the two join strategies.

The knobs do not live in one file: they are split into **three campaigns, one per
experiment** (`bench_scaling` the first two knobs plus the workers, `bench_threads` the
threads, `bench_knobs` the three choices specific to this task), with the shared engine in
`bench_common.py`. The split is by **experiment and not by figure**, because the figures
are different readings of the same table: the "time vs partitions" plot and the speedup
plot both come out of the worker × k grid, and measuring them separately would mean paying
for the same points twice. How to run them, with all the options: **`BENCHMARKS.md`**.

Two things are measured at every point: the **seconds** and the **peak RSS of the busiest
worker** (`peak_gb`, from `ru_maxrss`, the same tool as `Giulia/misura_ram.py`). The peak
is not an extra: it is what decides whether a configuration completes or dies with
`KilledWorker`, and on the word count curves it is half of the story.

The repetitions (`--repetitions 3`) are **whole passes**, not three consecutive measures of
the same point: hours pass between two repetitions, so the spread includes how the machine
varies during the day. And if the campaign is interrupted, what is left in hand is a
complete campaign rather than half a curve measured three times.

**Where everything ends up** (outside the repository, which on the cluster is disposable):

```
~/mapd-out/title_embeddings/embeddings/     the result (Parquet, on the workers' disks)
~/mapd-out/bench-embeddings/measures.csv    one row per measure  <- THIS is what to download
~/mapd-out/bench-embeddings/report_*.html   a Bokeh dashboard per measure
~/mapd-out/title_embeddings/bench_*.png     the figures, generated by the notebook
```

The notebook (section 7) reads `measures.csv` and draws, for every knob, time and peak RAM
side by side.

## 5 · Things to know / limits

- **The embeddings output is distributed**: the workers write it, each on its own disk, so
  the directory on the scheduler machine stays **empty**. That is the reason for the
  `client.run(os.makedirs, ...)` in the `.py`. What has to be downloaded to the laptop
  before the VMs are switched off is the **measures CSV** and the figures, which are a few
  MB; the embeddings themselves are needed by 2.3.4 and are best left where they are until
  that task runs.
- **The pipeline is not fully lazy**: the vocabulary has to exist on the driver before the
  model can be filtered, so there is a first pass over the titles that cannot be merged
  with the second. The benchmark stopwatch covers both, because that is what gets
  delivered.
- **Coverage is not 100%**: a title whose words are not in the model (typically a
  non-English title) produces no vector. The exact number is printed by the `.py`.
- **The stop-words are English only.** It is a choice already recorded in `DECISIONI.md`
  as a known limit common to the tasks; the agreed direction is to give German, French,
  Spanish and Portuguese their own lists, after the benchmarks.
- **`title_embeddings.py` imports nothing of the repository at top level**, and that is not
  a whim: it is shipped to the workers with `upload_file` and must be importable **on its
  own** (`PROJECT_CONTEXT.md` section 8.12a). `from cluster import get_client` lives inside
  `main()`.
- The pipeline was **verified numerically on the laptop** against a synthetic dataset: the
  distributed mean matches the one computed by hand in pandas (`atol 1e-5`), and the two
  join strategies give the same result row by row.
