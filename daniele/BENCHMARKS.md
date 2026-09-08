# Benchmarks for task 2.3.3 — how to run them

Three campaigns, one shared engine, one CSV. Each campaign answers a different question,
and every figure of the notebook is drawn from that single CSV.

| File | Question it answers |
|---|---|
| `bench_scaling.py` | How does time scale with **processes** and with the number of **partitions**? |
| `bench_threads.py` | What happens when tasks share a **process** — its GIL and its memory? |
| `bench_knobs.py` | The knobs that belong to **this** task: model blocksize, `split_out`, join strategy |
| `bench_common.py` | The engine. Not run directly: the three scripts import it |

**The rule that holds everything together: one row of the CSV = one measure = a brand new
cluster.** Every measure starts from freshly born workers, because on this cluster a
worker that has already chewed through millions of strings keeps RSS by allocator
fragmentation (`docs/MEMORY_LEAK_REPORT.md`). Reusing one would add wear to whatever we
are measuring.

Every measure records two numbers: the **seconds** and the **peak RSS of the busiest
worker** (`peak_gb`). The peak is not a bonus — it is what decides whether a
configuration finishes or dies with `KilledWorker`.

---

## 1 · Before anything: calibrate

**Never launch a whole campaign without knowing what one measure costs.** Run the
reference point three times: it gives both the time budget and the spread (the noise).

```bash
python daniele/bench_scaling.py ~/mapd-data/silver/papers --worker 3 --k 64 --repetitions 3
```

Multiply the time you see by the number of measures in the campaign (printed at startup,
~46 for a full pass of all three) and decide what fits in the night. If it does not fit,
trim the lists: every campaign takes `--worker`, `--k`, `--thread`, `--blocksize`,
`--split-out` as explicit lists.

## 2 · The three campaigns

```bash
# processes x partitions: the grid (24 points by default)
python daniele/bench_scaling.py ~/mapd-data/silver/papers

# threads: curve, equivalent arrangements, and the 4-thread partition curve (12 points)
python daniele/bench_threads.py ~/mapd-data/silver/papers

# the task's own knobs: blocksize, split_out, broadcast (10 points)
python daniele/bench_knobs.py ~/mapd-data/silver/papers
```

They all append to the **same** file, `~/mapd-out/bench-embeddings/measures.csv`, so they
can be run on different nights and the notebook still finds everything. Each one also
writes a Bokeh `report_<campaign>_<point>_<repetition>.html` per measure.

Run them **one at a time**: each starts its own cluster on port 8786, and two at once
would both fight for the port and share the workers' RAM, making the numbers meaningless.

## 3 · Options

Shared by all three:

| Option | Meaning |
|---|---|
| `input` (positional) | the `silver/papers` directory (default `~/mapd-data/silver/papers`) |
| `--model` | the `.vec` file — must exist **at the same path on every worker** |
| `--out` | where CSV and reports go (default `~/mapd-out/bench-embeddings`) |
| `--timeout` | ceiling in seconds for ONE measure (default 3600) |
| `--repetitions` | how many **whole passes** of the campaign (default 1) |

Specific to each:

```bash
bench_scaling.py  --worker 1 2 3 6      # how many worker processes to try
                  --k 8 16 32 64 128 256 # partitionings of the titles

bench_threads.py  --thread 1 2 4        # threads per worker
                  --worker 3            # processes the thread curve runs on
                  --k 16 32 64 128 256  # partitionings for the 4-thread curve
                  --group thread|slot|k_thread4   # run only one group

bench_knobs.py    --blocksize 16MB 32MB 64MB 128MB 256MB
                  --split-out 1 4 8 16
                  --worker 3  --thread 1
                  --group blocksize|split_out|broadcast
```

### Why `--repetitions` means *passes*, not repeats

Three repetitions are three **whole passes** of the campaign, not three consecutive
measures of the same point. It costs the same and says more: hours pass between two
repetitions of one point, so the spread includes how the machine varies during the day and
not just how two runs glued together differ. And if the night is interrupted, what is left
in hand is a **complete campaign** rather than half a curve measured three times.

## 4 · How the processes are placed

`cluster.txt` lists the **machines**. How many worker processes to put on them is decided
per measure by the campaign, through the `CORD19_HOSTS` environment variable — which
`cluster.py` reads *before* `cluster.txt`. **Nothing on disk is modified**, so a crashed
campaign cannot leave a corrupted `cluster.txt` behind.

With 3 machines of 2 cores each:

| `--worker` | Placement | Note |
|---|---|---|
| 1, 2, 3 | one process per machine | balanced: the classic speedup curve |
| 6 | two per machine | one process per **core** — the configuration that won the word count |
| 4, 5 | 2+1+1, 2+2+1 | **unbalanced**: Dask assumes equivalent workers, so these bend the curve for a reason that has nothing to do with the algorithm |

Memory is handled automatically: the share of a machine's RAM (85%) is **divided by how
many processes live on it**, so two workers on one machine get ~42% each instead of both
asking for 85% and being killed. The column `per_machine` in the CSV records it.

## 5 · Which figure comes from which campaign

The notebook (`task_2_3_3_title_embeddings.ipynb`, §7) reads the CSV and draws:

| Figure | Needs | From |
|---|---|---|
| §7.1 time vs partitions + memory wall | the grid, plus the 4-thread curve | `bench_scaling` + `bench_threads --group k_thread4` |
| §7.2 speedup and efficiency | the grid at one k | `bench_scaling` |
| §7.3 threads: time, and why memory does not multiply | the thread curve | `bench_threads --group thread` |
| §7.4 same slots as processes or as threads | the equivalence points | `bench_threads --group slot` |
| §7.5 blocksize / split_out / broadcast | the three knob groups | `bench_knobs` |

This is why the campaigns are split by **experiment** and not by figure: §7.1 and §7.2 are
two readings of the same grid, and measuring them separately would pay for the same points
twice.

## 6 · The CSV

One row per measure, in `measures.csv`:

| Column | Meaning |
|---|---|
| `campaign`, `point`, `repetition` | which measure this is |
| `worker`, `thread`, `slot` | what was **asked for** (`slot` = worker × thread) |
| `per_machine` | processes on the busiest machine |
| `k`, `blocksize`, `split_out`, `broadcast` | the configuration of the task |
| `seconds` | wall time of the whole task, vocabulary included |
| `peak_gb`, `mean_peak_gb` | peak RSS: busiest worker, and average over workers |
| `partitions`, `vocabulary` | what the run actually did |
| `real_workers`, `real_threads` | what the cluster actually **had** (if one died, it shows here) |
| `error` | empty when the measure completed |

**A row with an error is a datum, not a hole.** On the partition curve it is precisely
where the memory wall is, and the notebook draws it in red instead of dropping it.

## 7 · Downloading the results

The CSV, the HTML reports and the PNGs live on the **scheduler** (the driver writes them).
Only the parquet embeddings are scattered on the workers' disks.

```bash
# from the laptop: everything needed for the report, in one command
rsync -aP mapd-scheduler:~/mapd-out/bench-embeddings/ ./risultati/
```

## 8 · Practical notes

- Always run inside `tmux`: a campaign of hours must survive the laptop going to sleep.
- The startup block printed by `cluster.py` (`worker: N su M host [...]`) **is to be
  read**: it is the only defence against a run that quietly used one machine instead of
  three and looks merely slow.
- If a measure fails with `KilledWorker` at low `k`, that is not a bug to fix: the peak of
  one task goes as `1/k`, so it is the wall. Report it.
- `--timeout` exists for the night: a configuration that hangs must not eat the hours that
  are left. The default hour is generous — lower it once calibration says what is normal.
