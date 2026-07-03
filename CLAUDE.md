# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

Final project for **MAPD-B** (Management & Analysis of Physics Datasets, Module B — M.Sc. Physics of Data, UniPD). It is a **distributed-computing** exercise, not a software product: analyze the **CORD-19** COVID-19 research-paper corpus with **Dask** on a local machine (and, for delivery, a small multi-node cluster). The full spec is in `MAPD covid project.pdf`; course rules in `InstructionsAndGuidelines_MAPD2026_v1.pdf`.

The four assignment tasks: (1) distributed **word-count** over the papers' full body text (Map/Reduce, Bag-based); (2) most/least represented **countries & institutes** from author affiliations; (3) **title embeddings** with a pretrained FastText model; (4) **cosine similarity** between title pairs. Course rules require running on a **multi-node cluster** and providing **mandatory benchmarks** (execution time vs. number of partitions and number of workers) — an analysis without benchmarks is considered incomplete.

Not a git repo. No `requirements.txt`/`environment.yml` — the environment is a conda env (see below).

## Environment & commands

Everything runs in the conda env **`mapd-covid`** (Python 3.11). Recreate it with:

```bash
conda create -y -n mapd-covid -c conda-forge \
  python=3.11 dask distributed jupyterlab pandas matplotlib pyarrow bokeh python-graphviz ipykernel
```

Work is done in the notebook, opened either in **VS Code** (the Jupyter kernel is registered as `mapd-covid`) or via browser:

```bash
conda activate mapd-covid
jupyter lab            # then open 01_explore_schema.ipynb
```

Run a cell's logic outside the notebook (quick check) with the env's interpreter directly:
`/Users/federicosbarbati/miniconda/envs/mapd-covid/bin/python <script.py>`.

**Dask cluster lifecycle (important):** the notebook starts an in-process `LocalCluster` + `Client`; the Dask dashboard is on `localhost:8787`. Re-running the `cluster = LocalCluster(...)` cell **orphans** the previous cluster (variable gets reassigned, old cluster keeps holding 8787). Make the startup cell idempotent (`try: client.close(); cluster.close() except NameError: pass`) or "Restart Kernel". To force-clear a stuck cluster from the terminal (the scheduler lives inside the kernel process, so this also kills the kernel):

```bash
lsof -nP -iTCP:8787 -sTCP:LISTEN                 # see who holds the port
pkill -9 -f "envs/mapd-covid/bin/python"         # SIGTERM is caught — use -9
```

Note: running a Dask **Bag** in a plain `.py` script with **no active Client** falls back to the multiprocessing scheduler and fails with `BrokenProcessPool` unless the logic is under `if __name__ == "__main__":`. Inside the notebook (with a Client) this does not happen.

## Dataset architecture (`archive/`, ~28 GB)

This is the part that requires reading several files together. The dataset is **one catalog + two independent full-text sources**:

- **`metadata.csv`** — the catalog. ~425k rows (one paper, keyed by `cord_uid`), 19 columns. Holds bibliographic fields (`title`, `abstract`, `authors`, `journal`, `publish_time`, `doi`, `url`, `license`) that exist **even when no full-text JSON exists**.
- **`document_parses/pdf_json/`** — full text parsed from the paper's **PDF** (GROBID/S2ORC). File name = `<sha>.json` (the `sha` column, a 40-char sha1 of the PDF). ~151k files.
- **`document_parses/pmc_json/`** — full text parsed from the **PubMed Central XML** (NIH/NLM open archive; usually cleaner/more complete than the PDF parse). File name = `<pmcid>.xml.json` (the `pmcid` column). ~112k files.
- **`cord_19_embeddings/…csv`** (5.8 GB) — precomputed document embeddings (one row per paper); optional.

**Linking:** from a catalog row, the `pdf_json_files` / `pmc_json_files` columns give the relative path(s) to the JSON file(s); conversely a `pdf_json` filename stem == `sha`, and a `pmc_json` filename == `<pmcid>.xml.json`. A single `cord_uid` may reference **multiple** PDF parses (~7.7k rows have `;`-separated paths). Coverage reality: **~65% of rows (277k) have NO full-text JSON** — only ~148k papers have at least one parse.

Practical consequences: for **titles** (task 3/4), read them straight from `metadata.csv` — no need to open JSONs. For **body text** (task 1) you must read the JSON folders. When reading both folders, papers with both parses (105k) and multiple-PDF papers get **double-counted**; for per-paper analysis, dedup on `cord_uid` and prefer the `pmc_json` parse over `pdf_json`.

## Reading the JSON files (non-obvious gotchas)

- Each JSON file is **one pretty-printed (multi-line) JSON object**, NOT JSON-lines. Read whole files per element — `db.from_sequence(paths).map(load_json_file)` — **not** `db.read_text(...).map(json.loads)`, which splits by line and fails with `JSONDecodeError`.
- The bundled `json_schema.txt` (and the project PDF) **misleadingly** indent everything under `metadata`. In the actual files, `metadata` contains **only** `title` and `authors`; `abstract`, `body_text`, `bib_entries`, `ref_entries`, `back_matter` are **top-level** siblings. Full text is `record['body_text']`, not `record['metadata']['body_text']`.
- `pmc_json` files have **no `abstract`** key (only `pdf_json` do). Use `record.get('abstract', [])`. Author affiliations for task 2 are under `record['metadata']['authors'][i]['affiliation']`.
- Data is dirty: empty `authors`, missing `title`, malformed values are common — filter/clean in preprocessing.

## Reference

The course teaching repo lives next door at `../MAPD-B`. Its `dask/` module (README + `dask/notebooks/`) has the canonical Dask patterns: `Lecture2` uses **Bag**, `Lecture3` uses **DataFrame**. That repo's Docker Compose setup (`dask/docker-compose.yml`) is the template for the multi-node/benchmark phase — the Dask logic written here transfers unchanged; only the `Client(...)` target changes.
