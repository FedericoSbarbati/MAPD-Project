# CORD-19 Parquet datasets — data dictionary

Analysis-ready datasets derived from the full CORD-19 dump.
**Logical model:** relational/normalized, everything keyed on `cord_uid`.
**Physical:** partitioned Parquet (zstd), columnar → each task reads only the
columns it needs; benchmarks measure computation, not JSON parsing.

Two layers:
- **`bronze/`** — faithful extraction from JSON/CSV, structural gate only
  (unparseable file / missing key skipped). Reproducible raw.
- **`silver/`** — cleaned & canonicalized, **analysis-ready**. This is what
  the tasks read.

**Cleaning principle:** we fix objective errors (dirty country spellings,
whitespace, junk values) and **add flags**; we do **not** make a task's analysis
decision — duplicate titles are *flagged*, not removed; references are *flagged*,
not dropped; no tokenization/stopword removal.

**Where the data is:** `data/` on the Mac, `~/mapd-data/` on every cluster machine —
same content, the full corpus. It is **not regenerated**: the JSON→Parquet conversion
(`conversion_sanification.ipynb`) is a concluded phase and is not part of the assignment.

> All numbers below are **measured on the current datasets** (2026-08-13), not estimated.

---

## Which dataset for which task

| Task | Dataset | Columns to read |
|---|---|---|
| 2.3.1 — word-count (body text) | `silver/paragraphs` | `text` (optionally filter `~is_reference_like`) |
| 2.3.2 — countries & institutes | `silver/paper_countries` / `silver/paper_institutions` (per-paper) or `silver/authors` (per-author) | `country` / `institution_norm` |
| 2.3.3 / 2.3.4 — title embeddings & cosine | `silver/papers` | `cord_uid`, `title`, `title_norm`, `is_title_unique` |

```python
import dask.dataframe as dd, pandas as pd
# task 2.3.1
dd.read_parquet("data/silver/paragraphs", columns=["cord_uid", "text"])
# task 2.3.2 (per-paper country counts)
pd.read_parquet("data/silver/paper_countries").country.value_counts()
# task 2.3.3 / 2.3.4
pd.read_parquet("data/silver/papers", columns=["cord_uid", "title", "is_title_unique"])
```

## Size at a glance

| Dataset | Rows | Files | Size |
|---|---:|---:|---:|
| `bronze/papers` | 1,056,660 | 9 | 0.60 GB |
| `bronze/paragraphs` | 23,110,668 | 1024 | 4.39 GB |
| `bronze/authors` | 2,943,737 | 192 | 0.03 GB |
| `silver/papers` | 970,836 | 9 | 0.64 GB |
| `silver/paragraphs` | 12,445,234 | 1979 | 3.56 GB |
| `silver/authors` | 2,943,737 | 192 | 0.04 GB |
| `silver/paper_countries` | 284,042 | 64 | — |
| `silver/paper_institutions` | 517,911 | 48 | — |

> ⚠️ **Files ≠ partitions.** `dd.read_parquet("silver/paragraphs")` yields **990**
> partitions from 1979 files: the optimizer coalesces small ones. Nothing is missing —
> but don't confuse the two numbers when reading a benchmark.

---

## `silver/papers` — 970,836 rows (1 per paper), 9 files
Grain: one row per unique `cord_uid`, deduped from the 1,056,660 metadata rows,
preferring the row that has a full-text parse. Source: `metadata.csv`.

| column | type | notes |
|---|---|---|
| `cord_uid` | string | **PK**, unique |
| `title` | string | 482 null; already whitespace-clean |
| `abstract` | string | 21.35% null; whitespace-collapsed |
| `year` | int16 | from `publish_time[:4]`; 0.19% null. Corpus peaks in **2021 (400,457)**, then 2020 (343,005) and 2022 (128,081) |
| `has_pdf`, `has_pmc` | bool | a pdf / pmc full-text parse exists: 373,750 / 315,726 |
| `title_norm` | string | lower + whitespace-collapsed; key for matching (null if no title) |
| `title_dup_count` | int32 | # papers sharing this `title_norm`; max **82** |
| `is_title_unique` | bool | `title_dup_count == 1`. **272,191 papers (28.0%) have a non-unique title** — relevant for cosine similarity (2.3.4): exact-duplicate title pairs are trivially similar |
| `title_ok` | bool | title present and ≥3 chars: 99.95% |
| `doi`,`pmcid`,`pubmed_id`,`s2_id`,`url`,`license`,`source_x`,`journal`,`authors`,`sha`,`publish_time`,`pdf_json_files`,`pmc_json_files` | string | passthrough bibliographic fields |

Dropped dead columns: `mag_id` (100% null), `arxiv_id` (~99%), `who_covidence_id` (~60%).
`authors` here is the raw `"Last, First; ..."` string and covers **all** papers
(unlike the `authors` table, which is pdf-only).

## `silver/paragraphs` — 12,445,234 rows (1 per paragraph), 1979 files, 3.56 GB
Grain: one paragraph of body text. Source: `pmc_json` **preferred** over `pdf_json`
per paper (pmc is cleaner and always present); a paper never keeps both sources —
9,239,565 rows come from pmc, 3,205,669 from pdf. Covers **389,859 papers**.

| column | type | notes |
|---|---|---|
| `cord_uid` | string | FK → papers |
| `paper_id` | string | sha (pdf) or pmcid (pmc) — provenance |
| `source` | string | `'pmc'` or `'pdf'` |
| `para_idx` | int32 | paragraph position within the document |
| `section` | string | **raw & very dirty** (free-text misparses) — do not use as a category |
| `text` | string | paragraph text, verbatim (no tokenization applied). **Never null** |
| `is_reference_like` | bool | heuristic flag for references/acknowledgements/funding/conflict sections: 235,914 rows = **1.90%** — excluded by default in the word count |

Citation offsets (`cite_spans`/`ref_spans`) were intentionally dropped (no task uses them).
`bronze/paragraphs` (23,110,668 rows) keeps **both** sources, if you need the pdf parse.

## `silver/authors` — 2,943,737 rows (1 per author per paper), 192 files
Grain: one row per (paper, author). Source: **`pdf_json` only** — pmc affiliations
are 0% populated. Rows are kept even without affiliation (columns null).

| column | type | notes |
|---|---|---|
| `cord_uid` | string | FK → papers |
| `paper_id` | string | sha — provenance |
| `author_idx` | int32 | author position |
| `institution` | string | raw affiliation institution (**51.68%** non-null) |
| `institution_norm` | string | whitespace/punctuation-normalized (light only — see limitations) |
| `country_raw` | string | raw affiliation country (**46.01%** non-null; dirty) |
| `country_iso3` | string | canonical ISO3 (via `country_converter` + alias patch); **99.33%** of non-null raw resolved, else null |
| `country` | string | canonical country short name |
| `settlement` | string | affiliation city, if present |

## `silver/paper_countries` — 284,042 rows (rollup), distinct `(cord_uid, country_iso3, country)`
Distinct country per paper (co-authors from the same country counted once).
**214,799 papers.** Use for **per-paper** country counts (2.3.2).

## `silver/paper_institutions` — 517,911 rows (rollup), distinct `(cord_uid, institution_norm)`
Distinct institution per paper. **237,184 papers.** Use for per-paper institute counts.

---

## Integrity guarantees (verified on the full corpus)
- `papers.cord_uid` is unique; `authors`/`paragraphs`/rollups `.cord_uid` ⊆ `papers.cord_uid`.
- `paragraphs`: no paper keeps both `pdf` and `pmc` sources (prefer-pmc).
- rollups: `(cord_uid, country_iso3)` / `(cord_uid, institution_norm)` are unique.
- `paragraphs.text` and `paragraphs.cord_uid` are never null.

## Known limitations (deliberately left to the tasks / out of scope)
- **Institution disambiguation is only light-normalized** (105,967 distinct strings,
  39.23% singletons; abbreviations like `CAS`, address fragments remain). Full entity
  resolution (ROR/GRID) is a research problem — group on `institution_norm` with care.
- **`section` is not canonicalizable**; only `is_reference_like` is provided.
- **Country**: 0.67% of non-null `country_raw` stays unresolved (`country_iso3` null) —
  long-tail junk; `country_raw` is kept for inspection. coco's regex is greedy, so rare
  false positives are possible.
- **Duplicate titles / near-duplicate papers** are flagged (`is_title_unique`) but not removed.
- Multi-country affiliation strings resolve to the **first** recognized country.
