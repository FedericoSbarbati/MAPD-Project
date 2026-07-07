# CORD-19 Parquet datasets — data dictionary

Analysis-ready datasets derived from the raw CORD-19 dump (`archive/`).
**Logical model:** relational/normalized, everything keyed on `cord_uid`.
**Physical:** partitioned Parquet (zstd), columnar → each task reads only the
columns it needs; benchmarks measure computation, not JSON parsing.

Two layers:
- **`data/bronze/`** — faithful extraction from JSON/CSV, structural gate only
  (unparseable file / missing key skipped). Reproducible raw.
- **`data/silver/`** — cleaned & canonicalized, **analysis-ready**. This is what
  the tasks should read.

**Cleaning principle:** we fix objective errors (dirty country spellings,
whitespace, junk values) and **add flags**; we do **not** make a task's analysis
decision — duplicate titles are *flagged*, not removed; references are *flagged*,
not dropped; no tokenization/stopword removal.

Build: `python build_parquet.py --run` (bronze) → `python build_silver.py` (silver).

---

## Which dataset for which task

| Task | Dataset | Columns to read |
|---|---|---|
| 1 — word-count (body text) | `silver/paragraphs` | `text` (optionally filter `~is_reference_like`) |
| 2 — countries & institutes | `silver/paper_countries` / `silver/paper_institutions` (per-paper) or `silver/authors` (per-author) | `country` / `institution_norm` |
| 3/4 — title embeddings & cosine | `silver/papers` | `cord_uid`, `title`, `title_norm`, `is_title_unique` |

```python
import dask.dataframe as dd, pandas as pd
# task 1
dd.read_parquet("data/silver/paragraphs", columns=["cord_uid", "text"])
# task 2 (per-paper country counts)
pd.read_parquet("data/silver/paper_countries").country.value_counts()
# task 3/4
pd.read_parquet("data/silver/papers", columns=["cord_uid", "title", "is_title_unique"])
```

---

## `silver/papers` — 406,211 rows (1 per paper), 9 parts
Grain: one row per unique `cord_uid` (deduped from 425,796 metadata rows,
preferring the row that has a full-text parse). Source: `metadata.csv`.

| column | type | notes |
|---|---|---|
| `cord_uid` | string | **PK**, unique |
| `title` | string | 216 null; already whitespace-clean |
| `abstract` | string | ~28% null; whitespace-collapsed |
| `year` | int16 | from `publish_time[:4]`; null if non-numeric; corpus peaks 2020 (300,459) |
| `has_pdf`, `has_pmc` | bool | whether a pdf/pmc full-text parse exists |
| `title_norm` | string | lower + whitespace-collapsed; key for matching (null if no title) |
| `title_dup_count` | int32 | # papers sharing this `title_norm` |
| `is_title_unique` | bool | `title_dup_count == 1`. **122,390 papers have a non-unique title** (max 44) — relevant for cosine (task 4): exact-dup title pairs are trivially similar |
| `title_ok` | bool | title present and ≥3 chars |
| `doi`,`pmcid`,`pubmed_id`,`s2_id`,`url`,`license`,`source_x`,`journal`,`authors`,`sha`,`publish_time`,`pdf_json_files`,`pmc_json_files` | string | passthrough bibliographic fields |

Dropped dead columns: `mag_id` (100% null), `arxiv_id` (~99%), `who_covidence_id` (~60%).
`authors` here is the raw `"Last, First; ..."` string and covers **all** papers
(unlike the `authors` table, which is pdf-only).

## `silver/paragraphs` — 4,719,311 rows (1 per paragraph), 49 parts, 1.37 GB
Grain: one paragraph of body text. Source: `pmc_json` **preferred** over `pdf_json`
per paper (pmc is cleaner and always present); a paper never keeps both sources.
Covers 148,638 papers.

| column | type | notes |
|---|---|---|
| `cord_uid` | string | FK → papers |
| `paper_id` | string | sha (pdf) or pmcid (pmc) — provenance |
| `source` | string | `'pmc'` or `'pdf'` |
| `para_idx` | int32 | paragraph position within the document |
| `section` | string | **raw & very dirty** (~1M distinct, free-text misparses) — do not use as a category |
| `text` | string | paragraph text, verbatim (no tokenization applied) |
| `is_reference_like` | bool | heuristic flag for references/acknowledg/funding/etc. sections (1.68% of rows) — optionally exclude for word-count |

Citation offsets (`cite_spans`/`ref_spans`) were intentionally dropped (no task uses them).
`bronze/paragraphs` (8,075,476 rows, 2.35 GB) keeps **both** sources, if you need the pdf parse.

## `silver/authors` — 1,019,793 rows (1 per author per paper), 12 parts
Grain: one row per (paper, author). Source: **`pdf_json` only** — pmc affiliations
are 0% populated. Rows are kept even without affiliation (columns null).

| column | type | notes |
|---|---|---|
| `cord_uid` | string | FK → papers |
| `paper_id` | string | sha — provenance |
| `author_idx` | int32 | author position |
| `institution` | string | raw affiliation institution (~52.8% non-null) |
| `institution_norm` | string | whitespace/punctuation-normalized (light only — see limitations) |
| `country_raw` | string | raw affiliation country (~45.3% non-null; dirty) |
| `country_iso3` | string | canonical ISO3 (via `country_converter` + alias patch); **99.12%** of non-null raw resolved, else null |
| `country` | string | canonical country short name |
| `settlement` | string | affiliation city, if present |

## `silver/paper_countries` — 102,431 rows (rollup), distinct `(cord_uid, country_iso3, country)`
Distinct country per paper (co-authors from the same country counted once).
78,963 papers. Use for **per-paper** country counts (task 2).

## `silver/paper_institutions` — 184,818 rows (rollup), distinct `(cord_uid, institution_norm)`
Distinct institution per paper. 90,115 papers. Use for per-paper institute counts.

---

## Integrity guarantees (verified on the full corpus)
- `papers.cord_uid` is unique; `authors`/`paragraphs`/rollups `.cord_uid` ⊆ `papers.cord_uid`.
- `paragraphs`: no paper keeps both `pdf` and `pmc` sources (prefer-pmc).
- rollups: `(cord_uid, country_iso3)` / `(cord_uid, institution_norm)` are unique.

## Known limitations (deliberately left to the tasks / out of scope)
- **Institution disambiguation is only light-normalized** (49,824 distinct strings,
  38.9% singletons; abbreviations like `CAS`, address fragments remain). Full entity
  resolution (ROR/GRID) is a research problem — group on `institution_norm` with care.
- **`section` is not canonicalizable** (~1M distinct); only `is_reference_like` is provided.
- **Country**: 0.88% of non-null `country_raw` stays unresolved (`country_iso3` null) —
  long-tail junk; `country_raw` is kept for inspection. coco's regex is greedy, so rare
  false positives are possible.
- **Duplicate titles / near-duplicate papers** are flagged (`is_title_unique`) but not removed.
- Multi-country affiliation strings resolve to the **first** recognized country.
