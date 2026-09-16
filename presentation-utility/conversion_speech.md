# Conversion speech

## Bronze

So, the conversion part. The data are provided in JSON format: each file contains a nested JSON object, one for each parsing of a paper. The parsings are of two different types, the PDF one and the PMC one. And then there is also a `metadata.csv` file, which contains the metadata of the papers and the links to the JSON files.

**Figure B1 · The raw data: `metadata.csv` links every paper to its JSON parsings**

```text
archive/
├── metadata.csv                          1 row = 1 paper record · 1,056,660 rows
│
│     cord_uid │ title │ publish_time │ … │ pdf_json_files                │ pmc_json_files
│     ─────────┼───────┼──────────────┼───┼───────────────────────────────┼─────────────────────
│     A        │ …     │ …            │ … │ pdf_json/a.json               │ pmc_json/A.json
│     B        │ …     │ …            │ … │ (empty)                       │ (empty)
│     C        │ …     │ …            │ … │ pdf_json/c1.json; c2.json     │ (empty)
│                                           0, 1 or more paths (';')        0 or 1 path
│
└── document_parses/
    ├── pdf_json/     one file per PDF parsing  ─┐
    └── pmc_json/     one file per PMC parsing  ─┴─▶  each file = ONE nested JSON object

        {
          "paper_id": "…",
          "metadata": {
            "authors": [ { "affiliation": { "institution": "…",
                                            "location": { "country": "…", "settlement": "…" } } },
                         … ]                                  ← one entry per author
          },
          "body_text": [ { "section": "Introduction", "text": "…" },
                         { "section": "Methods",      "text": "…" },
                         … ]                                  ← one entry per paragraph
        }
```

The conversion to Parquet format happens in two phases. First there is the bronze phase, which converts the information to Parquet without doing any cleaning, and then there is the silver phase, which instead is the one that prepares the data, doing semantic cleaning, deduplication and sanitization.

**Figure B2 · Two layers: bronze extracts, silver cleans**

```text
 RAW                               BRONZE                              SILVER
                                   faithful extraction, no cleaning    semantic cleaning, dedup

 metadata.csv  ──────────────────▶ papers        1,056,660 rows  ────▶ papers               970,836
      │
      │ cord_uid + paths
      ▼
 pdf_json / pmc_json  ───────────▶ paragraphs   23,110,668 rows  ────▶ paragraphs        12,445,234
 pdf_json only  ─────────────────▶ authors       2,943,737 rows  ────▶ authors            2,943,737
                                                                   ├─▶ paper_countries      284,042
                                                                   └─▶ paper_institutions   517,911
```

Inside the bronze phase there are three different pipelines, to produce three different tables: the papers table, the paragraphs table and the authors table.

**Figure B3 · The three bronze pipelines, side by side**

```text
                                archive/metadata.csv
                                         │
          ┌──────────────────────────────┼──────────────────────────────┐
          │ PAPERS                       │ PARAGRAPHS                   │ AUTHORS
          ▼                              ▼                              ▼
   dd.read_csv                    dd.read_csv                    dd.read_csv
   all columns                    usecols: cord_uid,             usecols: cord_uid,
                                           pdf_json_files,                pdf_json_files
                                           pmc_json_files
   dtype=str · 64 MB blocks       dtype=str · 64 MB blocks       dtype=str · 64 MB blocks
          │                              │                              │
   repartition(9)                 repartition(1024)              repartition(192)
          │                              │                              │
          │                       map_partitions(                map_partitions(
          │                         paragraphs_partition,          authors_partition,
          │                         meta=PARA_META_DF)             meta=AUTH_META_DF)
          │                       ┌────────────────────┐         ┌────────────────────┐
          │                       │ on the WORKER:     │         │ on the WORKER:     │
          │                       │ opens PDF + PMC    │         │ opens PDF JSONs    │
          │                       │ 1 row → N rows     │         │ 1 row → N rows     │
          │                       └────────────────────┘         └────────────────────┘
 ╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌ up to here NOTHING has run: it is only a graph (lazy) ╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌
          │                              │                              │
   write_parquet → to_parquet     write_parquet → to_parquet     write_parquet → to_parquet
   schema: every column string    schema=PARA_SCHEMA             schema=AUTH_SCHEMA
                                  row_group_size=20,000
          │                              │                              │     ═══ TRIGGER
          ▼                              ▼                              ▼
   bronze/papers                  bronze/paragraphs              bronze/authors
   9 files                        1024 files · 1979 row groups   192 files
   1,056,660 rows (×1)            23,110,668 rows (×21.9)        2,943,737 rows (×2.8)
```

Let's look at the first pipeline, the one that produces papers. We read the metadata.csv file into a Dask DataFrame, we read all the columns and we split them into nine partitions. And then we trigger the computation of the graph through the `write_parquet` function, which calls `to_parquet`, which is the one that triggers the actual computation. And that's it for papers: it essentially converts metadata.csv into the columnar Parquet format.

Then, for paragraphs, we again create a Dask DataFrame that reads the CSV, selecting only the columns `cord_uid`, `pdf_json_files` and `pmc_json_files`, which are the paths to the JSON files of the papers' parsings. We repartition it into 1024 partitions, then we apply to each partition, through `map_partitions`, the `paragraphs_partition` function, which extracts the paragraphs of each paper in the format obtained from the parsing, PDF or PMC. And then we trigger the compute, again with `to_parquet`, which writes.

Let's look at this pipeline a bit more closely, and then we'll see the one for the authors table, because they work the same way.

The important things to say. The tool we use is Arrow, which is a standard for representing tables in memory, so in the RAM of the workers. To use it we use the Python library PyArrow, which is also the one that writes Parquet, that is the format in which the data are saved on the workers' disk.

**Figure B4 · Where the data live: pandas and Arrow in RAM, Parquet on disk**

```text
 ┌──────────────────────── WORKER RAM ─────────────────────────┐       ┌────── WORKER DISK ──────┐
 │                                                             │       │                         │
 │  pandas DataFrame           PyArrow       Arrow table       │ Py-   │  Parquet file           │
 │  one partition        ────────────────▶   columnar,         │ Arrow │  columnar, zstd,        │
 │  pandas API                               typed columns     │ ────▶ │  types in the footer    │
 │  dtypes: object, int64                    string, int32     │       │                         │
 └─────────────────────────────────────────────────────────────┘       └─────────────────────────┘
```

And why do we need Arrow? Because pandas, which is actually the API that gets called to work with the DataFrames inside each worker, has ambiguous types, while with Arrow we are the ones who decide what the types are, through a schema. Text columns in pandas have `object` as their dtype, which is a pointer to an arbitrary Python object. The problem is that in the distributed version the types inferred locally could be different for different partitions, and this would break the schema we want to give to the data, to shape them into a logical relational model.

**Figure B5 · Why type inference breaks when every partition is written separately**

```text
 each partition is written by a different worker, which only sees its own data

 partition i   section = ["Introduction", None, "Methods"]   → PyArrow infers  section: string
 partition j   section = [None, None, None]   (or 0 rows)     → PyArrow infers  section: null    ✗
                                       │
                                       ▼  reading the dataset back
                 file i says "string", file j says "null"  →  schemas cannot be merged  →  error

 with schema=PARA_SCHEMA:  every partition is written as  section: string   ✓
```

And the difference between meta and schema in our code is this. Meta is a Python dictionary that has the column name as key and the dtype as value. We use it twice: inside the function, with `astype`, to fix the types of the pandas DataFrame that gets created inside each worker; and passed to `map_partitions`, to tell Dask in advance what that DataFrame looks like, since Dask is lazy and builds the graph before executing it. The schema, instead, is what PyArrow needs to fix the types on disk, when it writes the Parquet. And the types are different: meta contains `object` or `int64`, while the schema has `string` and `int32`. The integer goes from 8 to 4 bytes, so we do a downcast to save disk space.

**Figure B6 · meta vs schema: same columns, two moments, two audiences**

```text
 PARA_META   Python dict: column name → pandas dtype
 { cord_uid: object, paper_id: object, source: object, para_idx: int64, section: object, text: object }
      │
      ├──▶ PARA_META_DF = empty DataFrame with these dtypes
      │      map_partitions(paragraphs_partition, meta=PARA_META_DF)
      │      WHEN   while the graph is built, before anything runs
      │      FOR    Dask: "this is what every output partition will look like"
      │
      └──▶ .astype(PARA_META)   at the end of paragraphs_partition
             WHEN   on the worker, at run time
             FOR    pandas: forces the types of the DataFrame each worker builds

 PARA_SCHEMA   PyArrow schema
 [ cord_uid: string, paper_id: string, source: string, para_idx: int32, section: string, text: string ]
      │
      └──▶ to_parquet(schema=PARA_SCHEMA)
             WHEN   on the worker, when the bytes are written
             FOR    PyArrow: the types written on disk

   column      meta (RAM)          schema (disk)
   ─────────   ─────────────────   ─────────────────────────
   cord_uid    object              string
   para_idx    int64   · 8 bytes   int32   · 4 bytes          ← downcast
   text        object              string  (variable length)
```

Also, for later convenience, when we write to disk we use a fixed row group size. A row group is essentially the smallest unit a Parquet file is made of, in the sense that it is a block of rows contained in the file. A Parquet file is built like this: the first group of rows, the second group of rows, and so on, and at the end of the file a footer with the metadata, that is the schema and where each row group is. Each row group is the smallest reading unit, and inside it the data are stored by column: all the values of the first column for those rows are written one after the other, then all the values of the second column, and so on. We use a fixed size of 20,000 rows.

**Figure B7 · Anatomy of a Parquet file (the biggest bronze paragraphs file)**

```text
 dataset (folder)  ⊃  file (= one partition when written)  ⊃  row group (≤ 20,000 rows)  ⊃  column chunk

 bronze/paragraphs/part.55.parquet                                             110,281 rows
 ┌───────────────────────────────────────────────────────────────────────────────────────┐
 │ row group 0 · 20,000 rows                                                             │
 │   ┌──────────┬──────────┬────────┬──────────┬─────────┬───────────────────────────┐   │
 │   │ cord_uid │ paper_id │ source │ para_idx │ section │ text                      │   │
 │   │ ×20,000  │ ×20,000  │×20,000 │ ×20,000  │ ×20,000 │ ×20,000                   │   │
 │   └──────────┴──────────┴────────┴──────────┴─────────┴───────────────────────────┘   │
 │   one contiguous chunk per column                                                     │
 ├───────────────────────────────────────────────────────────────────────────────────────┤
 │ row group 1 · 20,000 rows                                                             │
 ├───────────────────────────────────────────────────────────────────────────────────────┤
 │ row group 2 · 20,000 rows                                                             │
 ├───────────────────────────────────────────────────────────────────────────────────────┤
 │ row group 3 · 20,000 rows                                                             │
 ├───────────────────────────────────────────────────────────────────────────────────────┤
 │ row group 4 · 20,000 rows                                                             │
 ├───────────────────────────────────────────────────────────────────────────────────────┤
 │ row group 5 · 10,281 rows                                          ← the remainder    │
 ├───────────────────────────────────────────────────────────────────────────────────────┤
 │ FOOTER · schema + where every row group starts                                        │
 └───────────────────────────────────────────────────────────────────────────────────────┘
   the row group is the smallest unit that can be read
```

Now let's look logically at what happens inside this pipeline that converts the paragraphs in bronze. We read metadata.csv and we make 1024 partitions. Inside this DataFrame a row essentially contains a link to zero, one or more parsings, because it contains the paths of the PDFs, which are sometimes more than one, and the path of the PMC, which can be present or missing.

Then, when we do `map_partitions`, we call on each of these partitions the `paragraphs_partition` function, which extracts the paragraphs of every PDF and PMC linked to that row, creating one row for each paragraph of each of these parsings, when they are present.

**Figure B8 · One metadata partition becomes one paragraphs partition**

```text
 refs · partition i                                   ≈ 1,032 rows of metadata.csv
 ┌──────────┬──────────────────────┬─────────────────┐
 │ cord_uid │ pdf_json_files       │ pmc_json_files  │
 ├──────────┼──────────────────────┼─────────────────┤
 │ A        │ a.json               │ A.json          │ ──▶ paragraphs of a.json (pdf) + of A.json (pmc)
 │ B        │ (empty)              │ (empty)         │ ──▶ 0 rows
 │ C        │ c1.json; c2.json     │ (empty)         │ ──▶ paragraphs of c1.json + c2.json (pdf)
 └──────────┴──────────────────────┴─────────────────┘
                         │
                         │ map_partitions(paragraphs_partition)     the JSON files are opened on the worker
                         ▼                                          1 partition in → 1 partition out
 paragraphs · partition i                             1 row = 1 paragraph
 ┌──────────┬──────────┬────────┬──────────┬──────────────┬────────┐
 │ cord_uid │ paper_id │ source │ para_idx │ section      │ text   │
 ├──────────┼──────────┼────────┼──────────┼──────────────┼────────┤
 │ A        │ a        │ pdf    │ 0        │ Introduction │ …      │
 │ A        │ a        │ pdf    │ 1        │ …            │ …      │
 │ A        │ A        │ pmc    │ 0        │ Introduction │ …      │
 │ C        │ c1       │ pdf    │ 0        │ …            │ …      │
 │ …        │ …        │ …      │ …        │ …            │ …      │
 └──────────┴──────────┴────────┴──────────┴──────────────┴────────┘
                         │ to_parquet
                         ▼
 part.i.parquet                                       cut inside into row groups of ≤ 20,000 rows
```

So the size in terms of rows grows, in the sense that one metadata row can be associated with a PDF and a PMC, which in turn have many paragraphs. And if each paragraph becomes a row, then one metadata row becomes several rows of the paragraphs Dask DataFrame. But the correspondence between partitions, since we do a map, is always one to one: one metadata partition returns one paragraphs partition. So it's as if we multiplied the number of rows, but we always get the same number of partitions, also when writing: we'll have 1024 partitions, meaning 1024 Parquet files, for paragraphs too.

**Figure B9 · Rows multiply, partitions stay the same**

```text
                  refs                  map_partitions      paragraphs              to_parquet
 partitions       1024          ──────────────────────▶     1024           ──────────────────▶  1024 files
 rows             1,056,660                                 23,110,668 (×21.9)
 rows/partition   ≈ 1,032 (uniform)                         0 … 110,281 (skewed: 119 empty files)
                                                                                                    │
                                                                        row_group_size=20,000       │
                                                                                                    ▼
                                                                                  1979 row groups ≤ 20,000 rows
```

These files, however, are split internally into row groups, because we set the row group size to 20,000. This means that the smallest reading unit is a block of at most 20,000 rows of this table. And in this table each row identifies a paragraph, with the six columns of the schema attached. The types are fixed by meta, which describes the workers' pandas DataFrames while the operations happen in RAM, and by the Arrow schema, which fixes them when the data are saved to disk.

So, the last pipeline is the one for the authors, and it does essentially the same thing: it reads the CSV creating a DataFrame, selects only the `cord_uid` and `pdf_json_files` columns, repartitions them and maps the `authors_partition` function over each partition, which extracts the authors with their affiliations, one row per author. And then the compute is triggered with `to_parquet`, passing the schema.

**Figure B10 · The authors pipeline: one row per author**

```text
 refs_pdf · partition i   (cord_uid, pdf_json_files)                       192 partitions
      │
      │ map_partitions(authors_partition, meta=AUTH_META_DF)
      │   for every PDF JSON of the row:  metadata.authors[ ]  →  one row per author
      ▼
 ┌──────────┬──────────┬────────────┬─────────────┬─────────────┬────────────┐
 │ cord_uid │ paper_id │ author_idx │ institution │ country_raw │ settlement │
 ├──────────┼──────────┼────────────┼─────────────┼─────────────┼────────────┤
 │ A        │ a        │ 0          │ …           │ Italy       │ Rome       │
 │ A        │ a        │ 1          │ (null)      │ (null)      │ (null)     │  ← authors without
 └──────────┴──────────┴────────────┴─────────────┴─────────────┴────────────┘    affiliation are kept
      │
      │ to_parquet(schema=AUTH_SCHEMA)                 author_idx: int64 in RAM → int32 on disk
      ▼
 bronze/authors · 192 files · 2,943,737 rows
 PMC JSONs are not read: their affiliation fields are almost always empty
```

## Silver

Ok, now let's look at the silver part, which again has three different pipelines: one for the papers, one for the authors and one for the paragraphs.

**Figure S1 · The three silver pipelines, side by side**

```text
 bronze/papers                     bronze/authors                    bronze/paragraphs
 1,056,660 rows · 9 files          2,943,737 rows · 192 files        23,110,668 rows · 1024 files
        │                                 │                                 │
        ▼                                 ▼                                 ▼
 silver_papers()                   country_raw                       read_parquet(
  · + year, has_pdf, has_pmc        .dropna().unique()                 split_row_groups=True)
  · drop 3 dead columns             .compute()  ═══ TRIGGER            → 1979 partitions
  · _rank = has_pdf + has_pmc              │                                 │
  · SHUFFLE on cord_uid                    ▼                                 ▼
  · map_partitions(                 ┌──────────────┐                 pmc_uids: unique cord_uid
      _dedup_richest)               │    DRIVER    │                 where source = pmc
        │                           │ 8,710 values │                 .compute()  ═══ TRIGGER
        ▼                           │ country_     │                        │
 enrich_papers()                    │ converter →  │                 ┌──────┴───────┐
  · title_norm                      │ iso3_map,    │                 │    DRIVER    │
  · groupby(title_norm).size()      │ name_map     │                 │ set of       │
  · merge → title_dup_count         └──────┬───────┘                 │ 315,653 uids │
  · title_ok, is_title_unique              │ sent to the workers     └──────┬───────┘
        │                                  ▼                                │
        │                           .map(iso3_map)                   client.restart()
        │                           .map(name_map)                          │
        │                           .map(norm_institution)                  ▼
        │                           select columns                   LOOP · 5 blocks of 448
        ▼                                  ▼                          · prefer-PMC filter
 write_parquet  ═══ TRIGGER         write_parquet  ═══ TRIGGER        · + is_reference_like
        ▼                                  ▼                          · to_parquet ═══ TRIGGER
 silver/papers                      silver/authors                    · client.restart()
 970,836 rows (−85,824)             2,943,737 rows (×1)                     ▼
 9 files · 23 columns               192 files                         silver/paragraphs
                                           │                          12,445,234 rows (−46%)
                            ┌──────────────┴───────────────┐          1979 files
                            ▼                              ▼
                  silver/paper_countries        silver/paper_institutions
                  284,042 rows · 64 files       517,911 rows · 48 files

 what each row needs to know              how we get it                          what moves
 ──────────────────────────────────────   ────────────────────────────────────   ─────────────────────
 all rows with the same cord_uid          shuffle                                the papers table
 the ISO3 code of its country_raw         compute on the driver, send dicts      8,710 distinct values
 whether its paper has a PMC parsing      compute on the driver, send the set    315,653 cord_uids
```

**Papers.** Let's start with the pipeline for the papers. We read the Parquet obtained from the bronze phase, which is a DataFrame with nine partitions, and we apply to it first the `silver_papers` function and then the `enrich_papers` function. They are not maps: they are Python functions that chain lazy Dask operations one after the other.

**Figure S2 · The papers pipeline, step by step: what each operation does to the partitions**

```text
 read_parquet(bronze/papers)                        9 partitions
   │
   │ silver_papers()
   │   assign(year, has_pdf, has_pmc)       9 → 9   element-wise, partition by partition
   │   drop(mag_id, arxiv_id,               9 → 9   dead columns: almost completely empty
   │        who_covidence_id)
   │   assign(_rank)                        9 → 9
   │   shuffle(on="cord_uid")               9 → 9   rows MOVE between partitions
   │   map_partitions(_dedup_richest)       9 → 9   partition by partition
   │   drop(_rank)
   │
   │ enrich_papers()
   │   assign(title_norm)                   9 → 9
   │   groupby("title_norm").size()         counts over ALL partitions
   │   merge(counts, on="title_norm")       9 → 9   attaches title_dup_count to every row
   │   assign(title_ok, is_title_unique)    9 → 9
   │
   │ write_parquet                          ═══ TRIGGER: everything above runs now
   ▼
 silver/papers · 9 files · 970,836 rows
```

First of all, `silver_papers` assigns the `year` column, extracting it from `publish_time`: it takes the first four digits in a controlled way, in the sense that if they are not a number the year stays empty. Then it drops the dead columns, which are the ones we don't need because they are almost completely empty.

**Figure S3 · year and _rank (illustrative rows)**

```text
 publish_time    first 4 characters   year                    pdf_json_files   pmc_json_files   _rank
 ─────────────   ──────────────────   ─────────               ──────────────   ──────────────   ─────
 "2020-03-15"    "2020"               2020                    a.json           A.json           2
 "2019"          "2019"               2019                    a.json           (empty)          1
 "unknown"       "unkn"               empty (not a number)    (empty)          (empty)          0
```

Then it creates a new column, the rank, which tells us how many types of parsing that row has available, PDF plus PMC. So it can be zero if it has no parsing, one if it has one of the two types, two if it has both.

What happens next is essentially this. Since `cord_uid` is not unique, meaning that inside papers there can be several rows with the same `cord_uid`, we want to group them and keep only the one with the highest rank, dropping the other rows. Logically it's a group by on `cord_uid`, but we do it with a shuffle plus a `map_partitions`.

The shuffle on `cord_uid` essentially brings into the same partition all the rows that have the same `cord_uid`. This is because afterwards, for each `cord_uid`, we keep only the row with the highest rank, and this operation is done partition by partition. If we didn't do the shuffle, the rows with the same `cord_uid` could be scattered across different partitions, and we would remove the duplicates inside each partition, but not across the whole dataset. So the shuffle is necessary. It still preserves the number of partitions, but it destroys the correspondence between the partitions before and after, because the rows move.

**Figure S4 · Shuffle, then deduplicate**

```text
 BEFORE the shuffle                            AFTER shuffle(on="cord_uid")
 duplicates scattered across partitions        destination partition decided by hash(cord_uid)

 partition 0 ┌─────────────────┐               partition 3 ┌─────────────────┐
             │ A    _rank 1    │                           │ A    _rank 1    │
             │ B    _rank 2    │      ──────▶              │ A    _rank 2    │   all A rows together
             └─────────────────┘                           └─────────────────┘
 partition 4 ┌─────────────────┐               partition 7 ┌─────────────────┐
             │ A    _rank 2    │                           │ B    _rank 2    │
             │ C    _rank 0    │                           │ C    _rank 0    │
             └─────────────────┘                           └─────────────────┘
 still 9 partitions, but partition 3 no longer has anything to do with the old partition 3

 map_partitions(_dedup_richest):   sort by _rank (descending) → keep the first row of every cord_uid
 partition 3:  A _rank 2  ✓   (A _rank 1 dropped)
 partition 7:  B _rank 2  ✓    C _rank 0  ✓

 WITHOUT the shuffle:  partition 0 keeps A, partition 4 keeps A  →  a duplicate survives, and no error
```

After the shuffle, we call with `map_partitions` the `_dedup_richest` function, which sorts the rows by rank and, for each `cord_uid`, keeps only the first one, that is the one with the highest rank.

Then we call the `enrich_papers` function, which essentially adds more information about the titles. It assigns the `title_norm` column, which is the sanitized and normalized title. Then it does a group by on `title_norm`, to see how many duplicates there are for each normalized title, and it does a merge, that is, it attaches to each row, through `title_norm`, the corresponding count, in the `title_dup_count` column. It also adds another column, `is_title_unique`, to check whether that normalized title has duplicates, because several original titles can be different from each other but, once we normalize them, become the same title. And then we write with `write_parquet`, which triggers the computation. And that's it for the papers pipeline.

**Figure S5 · Count the normalized titles and attach the count back (illustrative titles)**

```text
 title (original)             title_norm                    groupby("title_norm").size()
 ──────────────────────────   ───────────────────────       ─────────────────────────────
 "COVID-19 in   Italy"        "covid-19 in italy"           "covid-19 in italy"   2
 " covid-19 in Italy"         "covid-19 in italy"           "sars-cov-2 spike"    1
 "SARS-CoV-2 spike"           "sars-cov-2 spike"                      │
              │                                                       │
              └───────────── merge(on="title_norm", how="left") ◀─────┘
                                           │
                                           ▼
 title_norm                   title_dup_count   is_title_unique
 "covid-19 in italy"          2                 False
 "covid-19 in italy"          2                 False
 "sars-cov-2 spike"           1                 True

 normalization: strip · lowercase · collapse repeated spaces      flags only: no row is dropped
```

**Authors.** Then there is the pipeline for the authors. We read the 192-partition Parquet obtained in the bronze phase. As a first step we immediately do a compute, the one for the distinct values of `country_raw`. In the sense that the country names we have are not standardized, they can even be misspelled, and we need the list of all the distinct values of `country_raw`, because we need it to use country converter. The compute is called immediately because we need this list in memory on the driver: from there we build the dictionaries that are then sent to all the workers.

So, after computing the distinct values, `build_country_maps` builds two dictionaries using country converter: `iso3_map`, which associates each value of `country_raw` with the ISO3 code of the country, and `name_map`, which associates each ISO3 code with the standard name.

Then, as a third step, it assigns the `country_iso3` column, that is, it applies `iso3_map` to each value of `country_raw`. Then it adds the `country` column, that is, on each partition it applies the `name_map` dictionary, which converts from ISO3 to the standard name. And then it maps the `norm_institution` function, which normalizes the name of the institution the author belongs to.

**Figure S6 · The authors pipeline: stop on the driver, then send the dictionaries back**

```text
 WORKERS                                          DRIVER  (the Python process that launches the job)

 bronze/authors · 192 partitions
 country_raw.dropna().unique()
 .compute()  ═══ TRIGGER  ──────────────────────▶ distinct = 8,710 raw country strings
                                                         │
                                                  build_country_maps(distinct)      plain Python
                                                    1. country_converter    "Italy"        → ITA
                                                    2. manual aliases       "Deutschland"  → DEU
                                                    3. retry on the first   "UK, London"   → "UK" → GBR
                                                       part of the string
                                                         │
                                                  iso3_map   country_raw → ISO3     8,710 keys
                                                  name_map   ISO3 → standard name   DEU → "Germany"
                                                         │
 .map(iso3_map)          → country_iso3   ◀──────────────┘ the dictionaries travel with the tasks
 .map(name_map)          → country
 .map(norm_institution)  → institution_norm      unicode normalization, repeated spaces, punctuation at the ends
 select columns
 192 → 192 partitions · element-wise · no communication between workers
 write_parquet  ═══ TRIGGER
      ▼
 silver/authors · 192 files · 2,943,737 rows (no row removed)

 one row, before and after
 country_raw      country_iso3   country     institution          institution_norm
 "Deutschland"    DEU            Germany     "Charité  Berlin ,"  "Charité Berlin"
```

The last step is selecting the columns we need and writing the Parquet: here the computation of all the maps is triggered, and the authors table is saved in `silver/authors`. So in this pipeline two computes are performed: one at the beginning, for the distinct values, and one at the end, for the writing.

**Rollup tables.** Then, from this table, the rollup tables are created. The first one is `paper_countries`, where we drop all the rows that don't have the ISO3 code: authors who didn't have a country at all, or countries we couldn't convert, for example because they were misspelled. Then we drop the duplicates, because identical rows would represent the same paper written by people from the same country. So we follow the rule: one row for each paper–country pair. In the sense that, if several authors from the same country wrote the same paper, drop duplicates keeps a single row and not N rows corresponding to N authors. This way a paper, for that country, is counted with weight one and not with the number of its authors who belong to that country.

Then there is the other rollup table, `paper_institutions`, which does exactly the same thing: it drops the rows where there is no normalized institution and then it drops the duplicates. So if, for example, a paper was written by several authors from the same institution, only one row is kept and not N rows equal to the number of authors from that institution. This way, in the metrics, the weight is always one paper per institution.

**Figure S7 · Rollup tables: from one row per author to one row per paper–country pair**

```text
 silver/authors  (1 row per author)                           paper_countries  (1 row per pair)
 cord_uid   country_iso3                                       cord_uid   country_iso3
 ────────   ────────────                                       ────────   ────────────
 P1         ITA                                                P1         ITA
 P1         ITA            dropna(subset=["country_iso3"])     P1         USA
 P1         USA          ───────────────────────────────────▶  P2         DEU
 P1         (null)  ✗      drop_duplicates()
 P2         DEU
 → P1 counts once for Italy, not twice

 2,943,737 author rows
   ├─ 1,589,223 with no country at all        ┐ dropped
   ├─     9,115 with a country not converted  ┘
   └─▶ paper_countries      284,042 rows · 64 files
 same logic on institution_norm
   └─▶ paper_institutions   517,911 rows · 48 files
```

**Paragraphs.** And then there is the slightly spicier part, which is the conversion of the paragraphs in the silver phase. Let's go through it one step at a time.

First of all we read the Parquet saved by the bronze phase, using the argument `split_row_groups=True`. What happens: before, we had 1024 Parquet files with a fixed row group size, and each file was a partition of our distributed DataFrame. The `split_row_groups=True` option changes the unit we partition on: now each row group becomes a partition. So we go from 1024 partitions, equal to the number of files, to 1979 partitions, that is the total number of row groups contained in those 1024 files.

**Figure S8 · split_row_groups=True: from one partition per file to one partition per row group**

```text
 bronze/paragraphs                         default: 1 partition = 1 file    split_row_groups=True
 ─────────────────────────────────────     ─────────────────────────────    ─────────────────────────
 part.55.parquet   [rg0][rg1]…[rg5]        1 partition  (110,281 rows)      6 partitions (≤ 20,000)
 a small file      [rg0]                   1 partition                      1 partition
 an empty file     [rg0: 0 rows]           1 partition                      1 partition
 …

 total             1024 files              1024 partitions                  1979 partitions
                                           0 … 110,281 rows each            ≤ 20,000 rows each
```

Then a compute is called immediately to extract the unique values of what we call `pmc_uids`. `pmc_uids` is the set of the `cord_uid`s of all the papers that have at least one paragraph with source PMC, that is, that have a PMC parsing. We call the compute because we need this list in memory, to then send it to all the workers and perform comparisons.

This compute does a shuffle over all 1979 partitions and leaves a non-negligible amount of unmanaged memory on the workers. So, before starting, we restart the client, so that the first iteration starts clean.

Then we work on one batch of partitions at a time, because we had memory problems. Basically we take a certain number of partitions, 448, instead of the whole dataset, we perform the operations on them and then we restart the client, because the unmanaged memory of the workers grows with every partition. With the restart we bring it back down and keep it under the limit, before the cluster dies on us.

**Figure S9 · The paragraphs pipeline: one compute, one restart, then a loop of blocks**

```text
 para_full · 1979 partitions
      │ filter source == "pmc" → cord_uid → dropna().unique()
      │ .compute()                        ═══ TRIGGER · a shuffle over all 1979 partitions
      ▼
 ┌──────────────── DRIVER ────────────────┐
 │ pmc_uids = set of 315,653 cord_uids    │   papers that have a PMC parsing
 └───────────────────┬────────────────────┘   computed ONCE, on the whole table
                     │
 client.restart(wait_for_workers=True)         workers restart clean · pmc_uids survives (it is on the driver)
                     │
                     ▼
 ┌──── LOOP · 5 blocks ─────────────────────────────────────────────────────────────────────┐
 │                                                                                          │
 │  block 0   partitions     0 –  447   (448)   → restart                                   │
 │  block 1   partitions   448 –  895   (448)   → restart                                   │
 │  block 2   partitions   896 – 1343   (448)   → restart                                   │
 │  block 3   partitions  1344 – 1791   (448)   → restart                                   │
 │  block 4   partitions  1792 – 1978   (187)   → no restart after the last block           │
 │                                                                                          │
 │  for every block:                                                                        │
 │    sub = para_full.partitions[b0 : b0 + 448]                                             │
 │    sub = silver_paragraphs(sub, pmc_uids)          prefer-PMC filter                     │
 │    sub = sub.assign(is_reference_like = …)         regex on the section title            │
 │    sub.to_parquet(silver/paragraphs)               ═══ TRIGGER                           │
 │      first block overwrites the folder, the others append · files part.0 … part.1978     │
 │    client.restart()                                unmanaged memory back down            │
 │                                                                                          │
 └──────────────────────────────────────────────────────────────────────────────────────────┘
                     │
                     ▼
 silver/paragraphs · 1979 files · 12,445,234 rows (−46%)

 unmanaged memory of a worker (schematic)
   without restarts   keeps growing with every partition, no plateau → past the memory limit
   with restarts      ╱│  ╱│  ╱│  ╱│  ╱
                     ╱ │ ╱ │ ╱ │ ╱ │ ╱
                    ╱  │╱  │╱  │╱  │╱
                    b0  b1  b2  b3  b4      every restart brings it back down
```

The operations performed on each batch are these. First we take the batch, of course. Then we apply the `silver_paragraphs` function: up to this point the paragraphs are stored from both the PMC parsing and the PDF parsing, and this function applies the policy of preferring PMC. Having sent the `pmc_uids` list to the workers, for each paragraph we look at its `cord_uid`: if the paper has the PMC parsing, we keep the PMC paragraphs and discard the PDF ones; if instead the PMC is not there, we keep the PDF paragraphs.

Then we assign the `is_reference_like` column, that is, we use a regular expression, lazily of course, on the section title, to see whether a paragraph is a real paragraph or boilerplate, like the bibliography or the acknowledgements.

**Figure S10 · Row by row: the prefer-PMC filter and the is_reference_like flag**

```text
 pmc_uids = { A, D, … }

 cord_uid   source   section             prefer-PMC                          is_reference_like
 ────────   ──────   ─────────────────   ─────────────────────────────────   ─────────────────
 A          pmc      Introduction        keep   (PMC row)                    False
 A          pdf      Introduction        drop   (A has a PMC parsing)        —
 C          pdf      Results             keep   (C has no PMC parsing)       False
 D          pmc      References          keep                                True   "referen…"
 D          pmc      Acknowledgements    keep                                True   "acknowledg…"
 E          pdf      (null)              keep                                False  (null → "")

 regex on the lowercased section title:
   referen · bibliograph · acknowledg · author contrib · conflict · competing interest ·
   funding · declarat · supplement · copyright                        flag only: nothing is dropped

 bronze   pmc  9,239,565 + pdf 13,871,103 = 23,110,668 rows
 silver   pmc  9,239,565 + pdf  3,205,669 = 12,445,234 rows     is_reference_like = True on 235,914 rows
```

Then the computation on this batch is triggered with `to_parquet`, and in the end we get 1979 partitions in total, that is 1979 files, one for each row group read from the bronze paragraphs table. And between one batch and the next, the client is restarted.
