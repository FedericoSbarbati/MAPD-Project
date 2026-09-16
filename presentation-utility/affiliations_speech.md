# Affiliations speech

Ok, let's look at the pipeline for the affiliations.

**Figure A1 · The whole affiliations pipeline**

```text
 silver/authors   192 Parquet files · 2,943,737 rows · 1 row = 1 author of 1 paper
       │
       │ author_files()           → list of Paths, sorted by partition number
       │ split_evenly(files, k)   → k groups of file paths          ← k is the benchmark knob (default 8)
       ▼
 dd.from_map(load_group, groups, meta=meta)
       │   load_group(group) = pq.read_table(group, columns=[cord_uid, country, institution_norm])
       │                         .to_pandas()
       │   one call per group → one pandas DataFrame → one partition
       ▼
 Dask DataFrame  (cord_uid, country, institution_norm)                          k partitions
       │
       │ ranking(authors, column)       for column in [country, institution_norm]       (lazy)
       │
       │   valid = authors[[cord_uid, column]].dropna(subset=[column])
       │      │
       │      ├──▶ valid[column].value_counts()                     ──▶ per_autore   spelling → authors
       │      │
       │      └──▶ .assign(chiave = valid[column].map(chiave))
       │           [cord_uid, chiave].drop_duplicates()
       │           .chiave.value_counts()                           ──▶ per_paper    key → papers
       ▼
 4 lazy Series:  per_paper, per_autore  ×  country, institution_norm
       │ dask.compute(*lazy)                               ═══ TRIGGER · one compute for all four
       ▼
 4 pandas Series on the DRIVER
       │ classifica(per_paper, per_autore)                 plain pandas
       ▼
 pandas DataFrame   entity · papers · authors    ──▶  *_ranking.csv · *_top.png · *_bottom.png
```

**Reading.** We start by reading the silver authors Parquet, which is split into 192 files. We call `author_files`, which sorts the paths of the Parquet files in ascending order by the partition number N, that is `part.N.parquet`. Then we use `split_evenly` to group the paths of these files into k groups. We want to create k partitions, where each partition corresponds to a certain number of Parquet files, because the number of partitions is an independent variable that we have to measure in the benchmarks. By default k is 8, which is the best value we measured.

**Figure A2 · From 192 files to k groups (default k = 8)**

```text
 silver/authors: 192 files      split_evenly(files, 8)  →  8 groups of 24 files

 group 0   part.0.parquet    …  part.23.parquet
 group 1   part.24.parquet   …  part.47.parquet
 group 2   part.48.parquet   …  part.71.parquet
 …
 group 7   part.168.parquet  …  part.191.parquet

 k = number of partitions = independent variable of the benchmark
 default 8: the best value measured on the cluster
```

Then, unlike the word count, we don't go through a Bag: we directly call `dd.from_map`, with the `load_group` function, the list of the k groups and a `meta`. `from_map` calls `load_group` once for each group, and the pandas DataFrame that each call returns becomes a partition of the Dask DataFrame. Inside `load_group` we call PyArrow's `read_table`, which reads the files of the group, and we convert the table to pandas. The columns we load are three: `cord_uid`, which identifies the paper, `country`, and `institution_norm`, that is the institution already normalized in the silver phase of the conversion. The `meta`, instead, is an empty pandas DataFrame with these three columns and their types, built by reading only the schema of the first file: it tells Dask what each partition looks like without reading the data, since everything is lazy.

**Figure A3 · dd.from_map and meta: each group becomes a partition directly**

```text
 dd.from_map(load_group, groups, meta=meta)

 group 0  ──load_group(group 0)──▶  partition 0 = pandas DataFrame
 group 1  ──load_group(group 1)──▶  partition 1 = pandas DataFrame
 …
 group 7  ──load_group(group 7)──▶  partition 7 = pandas DataFrame
                                    ┌──────────┬────────────────┬──────────────────────────────┐
                                    │ cord_uid │ country        │ institution_norm             │
                                    ├──────────┼────────────────┼──────────────────────────────┤
                                    │ P1       │ Hong Kong      │ The University of Hong Kong  │
                                    │ …        │ …              │ …                            │
                                    └──────────┴────────────────┴──────────────────────────────┘
 load_group(group) = pq.read_table(group, columns=["cord_uid", "country", "institution_norm"]).to_pandas()
 the files are read on the worker, when the computation starts

 meta = pq.read_schema(first file)          only the schema of the first file, no rows
          .empty_table()                    an Arrow table with 0 rows
          .select([cord_uid, country, institution_norm])
          .to_pandas()                      an empty pandas DataFrame
                                            cord_uid: string · country: string · institution_norm: string
        → Dask knows the columns and types of every partition without reading any data

 word count (2.3.1)                                  affiliations (2.3.2)
 db.from_sequence(groups, partition_size=1)          dd.from_map(load_group, groups, meta=meta)
   a Bag whose elements are lists of paths             no intermediate collection of paths
 .map_partitions(load_group)                           the DataFrame returned by load_group
   a Bag of (cord_uid, text) tuples                    IS the partition, with typed columns
```

**Ranking.** Then we apply the `ranking` function, which takes a column as argument, which can be `country` or `institution_norm`, and which we will therefore call the affiliation.

First of all we do a `dropna` on the rows where the affiliation is not defined: an author with no affiliation doesn't vote. What remains are the affiliations as they came out of silver, which we will call spellings, because they are the names written exactly as we find them.

Then we define `per_autore`: we select the affiliation column and do a `value_counts`, lazily, which for each distinct spelling computes the frequency, that is how many rows contain it. Since in silver authors each row is an author of a paper, this frequency is the number of authors for that spelling: co-authors of the same paper with the same affiliation add up, and if the same person wrote three papers with that affiliation, they count three times. It's like doing a group by on the spelling and counting the rows.

**Figure A4 · dropna and per_autore: counting rows per spelling (illustrative rows)**

```text
 silver/authors  (1 row = 1 author of 1 paper)                 column = institution_norm

 cord_uid   institution_norm
 ────────   ──────────────────────────────
 P1         The University of Hong Kong       ┐ two co-authors of P1
 P1         The University of Hong Kong       ┘
 P1         (null)                            ✗  dropna: an author with no affiliation does not vote
 P2         the University of Hong Kong
 P3         The University of Hong Kong       ← same person as in P1? on another paper it counts again
      │
      │ valid[column].value_counts()          counts ROWS per spelling  (≈ group by spelling + count)
      ▼
 per_autore
 The University of Hong Kong    3
 the University of Hong Kong    1

 full corpus: 1,345,399 valid rows for country · 1,521,262 for institution_norm
```

Then we assign a new column, `chiave`, applying with a map, on each partition, the `chiave` function to the affiliation column. So we have a new column which is the sanitized affiliation.

The `chiave` function does four things, in this order: it removes the non-alphanumeric characters at the beginning and at the end, like `‡` or `†`, which come from the parsing of the PDFs; it removes the leading article, like `The` or `La`; it lowercases everything; and it removes the accents, so `université` and `universite` become the same. This way the different spellings of the same entity end up on the same key: for example `The University of Hong Kong` and `the University of Hong Kong` both become `university of hong kong`. On institutions this brings the distinct names from 105,967 to 100,838, and it changes the ranking: the University of Hong Kong was split into six spellings, and it moves up from seventeenth to fourteenth place. On countries, instead, nothing changes, because silver already standardized them: they stay 206.

**Figure A5 · chiave(): four rules, and the number that justifies them**

```text
 chiave(nome)
   1. BORDI      ^[^\w]+ | [^\w]+$         non-alphanumeric characters at the start and at the end
   2. ARTICOLO   ^(the|la|le|el|il)\s+     leading article, any case
   3. lower()
   4. NFKD + drop the combining marks      accents:  "é" → "e" + "´" → "e"

 input (real outputs of the function)       key
 ────────────────────────────────────       ─────────────────────────
 "‡ Rice University"                        "rice university"
 "†University of Padua‡"                    "university of padua"
 "The University of Hong Kong"              "university of hong kong"
 "the University of Hong Kong"              "university of hong kong"
 "Université de Padoue"                     "universite de padoue"

 institutions, full corpus (rules added one at a time)
 rule                                          distinct names   merged
 ───────────────────────────────────────────   ──────────────   ──────
 silver institution_norm (baseline)                  105,967        —
 + lowercase                                         103,570    −2.3%
 + non-alphanumeric characters at the edges          103,203    −2.6%
 + leading article                                   102,008    −3.7%
 + accents folded                                    100,838    −4.8%

 countries: 206 → 206   (already standardized with country_converter in silver)
```

Then we define `per_paper`: we select `cord_uid` and `chiave`, and we drop all the duplicates. When we have the same `cord_uid` and the same key, we are looking at different authors who wrote the same paper with the same affiliation, and drop duplicates keeps only one row. So we are changing the metric from authors to papers: for that sanitized affiliation each paper counts only once. Then we select the `chiave` column and do the `value_counts`, which does the same thing as before: for each distinct key it gives us the frequency, which is now the number of papers.

These are all lazy operations. So, in the end, the difference between `per_autore` and `per_paper` lies in two things: the drop duplicates and the sanitization. `per_autore` looks at how many authors published with that spelling, while `per_paper` looks at how many papers that sanitized affiliation actually produced.

**Figure A6 · per_paper: one row per (paper, key), then count (same illustrative rows as A4)**

```text
 valid.assign(chiave = valid[column].map(chiave))
 cord_uid   institution_norm                 chiave
 ────────   ──────────────────────────────   ─────────────────────────
 P1         The University of Hong Kong      university of hong kong    ┐ same (paper, key)
 P1         The University of Hong Kong      university of hong kong    ┘ → one row kept
 P2         the University of Hong Kong      university of hong kong
 P3         The University of Hong Kong      university of hong kong
      │
      │ [["cord_uid", "chiave"]].drop_duplicates()
      ▼
 P1   university of hong kong
 P2   university of hong kong
 P3   university of hong kong
      │
      │ .chiave.value_counts()
      ▼
 per_paper
 university of hong kong    3

 ┌──────────────────────────────────────────┬──────────────────────────────────────────┐
 │ per_autore                               │ per_paper                                │
 ├──────────────────────────────────────────┼──────────────────────────────────────────┤
 │ counted on the SPELLING                  │ counted on the KEY                       │
 │ no dedup, no sanitization                │ drop_duplicates + chiave                 │
 │ The University of Hong Kong    3         │ university of hong kong    3             │
 │ the University of Hong Kong    1         │                                          │
 │ = how many authors                       │ = how many papers                        │
 └──────────────────────────────────────────┴──────────────────────────────────────────┘
```

**Compute.** These operations are prepared for both values of the affiliation, `country` and `institution_norm`, and we get four lazy Series. Then all four are computed with a single `dask.compute`: this way the files are read only once and the shared steps are computed only once, instead of reading everything four times. The result is four pandas Series, which come back to the driver.

**Figure A7 · One compute for four Series, and what Dask actually runs**

```text
 lazy = [ per_paper(country),     per_autore(country),
          per_paper(institution), per_autore(institution) ]

 dask.compute(*lazy)          ═══ TRIGGER
   one graph · the 192 files are read ONCE · shared steps run once
   (four separate compute calls would read the 192 files four times)
        │
        ▼
 4 pandas Series on the driver

 optimized plan (dask 2026.6, k = 8)
 per_paper                                             per_autore
 ───────────────────────────────────────────────       ──────────────────────────────────────────
 FromMap(load_group)              8 partitions         FromMap(load_group)            8 partitions
 Projection [cord_uid, column]                         Projection [column]
 Dropna                                                Dropna
 Assign chiave = Map(chiave)      element-wise         ValueCounts(Chunk)    count in each partition
 DropDuplicates(Chunk)            in each partition    SHUFFLE on the value
 SHUFFLE on (cord_uid, chiave)                         ValueCounts(Aggregate)         8 partitions
 DropDuplicates(Aggregate)        across partitions
 ValueCounts(Chunk)               in each partition
 SHUFFLE on the key
 ValueCounts(Aggregate)           8 partitions
```

**Ranking table.** Finally we call the `classifica` function, which from here on is only pandas, on the driver. From `per_autore`, with `rename_axis` we call the spelling `entity`, and with `reset_index` we call the count `authors`: we get the `grafie` DataFrame, the spellings, with the columns `entity` and `authors`. On `grafie` we then apply the `chiave` function with a map on all the entities, that is on all the spellings. Then we sort by number of authors in descending order and do a group by on the key: for each key we keep as `entity` the first spelling, which, since they are sorted in descending order, is the most frequent spelling, and we sum the number of authors.

This gives us the `frame` DataFrame, which has the key as index and `entity` and `authors` as columns. Then we add the `papers` column to it, taking it from `per_paper`: since `per_paper` is also indexed by key, pandas associates each row with its number of papers. What we return is the `frame` sorted by number of papers in descending order and, in case of a tie, in alphabetical order on `entity`, dropping the key. So the final shape is a pandas DataFrame with three columns: `entity`, that is the most frequent spelling of that affiliation, `papers` and `authors`.

**Figure A8 · classifica() on the driver, with the real University of Hong Kong rows**

```text
 per_autore   (pandas Series: spelling → authors)
      │ .rename_axis("entity").reset_index(name="authors")
      ▼
 grafie
 entity                            authors
 ───────────────────────────────   ───────
 The University of Hong Kong          4577
 University of Hong Kong               634
 the University of Hong Kong           126
 †The University of Hong Kong           12
 The University of Hong Kong)            8
 †University of Hong Kong                1
 …                                       …
      │ grafie["chiave"] = grafie["entity"].map(chiave)       pandas map: runs immediately
      │ .sort_values("authors", ascending=False)
      ▼
 entity                            authors   chiave
 The University of Hong Kong          4577   university of hong kong
 University of Hong Kong               634   university of hong kong
 the University of Hong Kong           126   university of hong kong
 …                                       …   …
      │ .groupby("chiave").agg(entity=("entity", "first"),     first = most frequent spelling
      │                        authors=("authors", "sum"))     4577+634+126+12+8+1
      ▼
 frame   (index = chiave)
 chiave                      entity                        authors
 university of hong kong     The University of Hong Kong      5358
      │ frame["papers"] = per_paper                            aligned on the index: the key
      ▼
 chiave                      entity                        authors   papers
 university of hong kong     The University of Hong Kong      5358     1188
      │ .sort_values(["papers", "entity"], ascending=[False, True])
      │ .reset_index(drop=True)[["entity", "papers", "authors"]]      the key is dropped
      ▼
 rank   entity                          papers   authors
 14     The University of Hong Kong      1,188     5,358     (17th without chiave: six spellings)
```

**Figure A9 · chiave() runs twice, in two different worlds**

```text
                  in ranking()  →  per_paper                   in classifica()
 ─────────────    ─────────────────────────────────────────    ─────────────────────────────────────────
 where            on the workers                               on the driver
 on what          every valid author row                       every distinct spelling
                  (1,521,262 institution rows)                 (105,967 institutions · 206 countries)
 how              Dask Series.map · lazy                       pandas Series.map · immediate
 triggered by     dask.compute(*lazy)                          nothing: at this point it is plain pandas
 why              count papers per key                         link each spelling to its key, so the
                                                               label is the most frequent spelling
```

At the end, for countries and for institutions, the driver writes the CSV with the complete ranking and two charts: the 20 entities with the most papers and the 20 with the fewest.

**Figure A10 · Output and results on the full corpus**

```text
 written by the driver (pandas)
   country_ranking.csv       country_top.png       country_bottom.png
   institution_ranking.csv   institution_top.png   institution_bottom.png

                           countries      institutions
 ───────────────────────   ─────────      ──────────────
 valid author rows         1,345,399      1,521,262
 distinct spellings              206        105,967
 distinct keys                   206        100,838
 (paper, key) pairs          284,042        517,058
 entities with 1 paper            16         68,126  (67.6%)

 top 5 by papers
 country          papers    authors      institution                    papers   authors
 ──────────────   ───────   ───────      ────────────────────────────   ──────   ───────
 United States     50,351   257,902      University of California        3,521    13,081
 China             28,398   180,235      University of Oxford            2,169     7,794
 United Kingdom    21,461    95,983      Harvard Medical School          1,971     6,712
 Italy             16,864   103,302      University of Toronto           1,857     5,189
 Germany           12,218    56,607      Chinese Academy of Sciences     1,763     7,741

 bottom of the ranking
   countries      a real result: 16 countries with a single paper (Tonga, Trinidad and Tobago, …)
   institutions   mostly noise: acronyms, address fragments, non-Latin names
```

## Results

**The most represented.** Let's look at the result first, that is the answer to the question of the task. Among the countries, the United States lead with about 50,000 papers, then China with 28,000, the United Kingdom, Italy and Germany. The distribution is very concentrated: the first ten countries hold 62% of all the paper–country pairs.

Here you can see why we keep both counts. The `authors` column is the same count without the drop duplicates, so the ratio between the two is the average number of authors from that country on one paper. It goes from about 4 for Australia to more than 6 for China and Italy. And it's not a detail: if we ranked by authors, Italy would be above the United Kingdom, because Italian papers have more co-authors. By papers, the United Kingdom is third and Italy fourth. So the metric changes the answer, and we chose papers because one paper with forty Italian authors is still one paper.

Among the institutions, the top five are the University of California, Oxford, Harvard Medical School, Toronto and the Chinese Academy of Sciences. The University of California is first also because every affiliation that writes only "University of California", without the campus, falls on the same key.

**Figure A11 · papers vs authors: the metric changes the ranking**  ·  notebook plot: *the most represented countries and institutions*

```text
 top 10 countries, ranked by papers                    authors
 rank   country           papers     authors     per paper
 ────   ──────────────    ───────    ───────     ─────────
 1      United States      50,351    257,902       5.1
 2      China              28,398    180,235       6.3
 3      United Kingdom     21,461     95,983       4.5     ┐ ranked by authors,
 4      Italy              16,864    103,302       6.1     ┘ Italy would pass the United Kingdom
 5      Germany            12,218     56,607       4.6
 6      Canada             10,207     41,826       4.1
 7      India              10,101     40,377       4.0
 8      Spain               9,284     47,914       5.2
 9      France              8,675     44,179       5.1
 10     Australia           8,532     32,493       3.8

 the top 10 hold 62.0% of all 284,042 (paper, country) pairs

 top 5 institutions by papers
 University of California 3,521 · University of Oxford 2,169 · Harvard Medical School 1,971 ·
 University of Toronto 1,857 · Chinese Academy of Sciences 1,763
```

**The least represented.** At the other end of the ranking, the two questions don't have the same quality of answer. For countries the tail is short and it means something: out of 206 countries, 16 appear in a single paper, and they are small states and territories, like Tonga, Kosovo or Trinidad and Tobago. That is a real result. There is one entry to read carefully: Hong Kong, with one paper. The authors from Hong Kong almost always write China as their country, so they are counted under China: the ranking follows what the papers declare.

For institutions, instead, the tail is the distribution itself: 68,126 institutions out of about 100,000, that is 67.6%, appear in exactly one paper. So a bottom 20 would just be twenty names taken in alphabetical order from a tie of 68,000. That's why in the notebook, on the right, we show the shape of the distribution: how many institutions have one paper, how many have two, and so on, on log-log axes. It is almost a straight line: very few institutions with thousands of papers, only 17 above a thousand, and a huge number with just one. What stays in that tail is mostly noise that our key cannot fix: acronyms, fragments of addresses, names in non-Latin alphabets. Merging those would need a dictionary of institutions, that is entity resolution, which is out of the scope of the project. The script still writes the bottom 20 plot, because the assignment asks for it.

And one last caveat: we know the country for 214,799 papers out of 970,836. So "least represented" means least represented among the papers where we can see an affiliation.

**Figure A12 · The least represented: a real tail for countries, noise for institutions**  ·  notebook plot: *the other end*

```text
 COUNTRIES · 206 in the ranking                    INSTITUTIONS · 100,838 in the ranking

 16 appear in one single paper                     68,126 appear in one single paper   (67.6%)
   Anguilla, Bermuda, Cabo Verde, Kosovo,
   Moldova, Tonga, Trinidad and Tobago, …          papers per institution    institutions
 → small states and territories                    exactly 1                       68,126
 → a real finding                                  100 or more                        763
                                                   1,000 or more                       17

 ⚠ Hong Kong: 1 paper                              log-log axes: almost a straight line
   author rows mentioning Hong Kong                → a few giants and a very long tail
   in the institution or the city: 14,621
     country = China       8,987                   a "bottom 20" = an alphabetical slice
     country = Hong Kong       3                   of a 68,126-way tie
     no country            5,490                   → we show the distribution instead
   → counted under China
                                                   what is left in the tail: acronyms ("CAS"),
 coverage: a country is known for                  address fragments, non-Latin names
 214,799 of the 970,836 papers                     → merging them is entity resolution: out of scope
```

## Benchmarks

**The campaign.** The benchmarks were run on the Cloud Veneto cluster: five medium machines, one only for the scheduler and four workers with two cores and 4 GB each, so eight cores in total. We change three things: the number of partitions k, that is how many groups of files we make; the number of workers; and the number of threads per worker. What we time is only the single compute of the four Series, which is the distributed part. Writing the CSVs and the plots is pandas on the driver: it costs the same in every configuration, and inside the timer it would only add a constant that flattens the curves.

We always compare with a baseline: the same work done with pandas on a single core of the same machine, which takes 14.15 seconds. This matters because the data are only 34 MB, so the question is not only how Dask scales, but whether distributing is worth it at all.

The repetitions are full passes of the whole campaign, not the same point measured several times in a row, so the error bars also include how the machines change over time. And we start one cluster for each shape, workers times threads, and we measure every k inside it, because starting an `SSHCluster` takes about 40 seconds, much more than the job itself. Between one cluster and the next we wait ten seconds, otherwise the new scheduler finds its port still busy and doesn't start.

To measure processes against threads we don't change the code. `SSHCluster` starts one worker for each entry of the host list, so if we write the four worker machines twice, we get two worker processes on each machine, with one thread each. In that case we also give each worker a memory limit of 1.7 GB, about half of the usual one: by default each worker takes a fraction of the machine's RAM, and two workers on the same machine would promise more memory than the machine has.

**Figure A13 · The campaign: what we change, what we time, what we compare to**

```text
 cluster    5 × cloudveneto.medium  =  1 scheduler + 4 workers · 2 cores and 4 GB each  →  8 cores
 data       silver/authors · 192 files · 34 MB · 2,943,737 rows

 knobs      k                    partitions = groups of files · split_evenly(files, k)     1 … 192
            workers              worker processes                                           1 … 8
            threads per worker   1 or 2

 timed      client.compute(the four lazy Series)           only the distributed work
            CSVs and plots are pandas on the driver: the same cost everywhere → left out
 baseline   the same work in pandas on ONE core of the same VM             14.15 s  (±1.5%, 21 runs)

 165 measurements · 0 errors · repetitions = full passes of the campaign
 one cluster per shape (workers × threads), every k measured inside it · 10 s pause between clusters

 8 processes × 1 thread on 4 machines, without touching the code:
   CORD19_HOSTS = "scheduler,W,W"   (W = the 4 worker machines)   → 2 worker processes per machine
   CORD19_WORKER_MEMORY_LIMIT = 1.7GB                             → the two fit in the 4 GB of one VM

 measurements per configuration
 workers × threads   k = 1    2    4    8   16   32   64  128  192
 4 × 2                   6    6    6    6    9    6    6    6    6    ← partitions curve + workers curve
 4 × 1                   3    3    3    3    6    3    3    3    3
 8 × 1                   3    3    3    3    6    3    3    3    3
 1, 2, 3 × 2                                 3                   6    ← workers curve only
```

**Runtime vs partitions.** This is the first mandatory curve: the same data, split into k partitions, with k from 1 to 192, for three configurations. The shape is a U: the minimum is in the middle, not at the edges.

On the left, with few partitions, there isn't enough to do in parallel. With one partition, one worker does the whole work and the others wait. You can also see it from the green curve, eight processes: up to k equal 4 it lies on top of the orange one, four processes, because with four partitions or fewer the extra processes have nothing to work on.

On the right, instead, every partition we add means more tasks: for each partition there is a read, a dropna, the map of `chiave`, the drop duplicates, the value counts and the shuffles, and all this for four Series. But the data are only 34 MB: at k equal 192 each partition has about 15,000 rows, so each task does almost nothing, and the fixed cost of scheduling it, sending it and collecting its result becomes bigger than the work inside it. That's why the time grows so fast after 64.

The minimum is at k equal 8, and that's the default in the code. For eight processes it's a clear minimum: k equal 16 costs 26% more, with error bars between 3 and 6%, so it's not noise. For four processes k equal 8 is the minimum, or equal to it within the noise. But careful: it's not the rule "one partition per process", because with four processes k equal 4 is worse than k equal 8. It's a measured number, not a formula, and that's why in the code it's a constant.

And the starting default, one partition per file, that is k equal 192, is the worst point of the curve: 41 seconds with the standard configuration, almost three times slower than pandas on one core.

**Figure A14 · Runtime vs number of partitions**  ·  notebook plot: *runtime vs number of partitions*

```text
 seconds, mean over the passes                                        pandas on one core: 14.15 s

 k                        1      2      4      8     16     32     64    128    192
 4 workers × 2 threads  15.56  12.46   7.63   6.77   6.62  10.12  13.16  27.16  41.09
 4 workers × 1 thread   10.42  11.50   7.53   6.35   6.53  10.71  13.87  28.46  42.41
 8 workers × 1 thread   11.52  11.08   7.72   3.91   4.92   6.35   7.57  16.83  25.98
                                               ▲ minimum                          ▲ starting default:
                                                                                    1 partition per file

 LEFT · too few partitions                     RIGHT · too many partitions
 k = 1: one worker works, the others wait      every partition adds tasks to every step:
 k ≤ 4: 8 × 1 ≈ 4 × 1                          read · dropna · chiave · drop_duplicates ·
   the extra processes have nothing to do      value_counts · shuffles     × 4 Series
                                               k = 192 → ≈ 15,000 rows per partition:
                                               scheduling a task costs more than its work

 8 × 1:   k = 8 → 3.91 ± 0.14 s     k = 16 → 4.92 ± 0.30 s      +26%: not noise
 4 × 1:   k = 4 → 7.53 s  >  k = 8 → 6.35 s      → k = 8 is measured, not "one partition per process"
 4 × 2 at k = 192:  41.09 s  =  2.9 × pandas on one core
```

**Runtime vs workers.** The second mandatory curve: from one to four workers, with two threads each and k equal 16, which for this configuration is in the flat bottom of the previous curve: with two threads, k equal 8 and k equal 16 are the same within the noise. The time goes from 19 seconds to 6.6, so four workers give a speedup of 2.9, with an efficiency of 0.72. It's not linear because a part of the job doesn't get smaller when we add workers: the shuffles have to move data between the workers, and the scheduler has to coordinate every task.

But there is one thing to say honestly. One worker with two threads takes 19 seconds, which is slower than pandas on one core. So the reference point of the speedup already pays the cost of Dask. If we compare with pandas instead, four workers are 2.1 times faster, not 2.9.

**Figure A15 · Runtime vs number of workers (k = 16, 2 threads per worker)**  ·  notebook plot: *runtime vs number of workers*

```text
 workers            1        2        3        4
 seconds        19.19    11.34     7.88     6.62          pandas on one core: 14.15 s
 speedup         1.00     1.69     2.43     2.90          = T(1 worker) / T(n workers)
 efficiency      1.00     0.85     0.81     0.72          = speedup / n

 why not linear   the shuffles move data between workers and the scheduler coordinates every task:
                  that part of the job does not shrink when we add workers

 read it honestly 1 worker × 2 threads = 19.19 s  →  already SLOWER than pandas on one core
                  against pandas, 4 workers give  14.15 / 6.62 = 2.14×,  not 2.90×

 why k = 16       with 2 threads per worker, k = 8 and k = 16 are equal within the noise (6.77 vs 6.62 s)
```

**Processes vs threads.** Now the comparison between processes and threads, which in Dask are two ways of using the same cores: more worker processes, or more threads inside the same worker. Here all the configurations are at k equal 16.

The key reading is this. Start from four workers with one thread, that is four cores. If we double the cores by giving a second thread to each worker, the time doesn't change: 6.5 against 6.6 seconds. If instead we double them by starting more processes, eight workers with one thread, the time goes down to 4.9 seconds, 1.33 times faster. Same hardware added, two opposite results. And at equal cores the processes always win: with eight cores, on the same four machines, eight processes beat four processes with two threads by 1.35 times.

The reason is the GIL, the global interpreter lock: inside a Python process only one thread at a time can execute Python code. The work we do on every row is `chiave`: regular expressions, lowercase, the unicode normalization and a loop over the characters. It's all Python, it holds the lock, so two threads in the same worker just take turns. Reading the Parquet with Arrow, which is C++, could release the lock, but on 34 MB it's a small part of the job. And it's not memory: eight processes and four processes with two threads have the same memory per core, 1.7 GB.

You can see the same thing in the partitions plot: the curves with one and with two threads per worker lie on top of each other for every k, even though one of them uses half the cores. The only exception is k equal 1, where the second thread even makes it slower. It's the same conclusion as in the word count: on this kind of work, the useful unit is the process, not the thread.

**Figure A16 · Processes vs threads at equal cores (k = 16)**  ·  notebook plot: *processes vs threads, at k=16*

```text
 workers × threads   cores   seconds   vs pandas on one core
 ─────────────────   ─────   ───────   ─────────────────────
 1 × 2                   2     19.19          0.74×
 2 × 2                   4     11.34          1.25×
 3 × 2                   6      7.88          1.79×
 4 × 1                   4      6.53          2.17×
 4 × 2                   8      6.62          2.14×
 8 × 1                   8      4.92          2.88×

 start from 4 × 1 (4 cores) and double the cores
   as a 2nd THREAD per worker    4 × 1 → 4 × 2     6.53 → 6.62 s     0.99×   nothing
   as more PROCESSES             4 × 1 → 8 × 1     6.53 → 4.92 s     1.33×

 at equal cores, processes always win
   8 cores, same 4 machines    8 × 1  vs  4 × 2     4.92  vs   6.62 s     1.35×
   4 cores                     4 × 1  vs  2 × 2     6.53  vs  11.34 s     1.74×

 the cause: the GIL = one lock per Python process → one thread at a time runs Python code
   chiave() on every row:  re.sub · lower() · unicodedata.normalize · loop over the characters
   → pure Python, holds the lock → two threads in one worker take turns
   Parquet reading (Arrow, C++) could release it, but on 34 MB it is a small part of the job
   not memory: 8 × 1 and 4 × 2 have the same memory per core (1.7 GB)

 in the partitions plot, 4 × 1 and 4 × 2 overlap at every k   (exception k = 1: 10.42 vs 15.56 s)
```

**The practical consequence.** Putting the two knobs together. The configuration we started from, four workers with two threads, which is the default of `SSHCluster`, with one partition per file, took 41 seconds. Eight processes with one thread and k equal 8 take 3.9 seconds. Same hardware, same code, 10.5 times faster, only by choosing the number of partitions and using processes instead of threads. And against pandas on one core we are 3.6 times faster.

So the lesson of these benchmarks is that, on a small dataset like this one, how we use the cluster matters more than its size: with the default configuration, half of the cores were only waiting for the lock.

**Figure A17 · The two knobs together: same hardware, same code**

```text
                                                                          seconds
 starting point   4 workers × 2 threads  (SSHCluster default)  · k = 192    41.09
 best point       8 workers × 1 thread                          · k = 8       3.91
                                                                          ───────
                                                                   10.5× faster

 against pandas on one core:   14.15 / 3.91 = 3.6× faster, with 8 cores

 with the default 4 × 2, only about 4 of the 8 cores did useful work: 4 × 2 ≈ 4 × 1
```
