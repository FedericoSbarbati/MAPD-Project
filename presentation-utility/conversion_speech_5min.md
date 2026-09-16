# Conversion speech · 5-minute version

Figures refer to `conversion_speech.md`. Timings assume a calm pace (~130 words per minute).

## 1 · Raw data, two layers  (≈ 45 s) · Figure B2

The raw data come in two parts. `metadata.csv` has roughly one row per paper, about a million rows, and it holds the paths to the parsings of each paper: JSON files, one nested object per parsing, with the authors and the paragraphs of the text. A paper can have zero, one or more PDF parsings, and at most one PMC parsing.

We convert everything to Parquet, a columnar format, in two layers. **Bronze** only changes the format: nothing is cleaned. **Silver** does the cleaning: deduplication, normalization, and choosing which parsing to keep. The result is a small relational model, and that is what the four tasks read.

## 2 · Bronze: one pattern for three tables  (≈ 1 min 30 s) · Figure B8

The three bronze pipelines, papers, paragraphs and authors, share one pattern: read `metadata.csv` into a Dask DataFrame, repartition it, apply an extraction function with `map_partitions`, and write with `to_parquet`. Everything is lazy until `to_parquet`, which triggers the computation.

For papers there is nothing to extract: it is the CSV rewritten as Parquet. For paragraphs and authors, each worker opens the JSON files of its own rows and returns one row per paragraph, or one row per author.

The point to notice is that a map is one partition in, one partition out. The rows multiply, from one million to 23 million paragraphs, but the partitions don't: 1024 partitions in, 1024 files out. And these files are very unbalanced, because papers have very different lengths.

Two choices when writing. **Types**: every worker writes its own file and only sees its own data, so types inferred locally can differ between files, for example a column that happens to be empty in one partition, and then the dataset can't be read back as one table. So we fix them ourselves: `meta` tells Dask what each pandas partition looks like in RAM, and the Arrow `schema` fixes the types on disk. **Row groups**: inside each file, the rows are cut into blocks of at most 20,000. We'll need them in silver.

## 3 · Silver: a row needs to know something global  (≈ 2 min 30 s) · Figures S1 (bottom table), S4, S9

In silver every rule has the same problem: a worker only sees its own partition, but to decide what to do with a row it needs to know something about the whole dataset. We solve it in two ways.

**Papers: move the rows.** The same `cord_uid` can appear in several rows, and we keep the richest one, the one with more parsings. Duplicates can sit in different partitions, so first we **shuffle** on `cord_uid`, which sends all its rows to the same partition, and then `map_partitions` keeps the best row in each partition. Without the shuffle, duplicates in different partitions would survive, and nothing would tell us. This removes about 86,000 rows.

**Authors: move the information.** Country names are dirty: 8,710 distinct spellings. We compute only the distinct values and bring them to the **driver**, the process that launches the job, where we build a dictionary from raw name to ISO3 code. The dictionary is small, so it is sent to the workers, and each partition applies it with a plain map, without any communication between workers. From this table we build the two rollup tables, one row per paper–country and one per paper–institution pair, so a paper counts once for a country, not once per author.

**Paragraphs: the same idea, plus memory.** When a paper has both parsings, we keep the PMC one. So we compute on the driver the set of the 315,000 papers that have a PMC parsing, we send it to the workers, and each partition filters its own rows.

Here the row groups pay off: with `split_row_groups=True` every row group becomes a partition, so 1024 unbalanced files become 1979 partitions of at most 20,000 rows. But the unmanaged memory of the workers, memory that Dask doesn't track and can't free, kept growing with every partition. So we process 448 partitions at a time and restart the client between blocks, which brings the memory back down. The final table has 12.4 million paragraphs, 46% fewer than in bronze.

## Closing  (≈ 10 s)

So the four tasks never touch the JSON: they read typed, balanced Parquet tables, where every row has one clear meaning.
