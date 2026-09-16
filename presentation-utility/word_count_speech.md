# Word count speech

Ok, now let's do the speech on the word count.

**Figure W1 · The whole word count pipeline**

```text
 silver/paragraphs   1979 Parquet files · 12,445,234 paragraphs
       │
       │ paragraph_files()        → list of Paths, sorted by partition number
       │ split_evenly(files, k)   → k groups of file paths                ← k is the benchmark knob
       ▼
 db.from_sequence(groups, partition_size=1)
       │      ┌──────────────────────────────────────────────────┐
       │      │ the Bag contains ONLY FILE PATHS (strings)       │
       │      │ k partitions, 1 group each                       │
       │      └──────────────────────────────────────────────────┘
       │ .map_partitions(load_group)        ← pq.read_table ON THE WORKER, 2 columns out of 7
       ▼
 Bag of (cord_uid, text)                                                   k partitions
       │
       │ ══ MAP PHASE ══   .map_partitions(document_counts)
       │    defaultdict(Counter): counts INSIDE the partition, per document
       │    words(text) = sanitize → tokenize → filter
       ▼
 Bag of ((cord_uid, word), count)                                          k partitions · no shuffle
       │
       │ ══ REDUCE PHASE ══   two ways, chosen by split_out
       │
       ├─ split_out=0  ─▶ .foldby(word_of, add_count, initial=0, combine=operator.add)
       │                                                             ─▶  1 partition
       │
       └─ split_out=16 ─▶ .map(word_and_count)                           ← drops cord_uid
                          .to_dataframe(meta={"word": "string", "count": "int64"})
                          .groupby("word")["count"].sum(split_out=16)   ← shuffle method: tasks
                          .reset_index().to_bag(format="tuple")      ─▶  16 partitions
       ▼
 Bag of (word, count)
       │ .persist()                                     ═══ TRIGGER: Map and Reduce run once
       ├──▶ .topk(20, key=1).compute()      ─▶ top_words.csv + top_words.png
       └──▶ .to_dataframe().to_parquet()    ─▶ word_counts/   (6,037,808 distinct words)
```

**Reading.** The pipeline for the word count is this. We read the silver paragraphs, which are split into 1979 Parquet files, and we call the `paragraph_files` function. This function takes the list of file paths and sorts them by extracting the partition number from the file name, in ascending order: it goes partition 0, 1, 2, 3, and not in alphabetical order, where `part.10` would come before `part.2`.

**Figure W2 · Alphabetical order vs the order of paragraph_files**

```text
 alphabetical order                          paragraph_files(): by partition number
 ──────────────────────                      ──────────────────────────────────────
 part.0.parquet                              part.0.parquet
 part.1.parquet                              part.1.parquet
 part.10.parquet       ← 10 before 2         part.2.parquet
 part.100.parquet                            part.3.parquet
 part.1000.parquet                           …
 part.2.parquet                              part.1978.parquet
```

Then we don't use the native `dd.read_parquet` function, because it wouldn't let us choose the number of partitions, which for us is a benchmark variable. The number would be chosen automatically by Dask: for example, with `read_parquet` followed by `to_bag`, on 1979 files Dask was running 990 partitions. And we don't do a repartition after reading either, because the reading would always stay at the same granularity, and the measured time would also include the cost of stitching the partitions back together, which changes with k. We want to have control, so we use the `split_evenly` function, with the number of partitions k as argument, which essentially pre-creates k groups: not exactly groups of files, but groups of paths to the files. Each of these groups contains a set of paths to the files to read, and the groups have almost the same number of files. If we don't choose k, it is 1979, that is one group per file.

**Figure W3 · Who decides the number of partitions**

```text
 dd.read_parquet(…).to_bag()          Dask decides      1979 files → 990 partitions actually run
 dd.read_parquet(…) + repartition(k)  reading stays at 1979 files; re-stitching (depends on k)
                                                        ends up in the measured time
 split_evenly(files, k)               WE decide         exactly k partitions

 example: 1979 files, k = 4
   group 0   part.0.parquet    …  part.493.parquet     494 files   → partition 0
   group 1   part.494.parquet  …  part.988.parquet     495 files   → partition 1
   group 2   part.989.parquet  …  part.1483.parquet    495 files   → partition 2
   group 3   part.1484.parquet …  part.1978.parquet    495 files   → partition 3
 k not given  →  k = 1979, one group per file
```

Then we call `db.from_sequence`. We have the `groups` sequence, which contains k groups, where each group is a list of file paths, and we pass `partition_size=1` as argument, meaning each group becomes a partition, so we get k partitions. At this point we have a Bag that contains only the path strings, split into k partitions. On each partition of this Bag we apply, through `map_partitions`, the `load_group` function, which calls PyArrow's `read_table` on the `cord_uid` and `text` columns. This way each partition reads only the group of files it contains, and we get a Bag of `(cord_uid, text)` pairs, where `text` is the paragraph. All of this is lazy: the files are actually read by the workers only when the computation starts.

**Figure W4 · What the Bag contains, before and after load_group**

```text
 db.from_sequence(groups, partition_size=1)                                  k partitions
 ┌─ partition 0 ─────────────────────────────────────────────────────────────┐
 │ [ ["…/part.0.parquet", "…/part.1.parquet", …, "…/part.493.parquet"] ]     │  ← ONE element:
 └───────────────────────────────────────────────────────────────────────────┘    a list of paths
 ┌─ partition 1 ─────────────────────────────────────────────────────────────┐
 │ [ ["…/part.494.parquet", …, "…/part.988.parquet"] ]                       │
 └───────────────────────────────────────────────────────────────────────────┘
        │
        │ .map_partitions(load_group)             lazy · runs on the worker when the computation starts
        │   pq.read_table(group, columns=["cord_uid", "text"])
        ▼
 ┌─ partition 0 ─────────────────────────────────────────────────────────────┐
 │ [ ("uid_A", "paragraph text …"), ("uid_A", "…"), ("uid_B", "…"), … ]      │  ← one element
 └───────────────────────────────────────────────────────────────────────────┘    per paragraph

 silver/paragraphs columns
   read:      cord_uid · text
   not read:  paper_id · source · para_idx · section · is_reference_like
```

**Map.** Now the Map phase of the assignment starts. Through `map_partitions` we apply the `document_counts` function on each partition. Each partition contains `(cord_uid, text)` pairs: they are not rows, because a Bag has no columns and no schema, it is a collection of Python objects.

`document_counts` runs a for loop over each `cord_uid`, `text` pair, and for each document, identified by its `cord_uid`, it updates a `Counter` with the words returned by `words(text)`. So, for each paragraph, `words` splits the text into words, and that document's `Counter` updates their occurrences. At the end the function returns a list of nested pairs `((cord_uid, word), count)`, which associates a word inside a paper with the number of times it appears in that paper. This, however, is done only on the paragraphs that are inside that specific partition. And since it is a map, there are still k partitions and there is no shuffle.

**Figure W5 · document_counts on one partition (real output of the function)**

```text
 input partition
   ("doc_A", "COVID-19 patients were treated.")
   ("doc_A", "Patients with COVID-19 recovered.")
   ("doc_B", "The patients recovered.")
        │
        │ per_document = defaultdict(Counter)
        │ for cord_uid, text in partition:
        │     per_document[cord_uid].update(words(text))
        ▼
 per_document                                          (lives only inside this task)
   doc_A → Counter({ covid-19: 2, patients: 2, treated: 1, recovered: 1 })
   doc_B → Counter({ patients: 1, recovered: 1 })
        │
        │ flattened into a list of nested pairs
        ▼
 output partition
   (("doc_A", "covid-19"),  2)    (("doc_A", "patients"),  2)    (("doc_A", "treated"), 1)
   (("doc_A", "recovered"), 1)    (("doc_B", "patients"),  1)    (("doc_B", "recovered"), 1)

 "were", "with", "the" are stopwords → not counted
```

**What is a word.** Here we need to stop on what `words` does, because that's where we decide what counts as a word. First the text is sanitized by the `sanitize` function, in four steps.

**Figure W6 · From a paragraph to its words (real output of sanitize and words)**

```text
 input paragraph
   COVID–19 patients (Müller et al., Fig. 2) received 5 µg/ml of TNF-α \documentclass[12pt]{minimal}
   \usepackage{amsmath}\begin{document}$x_1$\end{document} in 2020.

 sanitize(text)
   1. LaTeX block → " "   COVID–19 patients (Müller et al., Fig. 2) received 5 µg/ml of TNF-α   in 2020.
   2. NFKC                COVID–19 patients (Müller et al., Fig. 2) received 5 μg/ml of TNF-α   in 2020.
                                                                            µ (micro sign) → μ (Greek mu)
   3. lowercase           covid–19 patients (müller et al., fig. 2) received 5 μg/ml of tnf-α   in 2020.
   4. punctuation         covid-19 patients (müller et al., fig. 2) received 5 μg/ml of tnf-α   in 2020.
                          – (en dash) → - (hyphen)

 TOKEN.findall(…)
   covid-19 · patients · müller · et · al · fig · received · μg/ml · of · tnf-α · in
   ("5", "2", "2020" are not tokens: a word must start with a letter)

 filter: length ≥ 2 and not in IGNORED
   et · al · fig   → ARTIFACTS
   of · in         → STOPWORDS

 words(text)
   covid-19 · patients · müller · received · μg/ml · tnf-α
```

First, the LaTeX formulas are removed. In PMC paragraphs every inline formula carries a LaTeX version with the whole preamble, from `\documentclass` to `\end{document}`. With a regular expression we remove the entire block and replace it with a space. Without this step, `usepackage` was the tenth word of the corpus. By removing the whole block we get rid, in one go, of the whole family of junk words, like `amsmath` or `documentclass`, instead of chasing them one by one in the stopwords.

**Figure W7 · The LaTeX block removed in one go**

```text
 LATEX_FORMULA = \\documentclass.*?\\end\{document\}        flags: DOTALL (the block can span lines)

 … received \documentclass[12pt]{minimal}                ┐
            \usepackage{amsmath} \usepackage{wasysym}    │  whole block → " "
            \begin{document} $x_1$ \end{document} in …   ┘  .*? stops at the FIRST \end{document}

 without it:  usepackage was the 10th word of the corpus (255,743 occurrences)
              documentclass · amsmath · wasysym · upgreek · setlength · pt · … all counted as words
 only PMC paragraphs contain these blocks
```

Second, the text is normalized to Unicode NFKC, meaning that characters that are the same letter written in different ways are rewritten in a single form: for example, the micro sign and the Greek letter mu become the same character. Third, everything is lowercased. Fourth, typographic punctuation is normalized: the various dashes, like the non-breaking hyphen, the en dash, the em dash and the minus sign, become the normal hyphen, and curly apostrophes and quotes become straight ones. This is needed because the en dash is the most frequent non-ASCII character in the corpus, and `covid–19` written with an en dash wouldn't be recognized as `covid-19`.

**Figure W8 · The PUNCTUATION substitution table**

```text
 ‐  ‑  ‒  –  —  ―  −     hyphen, non-breaking hyphen, figure dash, en dash, em dash,     →   -
                         horizontal bar, minus sign
 ‘  ’  ‚  ‛              curly apostrophes                                                 →   '
 “  ”  „                 curly quotes                                                      →   "

 en dash (U+2013) = the most frequent non-ASCII character in the corpus (286,687 occurrences)
 covid–19  →  covid-19
```

Then we define which characters are valid: the lowercase alphabet from a to z, lowercase accented Latin letters and lowercase Greek letters. With these we build the regular expression that defines a valid word: it must start with a letter, then it can continue with letters or digits, and it can contain hyphens or slashes, as long as they are followed by more letters or digits. This way `covid-19`, `sars-cov-2` or `μg/ml` stay a single word, while numbers on their own are not counted. With a naive pattern, only from a to z, `covid-19` would be split into `covid` and `19`, with the 19 thrown away, and `müller` would become `m` and `ller`.

**Figure W9 · Valid characters and the token pattern**

```text
 LETTER = a-z  à-ö  ø-þ  α-ω
          │    │    │    └─ lowercase Greek letters
          │    │    └────── more lowercase Latin letters (ø … þ)
          │    └─────────── lowercase accented Latin letters (à … ö)   (÷ sits between ö and ø: excluded)
          └──────────────── lowercase ASCII letters

 TOKEN = [LETTER] [LETTER 0-9]* ( [-/] [LETTER 0-9]+ )*
         │        │              └─ zero or more pieces: a hyphen or slash, then letters or digits
         │        └──────────────── then letters or digits
         └───────────────────────── starts with a letter

 text          naive [a-z]+              TOKEN
 ───────────   ───────────────────────   ──────────
 covid-19      covid          (19 lost)  covid-19
 sars-cov-2    sars · cov     (2 lost)   sars-cov-2
 müller        m · ller                  müller
 tnf-α         tnf            (α lost)   tnf-α
 μg/ml         g · ml                    μg/ml
 2020          (lost)                    (not a word)
```

Finally we filter the words we found. We keep only those at least two characters long, to remove the single letters left behind by formulas and initials, and we discard the ones that belong to two sets. The stopwords, which are only English function words, like `the`, `and`, `of`: they are a property of the language. And the artifacts, which instead are noise from this corpus: `et` and `al`, which come from "et al." and were the third and fourth word, and the references to figures and tables, like `fig` or `table`. The rule we gave ourselves is not to remove content words, not even generic ones like `study` or `data`, because that is an analysis decision and not a cleaning one. And we don't use the `is_reference_like` flag from silver: we measured it, the flagged paragraphs weighed less than 1% of the text, and removing them didn't change the top 20 words.

**Figure W10 · The filter**

```text
 MIN_LENGTH = 2                     drops single letters left by formulas and initials

 IGNORED = STOPWORDS ∪ ARTIFACTS
 ┌──────────────────────────────────────────┬──────────────────────────────────────────────────┐
 │ STOPWORDS                                │ ARTIFACTS                                        │
 │ property of the English language         │ noise of THIS corpus                             │
 │ the · and · of · with · were · which · … │ et · al · fig · figs · figure · figures ·        │
 │ (function words only)                    │ table · tables · eq · eqn · respectively         │
 │                                          │ et, al were the 3rd and 4th word                 │
 │                                          │ (347,632 and 343,098 occurrences: "et al.")      │
 └──────────────────────────────────────────┴──────────────────────────────────────────────────┘

 kept on purpose   content words, even generic ones: study · data     → analysis decision, not cleaning
 not used          is_reference_like: flagged paragraphs = 0.74% of the text, top 20 unchanged
```

**Reduce.** Now the Reduce phase starts, and we have two different ways. The variable that controls it is `split_out`, which is essentially the number of output partitions of the Reduce, into which we split the word–count pairs.

If `split_out` is equal to zero, we use a `foldby`, which groups by word and sums the counts of every document and every partition. The `foldby` uses as grouping key the `word_of` function, which extracts the word from the nested object. The binary operation is the `add_count` function, defined by us, which inside each partition, starting from zero, adds to that word's total the count of each document it appears in. So logically it is equivalent to removing `cord_uid` and keeping only word and count, which repeats because the same word can appear in several papers or several paragraphs inside the partition. Instead, to combine the results across the different partitions, `operator.add` is used as combine, which sums the totals obtained from all the partitions.

**Figure W11 · split_out=0: foldby**

```text
 doc_counts.foldby(key=word_of, binop=add_count, initial=0, combine=operator.add)

 partition 0                                      partition 1
 (("A", "covid-19"), 2)                           (("C", "covid-19"), 5)
 (("A", "patients"), 2)                           (("C", "patients"), 1)
 (("B", "patients"), 1)
        │                                                │
        │ key = word_of → the word                       │
        │ binop = add_count: total + count, from 0       │
        ▼                                                ▼
 { covid-19: 2, patients: 3 }                     { covid-19: 5, patients: 1 }
        │                                                │
        └──────────────── combine = operator.add ────────┘
                                   ▼
                    { covid-19: 7, patients: 4 }               ← ONE output partition
```

The problem with `foldby` is that, like all group-by-like operations on a Bag, it always reduces to a single partition: the whole vocabulary, more than 6 million words, has to fit in a single task on a single worker. So the tail of the computation is serial, and that task grinds through a Python dictionary with millions of entries. On the real cluster `foldby` takes 1091 seconds, against 498 for the version with `split_out`.

**Figure W12 · Why foldby is slow: everything ends in one task**

```text
 split_out = 0 (foldby)                                 split_out = 16

 worker 1  ██ ██ ██ ██ ████████████████████             worker 1  ██ ██ ██ ██ ███
 worker 2  ██ ██ ██ ██                                  worker 2  ██ ██ ██ ██ ███
 worker 3  ██ ██ ██ ██      (idle)                      worker 3  ██ ██ ██ ██ ███
 worker 4  ██ ██ ██ ██                                  worker 4  ██ ██ ██ ██ ███
           └── Map ──┘└── one task: a Python dict ──┘             └── Map ──┘└ 16 reduce tasks ┘
                         with 6,037,808 words
 (schematic)

 real cluster · 4 workers
   foldby            1,091 s
   split_out = 16      498 s
```

When instead we set `split_out` equal to 16, the matter is a bit more subtle, because a Bag doesn't allow splitting the result of a reduce into several partitions, while the DataFrame group by does, with the `split_out` argument. So we move to a DataFrame. First, though, with `word_and_count` we remove `cord_uid`: this way each element goes from a nested pair `((cord_uid, word), count)` to a flat pair `(word, count)`, which can be converted into two columns. Then we convert the Bag into a DataFrame, and the sums are done by pandas, with Arrow strings, in a vectorized way instead of with a Python dictionary. We do the group by on the word with `split_out=16`, getting 16 output partitions from the Reduce, and then we convert the result back into a Bag, so that the function always returns a Bag of `(word, count)`, in both cases. `split_out` defaults to 16 and it's a command-line option: with 0 we go back to `foldby`, and that's how we compare the two approaches in the benchmarks.

**Figure W13 · split_out=16: a detour through the DataFrame**

```text
 Bag  ((cord_uid, word), count)                                   k partitions
   │
   │ .map(word_and_count)             (("A", "covid-19"), 2)  →  ("covid-19", 2)      nested → flat
   ▼
 Bag  (word, count)
   │
   │ .to_dataframe(meta={"word": "string", "count": "int64"})
   ▼
 DataFrame                                                         k partitions
 ┌────────────┬───────┐
 │ word       │ count │      pandas partitions · Arrow strings · vectorized sums
 ├────────────┼───────┤
 │ covid-19   │ 2     │
 │ patients   │ 2     │
 │ …          │ …     │
 └────────────┴───────┘
   │
   │ .groupby("word")["count"].sum(split_out=16)                   shuffle method = "tasks"
   ▼
 DataFrame                                                         16 partitions
   each word lands in exactly one of the 16
   │
   │ .reset_index().to_bag(format="tuple")
   ▼
 Bag  (word, count)                                                16 partitions
   same type as the foldby branch → word_count() always returns a Bag

 command line:  --split-out 16 (default)   |   --split-out 0 → foldby, used to compare the two
```

The shuffle method, in this case, is `tasks`. We don't use the standard one, P2P, where the workers exchange data directly outside the graph, because when the graph comes from a Bag converted into a DataFrame, P2P fails. So we replace it with `tasks`, which writes the shuffle inside the computational graph explicitly, as normal tasks.

**Figure W14 · P2P vs tasks**

```text
 ┌──────────────────────────────────────────┬──────────────────────────────────────────┐
 │ P2P  (Dask's default)                    │ tasks  (our choice)                      │
 ├──────────────────────────────────────────┼──────────────────────────────────────────┤
 │ workers exchange the data directly,      │ the shuffle is written inside the graph  │
 │ outside the task graph                   │ as ordinary tasks                        │
 │                                          │                                          │
 │ graph born from a Bag → fails:           │ works with a Bag converted to DataFrame  │
 │ "P2P … failed during transfer phase"     │                                          │
 └──────────────────────────────────────────┴──────────────────────────────────────────┘
 dask.config.set({"dataframe.shuffle.method": "tasks"})
```

**Output.** At the end, in `main`, we `persist` the result: this triggers Map and Reduce and keeps the vocabulary in the workers' memory. From there we compute with `topk` the 20 most frequent words, without having to redo Map and Reduce. Then we write the full vocabulary to Parquet, and the top 20 words to a CSV and a bar plot. On the whole corpus there are about 786 million occurrences and 6 million distinct words, and the second most frequent word is precisely `covid-19`, which with naive tokenization wouldn't exist.

**Figure W15 · persist, the two outputs, and the result**

```text
 global_counts.persist()                ═══ TRIGGER · Map and Reduce run once
        │                                   the result stays in the workers' memory
        ├──▶ .topk(20, key=1).compute()     → back to the driver → top_words.csv · top_words.png
        └──▶ .to_dataframe().to_parquet()   → word_counts/  (full vocabulary, zstd)
 without persist, each of the two outputs would recompute Map and Reduce

 full corpus · 785,753,529 occurrences · 6,037,808 distinct words

 rank   word          occurrences
 ────   ───────────   ───────────
    1   patients        4,403,513
    2   covid-19        3,911,578   ← would not exist with a naive tokenizer
    3   study           3,280,997
    4   data            2,980,002
    5   using           2,359,949
    …
   18   sars-cov-2      1,488,035
```
