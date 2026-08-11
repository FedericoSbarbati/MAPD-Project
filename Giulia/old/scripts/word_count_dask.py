#!/usr/bin/env python3
"""
Distributed word count for the CORD-19 project, task 1.

Expected input: the silver Parquet table produced by the conversion workflow:

    data/silver/paragraphs

The table is expected to contain at least:
    - cord_uid: paper/document identifier
    - text: paragraph body text
Optionally:
    - is_reference_like: boolean flag for reference/boilerplate sections

The implementation mirrors the assignment's MapReduce shape without materializing full
documents as giant strings:
    1. map partitions into per-document word counts: (cord_uid, word, cp(word))
    2. reduce by (cord_uid, word) to finish the document-level counts
    3. reduce by word to obtain the global corpus counts
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
import unicodedata
from collections import Counter, defaultdict
from pathlib import Path
from typing import Iterable


DEFAULT_TOKEN_PATTERN = r"[a-z][a-z0-9]+(?:[-/][a-z0-9]+)*"

# Compact built-in English stopword list. The dataset is intentionally not
# tokenized upstream, so this belongs to the analysis task rather than to silver.
DEFAULT_STOPWORDS = frozenset(
    """
    a about above after again against all am an and any are as at be because been
    before being below between both but by can could did do does doing down during
    each few for from further had has have having he her here hers herself him
    himself his how i if in into is it its itself just me more most my myself no
    nor not now of off on once only or other our ours ourselves out over own same
    she should so some such than that the their theirs them themselves then there
    these they this those through to too under until up very was we were what when
    where which while who whom why will with you your yours yourself yourselves
    also al et fig figure table using use used may one two three however within
    without among across per
    """.split()
)

UNICODE_TRANSLATION = str.maketrans(
    {
        "\u2010": "-",
        "\u2011": "-",
        "\u2012": "-",
        "\u2013": "-",
        "\u2014": "-",
        "\u2015": "-",
        "\u2212": "-",
        "\u2018": "'",
        "\u2019": "'",
        "\u201c": '"',
        "\u201d": '"',
    }
)


def normalize_text(text: str) -> str:
    """Normalize punctuation variants while preserving scientific hyphenated terms."""
    return unicodedata.normalize("NFKC", text).lower().translate(UNICODE_TRANSLATION)


def tokenize(
    text: str,
    token_re: re.Pattern[str],
    stopwords: set[str],
    min_token_len: int,
) -> Iterable[str]:
    for token in token_re.findall(normalize_text(text)):
        if len(token) < min_token_len:
            continue
        if token in stopwords:
            continue
        yield token


def load_stopwords(path: str | None, disable_default: bool) -> set[str]:
    words: set[str] = set() if disable_default else set(DEFAULT_STOPWORDS)
    if not path:
        return words

    with open(path, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip().lower()
            if not line or line.startswith("#"):
                continue
            words.add(line)
    return words


def empty_count_frame(doc_col: str):
    import pandas as pd

    return pd.DataFrame(
        {
            doc_col: pd.Series(dtype="object"),
            "word": pd.Series(dtype="object"),
            "count": pd.Series(dtype="int64"),
        }
    )


def count_partition(
    pdf,
    doc_col: str,
    text_col: str,
    token_pattern: str,
    stopwords_tuple: tuple[str, ...],
    min_token_len: int,
):
    """Map phase for one pandas partition: paragraph rows -> document word counts."""
    import pandas as pd

    if pdf.empty:
        return empty_count_frame(doc_col)

    token_re = re.compile(token_pattern)
    stopwords = set(stopwords_tuple)
    by_doc: defaultdict[str, Counter[str]] = defaultdict(Counter)

    for doc_id, text in zip(pdf[doc_col], pdf[text_col]):
        if doc_id is None or not isinstance(text, str) or not text:
            continue
        by_doc[str(doc_id)].update(tokenize(text, token_re, stopwords, min_token_len))

    rows = [
        (doc_id, word, int(count))
        for doc_id, counts in by_doc.items()
        for word, count in counts.items()
    ]
    if not rows:
        return empty_count_frame(doc_col)

    return pd.DataFrame(rows, columns=[doc_col, "word", "count"]).astype(
        {doc_col: "object", "word": "object", "count": "int64"}
    )


def parquet_columns(path: str) -> set[str] | None:
    try:
        import pyarrow.dataset as ds

        return set(ds.dataset(path, format="parquet").schema.names)
    except Exception:
        return None


def is_relative_to(path: Path, parent: Path) -> bool:
    try:
        path.relative_to(parent)
        return True
    except ValueError:
        return False


def check_safe_paths(input_path: Path, output_dir: Path) -> None:
    """Avoid writing generated outputs inside the input dataset."""
    input_abs = input_path.expanduser().resolve()
    output_abs = output_dir.expanduser().resolve()

    if output_abs == input_abs or is_relative_to(output_abs, input_abs):
        raise SystemExit(
            "Refusing to write outputs inside the input Parquet dataset.\n"
            f"Input:  {input_abs}\n"
            f"Output: {output_abs}\n"
            "Use a separate output directory, for example reports/word_count."
        )

    if is_relative_to(input_abs, output_abs):
        print(
            "Warning: output directory is a parent of the input dataset. "
            "Prefer reports/word_count to keep generated files clearly separated."
        )

    if str(input_abs).startswith("/data/") and not str(output_abs).startswith("/data/"):
        print(
            "Warning: input is on /data but output is not. On Cloud Veneto, prefer "
            "writing reports on the persistent volume so results survive VM teardown."
        )


def grouped_sum(ddf, by: list[str], split_out: int):
    grouped = ddf.groupby(by)["count"]
    try:
        return grouped.sum(split_out=split_out).reset_index()
    except TypeError:
        return grouped.sum().reset_index()


def distributed_cluster_info(client):
    if client is None:
        return {"mode": "no_distributed"}

    try:
        info = client.scheduler_info()
        workers = info.get("workers", {})
        return {
            "mode": "distributed",
            "dashboard": getattr(client, "dashboard_link", None),
            "workers": len(workers),
            "threads": int(sum(w.get("nthreads", 0) for w in workers.values())),
            "memory_limit_bytes": int(sum(w.get("memory_limit", 0) for w in workers.values())),
        }
    except Exception as exc:
        return {"mode": "distributed", "cluster_info_error": str(exc)}


def start_dask_client(args):
    scheduler = args.scheduler or os.environ.get("DASK_SCHEDULER")
    if args.no_distributed or (scheduler and scheduler.lower() in {"sync", "synchronous", "threads"}):
        return None, None

    try:
        from dask.distributed import Client, LocalCluster
    except ModuleNotFoundError as exc:
        raise SystemExit(
            "Missing dependency 'dask.distributed'. Install it in the project env with:\n"
            "  pip install \"dask[complete]\" pyarrow matplotlib\n"
            "or run this script inside the VM/env prepared for the MAPD project."
        ) from exc

    if scheduler:
        client = Client(scheduler)
        return client, None

    dashboard_address = args.dashboard_address
    if isinstance(dashboard_address, str) and dashboard_address.lower() in {"none", "off", "false"}:
        dashboard_address = None

    cluster = LocalCluster(
        n_workers=args.workers,
        threads_per_worker=args.threads_per_worker,
        memory_limit=args.memory_limit,
        processes=True,
        dashboard_address=dashboard_address,
    )
    client = Client(cluster)
    return client, cluster


def write_barplot(top_df, output_path: Path, title: str) -> str | None:
    try:
        mpl_config = output_path.parent / ".matplotlib"
        mpl_config.mkdir(parents=True, exist_ok=True)
        os.environ.setdefault("MPLCONFIGDIR", str(mpl_config))

        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except Exception as exc:
        return f"Skipping plot because matplotlib is unavailable: {exc}"

    plot_df = top_df.sort_values("count", ascending=True)
    height = max(5, 0.34 * len(plot_df))
    fig, ax = plt.subplots(figsize=(11, height))
    ax.barh(plot_df["word"], plot_df["count"], color="#2f6f73")
    ax.set_title(title)
    ax.set_xlabel("occurrences")
    ax.set_ylabel("")
    ax.grid(axis="x", alpha=0.25)
    fig.tight_layout()
    fig.savefig(output_path, dpi=150)
    plt.close(fig)
    return None


def parse_args(argv: list[str]) -> argparse.Namespace:
    data_root = os.environ.get("CORD19_DATA", "data")
    reports_root = os.environ.get("CORD19_REPORTS", "reports")
    run_name = os.environ.get("WORDCOUNT_RUN_NAME", "word_count")
    cpu_count = os.cpu_count() or 2
    default_workers = max(1, min(4, cpu_count // 2))
    default_workers = int(os.environ.get("WORDCOUNT_WORKERS", str(default_workers)))
    default_threads = int(os.environ.get("WORDCOUNT_THREADS_PER_WORKER", "1"))
    default_memory = os.environ.get("WORDCOUNT_WORKER_MEMORY", "auto")

    parser = argparse.ArgumentParser(
        description="Distributed CORD-19 word count on data/silver/paragraphs."
    )
    parser.add_argument(
        "--input",
        default=str(Path(data_root) / "silver" / "paragraphs"),
        help="Input Parquet directory produced by the silver workflow. "
        "Defaults to $CORD19_DATA/silver/paragraphs or data/silver/paragraphs.",
    )
    parser.add_argument(
        "--output-dir",
        default=str(Path(reports_root) / run_name),
        help="Directory for top_words.csv, global counts Parquet, plot, and summary. "
        "Defaults to $CORD19_REPORTS/$WORDCOUNT_RUN_NAME or reports/word_count.",
    )
    parser.add_argument("--doc-col", default="cord_uid", help="Document id column.")
    parser.add_argument("--text-col", default="text", help="Paragraph text column.")
    parser.add_argument(
        "--reference-col",
        default="is_reference_like",
        help="Boolean column used by --exclude-reference-like.",
    )
    parser.add_argument(
        "--exclude-reference-like",
        action="store_true",
        help="Drop paragraphs flagged as references/acknowledgements/funding sections.",
    )
    parser.add_argument(
        "--top-n",
        type=int,
        default=50,
        help="Number of most frequent words to export and plot.",
    )
    parser.add_argument(
        "--min-token-len",
        type=int,
        default=2,
        help="Minimum token length after normalization.",
    )
    parser.add_argument(
        "--token-pattern",
        default=DEFAULT_TOKEN_PATTERN,
        help="Regex used to extract tokens. Defaults preserve hyphenated terms.",
    )
    parser.add_argument(
        "--stopwords",
        help="Optional newline-separated stopword file. Lines starting with # are ignored.",
    )
    parser.add_argument(
        "--no-default-stopwords",
        action="store_true",
        help="Disable the built-in English stopword list.",
    )
    parser.add_argument(
        "--write-document-counts",
        action="store_true",
        help="Also write the intermediate (cord_uid, word, count) table. This can be large.",
    )
    parser.add_argument(
        "--split-out",
        type=int,
        default=32,
        help="Dask groupby split_out for distributed shuffle/reduce.",
    )
    parser.add_argument(
        "--scheduler",
        help="Existing Dask scheduler address, e.g. tcp://10.67.22.x:8786. "
        "Defaults to DASK_SCHEDULER env var or a local cluster. Use 'synchronous' "
        "for local Dask execution without dask.distributed.",
    )
    parser.add_argument(
        "--no-distributed",
        action="store_true",
        help="Do not start dask.distributed; useful for local smoke tests.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=default_workers,
        help="LocalCluster workers. Default is conservative and can be overridden with WORDCOUNT_WORKERS.",
    )
    parser.add_argument(
        "--threads-per-worker",
        type=int,
        default=default_threads,
        help="LocalCluster threads per worker. Default 1 avoids oversubscribing CloudVeneto VCPUs.",
    )
    parser.add_argument(
        "--memory-limit",
        default=default_memory,
        help="LocalCluster memory limit per worker. Defaults to Dask 'auto' or WORDCOUNT_WORKER_MEMORY.",
    )
    parser.add_argument(
        "--dashboard-address",
        default=":8787",
        help="LocalCluster dashboard address when supported by dask.distributed.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    input_path = Path(args.input)
    output_dir = Path(args.output_dir)
    global_counts_dir = output_dir / "global_word_counts"
    document_counts_dir = output_dir / "document_word_counts"
    top_csv = output_dir / "top_words.csv"
    plot_path = output_dir / "top_words.png"
    summary_path = output_dir / "summary.json"
    benchmark_path = output_dir / "benchmark.json"
    run_started = time.perf_counter()
    timings = {}

    if not input_path.exists():
        raise SystemExit(
            f"Input not found: {input_path}\n"
            "Run the conversion workflow first; expected output is data/silver/paragraphs."
        )
    check_safe_paths(input_path, output_dir)

    try:
        import dask.dataframe as dd
        import pandas as pd
        import pyarrow as pa
    except ModuleNotFoundError as exc:
        raise SystemExit(
            "Missing dependency for the word count. Install in the project env:\n"
            "  pip install \"dask[complete]\" pyarrow matplotlib"
        ) from exc

    columns = parquet_columns(str(input_path))
    required = {args.doc_col, args.text_col}
    if columns is not None:
        missing = sorted(required - columns)
        if missing:
            raise SystemExit(f"Input {input_path} is missing required columns: {missing}")

    read_cols = [args.doc_col, args.text_col]
    can_filter_reference = columns is None or args.reference_col in columns
    if args.exclude_reference_like and can_filter_reference:
        read_cols.append(args.reference_col)

    output_dir.mkdir(parents=True, exist_ok=True)
    stopwords = load_stopwords(args.stopwords, args.no_default_stopwords)
    t0 = time.perf_counter()
    client, cluster = start_dask_client(args)
    timings["dask_startup_seconds"] = round(time.perf_counter() - t0, 3)
    cluster_info = distributed_cluster_info(client)

    print(
        "Dask dashboard:",
        getattr(client, "dashboard_link", "not using dask.distributed"),
    )
    print("Input:", input_path)
    print("Output:", output_dir)
    print("Map phase: paragraph rows -> (cord_uid, word, count) per partition")

    try:
        t0 = time.perf_counter()
        paragraphs = dd.read_parquet(str(input_path), columns=read_cols, engine="pyarrow")
        if args.exclude_reference_like:
            if args.reference_col in paragraphs.columns:
                paragraphs = paragraphs[~paragraphs[args.reference_col].fillna(False)]
            else:
                print(
                    f"Warning: --exclude-reference-like requested, but column "
                    f"'{args.reference_col}' was not found. Keeping all paragraphs."
                )

        paragraphs = paragraphs[[args.doc_col, args.text_col]].dropna(
            subset=[args.doc_col, args.text_col]
        )
        timings["read_graph_setup_seconds"] = round(time.perf_counter() - t0, 3)

        t0 = time.perf_counter()
        meta = pd.DataFrame(
            {
                args.doc_col: pd.Series(dtype="object"),
                "word": pd.Series(dtype="object"),
                "count": pd.Series(dtype="int64"),
            }
        )
        partial_doc_counts = paragraphs.map_partitions(
            count_partition,
            args.doc_col,
            args.text_col,
            args.token_pattern,
            tuple(sorted(stopwords)),
            args.min_token_len,
            meta=meta,
        )
        timings["map_graph_setup_seconds"] = round(time.perf_counter() - t0, 3)

        t0 = time.perf_counter()
        print("Reduce phase 1: summing counts per (cord_uid, word)")
        doc_counts = grouped_sum(partial_doc_counts, [args.doc_col, "word"], args.split_out)

        if args.write_document_counts:
            print("Writing intermediate document counts:", document_counts_dir)
            doc_counts_to_write = doc_counts[[args.doc_col, "word", "count"]].astype(
                {args.doc_col: "string", "word": "string", "count": "int64"}
            )
            document_schema = pa.schema(
                [
                    (args.doc_col, pa.string()),
                    ("word", pa.string()),
                    ("count", pa.int64()),
                ]
            )
            doc_counts_to_write.to_parquet(
                str(document_counts_dir),
                engine="pyarrow",
                compression="zstd",
                write_index=False,
                overwrite=True,
                schema=document_schema,
            )
            doc_counts = dd.read_parquet(str(document_counts_dir), engine="pyarrow")
            timings["document_counts_write_seconds"] = round(time.perf_counter() - t0, 3)

        print("Reduce phase 2: summing counts per word")
        global_counts = grouped_sum(doc_counts, ["word"], args.split_out)
        print("Writing global counts:", global_counts_dir)
        global_counts_to_write = global_counts[["word", "count"]].astype(
            {"word": "string", "count": "int64"}
        )
        global_schema = pa.schema(
            [
                ("word", pa.string()),
                ("count", pa.int64()),
            ]
        )
        global_counts_to_write.to_parquet(
            str(global_counts_dir),
            engine="pyarrow",
            compression="zstd",
            write_index=False,
            overwrite=True,
            schema=global_schema,
        )
        timings["map_reduce_global_write_seconds"] = round(time.perf_counter() - t0, 3)

        t0 = time.perf_counter()
        global_counts = dd.read_parquet(str(global_counts_dir), engine="pyarrow")
        try:
            top_words = global_counts.nlargest(args.top_n, "count").compute()
        except Exception:
            top_words = global_counts.compute().nlargest(args.top_n, "count")

        top_words = top_words.sort_values("count", ascending=False).reset_index(drop=True)
        top_words.to_csv(top_csv, index=False)

        plot_warning = write_barplot(
            top_words,
            plot_path,
            f"Top {len(top_words)} words in CORD-19 body text",
        )
        if plot_warning:
            print("Warning:", plot_warning)

        total_tokens = int(global_counts["count"].sum().compute())
        vocabulary_size = int(global_counts.shape[0].compute())
        timings["top_words_and_summary_seconds"] = round(time.perf_counter() - t0, 3)
        timings["total_seconds"] = round(time.perf_counter() - run_started, 3)
        benchmark = {
            "input": str(input_path),
            "output_dir": str(output_dir),
            "cluster": cluster_info,
            "parameters": {
                "top_n": int(args.top_n),
                "split_out": int(args.split_out),
                "exclude_reference_like": bool(args.exclude_reference_like),
                "min_token_len": int(args.min_token_len),
                "token_pattern": args.token_pattern,
                "default_stopwords_enabled": not args.no_default_stopwords,
                "write_document_counts": bool(args.write_document_counts),
            },
            "results": {
                "total_tokens": total_tokens,
                "vocabulary_size": vocabulary_size,
            },
            "timings": timings,
        }
        summary = {
            "input": str(input_path),
            "output_dir": str(output_dir),
            "global_counts": str(global_counts_dir),
            "document_counts": str(document_counts_dir)
            if args.write_document_counts
            else None,
            "top_words_csv": str(top_csv),
            "top_words_plot": str(plot_path) if plot_path.exists() else None,
            "benchmark_json": str(benchmark_path),
            "top_n": int(args.top_n),
            "total_tokens": total_tokens,
            "vocabulary_size": vocabulary_size,
            "excluded_reference_like": bool(args.exclude_reference_like),
            "token_pattern": args.token_pattern,
            "min_token_len": int(args.min_token_len),
            "default_stopwords_enabled": not args.no_default_stopwords,
            "total_seconds": timings["total_seconds"],
        }
        summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
        benchmark_path.write_text(json.dumps(benchmark, indent=2), encoding="utf-8")

        print("\nTop words:")
        print(top_words.head(args.top_n).to_string(index=False))
        print("\nDone.")
        print("CSV:", top_csv)
        print("Global counts:", global_counts_dir)
        print("Summary:", summary_path)
        print("Benchmark:", benchmark_path)
        return 0
    finally:
        if client is not None:
            client.close()
        if cluster is not None:
            cluster.close()


if __name__ == "__main__":
    raise SystemExit(main())
