#!/usr/bin/env python3
"""
Task 2.3.2 - Best and worst represented countries/institutions in CORD-19.

The script reads the silver affiliation rollups:

  data/silver/paper_countries
  data/silver/paper_institutions

These tables are already deduplicated at paper level, so a country/institution is
counted at most once per paper. This avoids inflating countries or institutes
that simply have many co-authors on the same publication.

Outputs are written under Giulia/reports/task_2_3_2_affiliations by default.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from collections import Counter
from pathlib import Path
from typing import Iterable


DEFAULT_TOP_N = 25
DEFAULT_BOTTOM_N = 25
DEFAULT_BATCH_SIZE = 65_536


def find_project_root(start: Path) -> Path:
    for candidate in [start.resolve(), *start.resolve().parents]:
        if (candidate / "DATA_DICTIONARY.md").exists():
            return candidate
    raise SystemExit(
        "Cannot find project root. Run this script from the MAPD-Project repo "
        "or pass --project-root explicitly."
    )


def parse_args(argv: list[str]) -> argparse.Namespace:
    script_root = find_project_root(Path(__file__).resolve())
    default_data_root = Path(os.environ.get("CORD19_DATA", script_root / "data"))
    default_output = script_root / "Giulia" / "reports" / "task_2_3_2_affiliations"

    parser = argparse.ArgumentParser(
        description=(
            "Compute best/worst represented countries and institutions from "
            "CORD-19 author affiliations."
        )
    )
    parser.add_argument(
        "--project-root",
        default=str(script_root),
        help=f"Project root. Default: {script_root}",
    )
    parser.add_argument(
        "--data-root",
        default=str(default_data_root),
        help="Data root containing silver/. Default: $CORD19_DATA or <project>/data.",
    )
    parser.add_argument(
        "--output-dir",
        default=str(default_output),
        help="Output directory for CSV/JSON results.",
    )
    parser.add_argument(
        "--top-n",
        type=int,
        default=DEFAULT_TOP_N,
        help=f"Rows to keep in top output files. Default: {DEFAULT_TOP_N}.",
    )
    parser.add_argument(
        "--bottom-n",
        type=int,
        default=DEFAULT_BOTTOM_N,
        help=f"Rows to keep in bottom output files. Default: {DEFAULT_BOTTOM_N}.",
    )
    parser.add_argument(
        "--bottom-min-count",
        type=int,
        default=1,
        help=(
            "Minimum paper count for worst-represented outputs. Keep 1 for the "
            "literal least represented entities; use e.g. 2 or 5 to ignore one-offs."
        ),
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help=f"PyArrow scan batch size. Default: {DEFAULT_BATCH_SIZE}.",
    )
    parser.add_argument(
        "--include-author-counts",
        action="store_true",
        help=(
            "Also compute author-row counts from silver/authors. This is optional "
            "and can inflate papers with many co-authors, so per-paper counts remain primary."
        ),
    )
    return parser.parse_args(argv)


def normalize_value(value: object, min_len: int = 1) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text or text.lower() in {"none", "nan", "null", "<na>"}:
        return None
    if len(text) < min_len:
        return None
    return text


def dataset_batches(path: Path, columns: list[str], batch_size: int):
    try:
        import pyarrow.dataset as ds
    except ModuleNotFoundError as exc:
        raise SystemExit(
            "Missing dependency 'pyarrow'. Install Giulia/requirements.txt first."
        ) from exc

    if not path.exists():
        raise SystemExit(
            f"Required dataset not found: {path}\n"
            "Expected cleaned silver data. On CloudVeneto this should be under "
            "/data/MAPD-Project/data/silver/."
        )

    dataset = ds.dataset(path, format="parquet")
    missing = sorted(set(columns) - set(dataset.schema.names))
    if missing:
        raise SystemExit(f"Dataset {path} is missing required columns: {missing}")

    return dataset.to_batches(columns=columns, batch_size=batch_size)


def count_country_papers(path: Path, batch_size: int) -> tuple[Counter[tuple[str, str]], int]:
    counts: Counter[tuple[str, str]] = Counter()
    rows_seen = 0

    for batch in dataset_batches(path, ["country_iso3", "country"], batch_size):
        iso_values = batch.column("country_iso3").to_pylist()
        country_values = batch.column("country").to_pylist()
        rows_seen += batch.num_rows
        for iso_raw, country_raw in zip(iso_values, country_values):
            country = normalize_value(country_raw, min_len=2)
            if country is None:
                continue
            iso = normalize_value(iso_raw, min_len=2) or ""
            counts[(iso, country)] += 1

    return counts, rows_seen


def count_single_column(path: Path, column: str, batch_size: int, min_len: int) -> tuple[Counter[str], int]:
    counts: Counter[str] = Counter()
    rows_seen = 0

    for batch in dataset_batches(path, [column], batch_size):
        values = batch.column(column).to_pylist()
        rows_seen += batch.num_rows
        for raw_value in values:
            value = normalize_value(raw_value, min_len=min_len)
            if value is not None:
                counts[value] += 1

    return counts, rows_seen


def country_frame(counts: Counter[tuple[str, str]]):
    import pandas as pd

    rows = [
        {"country_iso3": iso, "country": country, "paper_count": int(count)}
        for (iso, country), count in counts.items()
    ]
    frame = pd.DataFrame(rows, columns=["country_iso3", "country", "paper_count"])
    if frame.empty:
        return frame
    return frame.sort_values(
        ["paper_count", "country"], ascending=[False, True]
    ).reset_index(drop=True)


def count_frame(counts: Counter[str], name_col: str, count_col: str):
    import pandas as pd

    rows = [{name_col: name, count_col: int(count)} for name, count in counts.items()]
    frame = pd.DataFrame(rows, columns=[name_col, count_col])
    if frame.empty:
        return frame
    return frame.sort_values([count_col, name_col], ascending=[False, True]).reset_index(drop=True)


def top_bottom(frame, count_col: str, top_n: int, bottom_n: int, bottom_min_count: int):
    top = frame.head(top_n).copy()
    bottom_source = frame[frame[count_col] >= bottom_min_count]
    bottom = bottom_source.sort_values(
        [count_col, frame.columns[0]], ascending=[True, True]
    ).head(bottom_n)
    return top, bottom.reset_index(drop=True)


def write_csv(frame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(path, index=False)


def frame_to_markdown(frame) -> str:
    if frame.empty:
        return "_No rows._"

    columns = list(frame.columns)
    rows = [[str(value) for value in row] for row in frame.itertuples(index=False, name=None)]
    widths = [
        max(len(str(column)), *(len(row[idx]) for row in rows))
        for idx, column in enumerate(columns)
    ]

    def fmt(values: Iterable[object]) -> str:
        return "| " + " | ".join(
            str(value).ljust(widths[idx]) for idx, value in enumerate(values)
        ) + " |"

    header = fmt(columns)
    sep = "| " + " | ".join("-" * width for width in widths) + " |"
    body = [fmt(row) for row in rows]
    return "\n".join([header, sep, *body])


def write_markdown_summary(
    path: Path,
    country_top,
    country_bottom,
    institution_top,
    institution_bottom,
    top_n: int,
    bottom_n: int,
    bottom_min_count: int,
) -> None:
    lines = [
        "# Task 2.3.2 - Countries and institutions",
        "",
        "Primary metric: number of distinct papers represented by each country/institution.",
        "The inputs are the silver per-paper rollups, so repeated authors from the same entity in one paper are not double-counted.",
        "",
        f"Top rows: {top_n}. Bottom rows: {bottom_n}. Bottom minimum count: {bottom_min_count}.",
        "",
        "## Best represented countries",
        "",
        frame_to_markdown(country_top),
        "",
        "## Worst represented countries",
        "",
        frame_to_markdown(country_bottom),
        "",
        "## Best represented institutions",
        "",
        frame_to_markdown(institution_top),
        "",
        "## Worst represented institutions",
        "",
        frame_to_markdown(institution_bottom),
        "",
    ]
    path.write_text("\n".join(lines), encoding="utf-8")


def maybe_compute_author_counts(data_root: Path, output_dir: Path, batch_size: int) -> dict[str, object]:
    authors_path = data_root / "silver" / "authors"
    if not authors_path.exists():
        return {"enabled": True, "status": "missing", "path": str(authors_path)}

    country_counts, author_country_rows = count_single_column(
        authors_path, "country", batch_size, min_len=2
    )
    institution_counts, author_institution_rows = count_single_column(
        authors_path, "institution_norm", batch_size, min_len=3
    )

    author_country = count_frame(country_counts, "country", "author_row_count")
    author_institution = count_frame(
        institution_counts, "institution_norm", "author_row_count"
    )

    write_csv(author_country, output_dir / "author_country_counts.csv")
    write_csv(author_institution, output_dir / "author_institution_counts.csv")

    return {
        "enabled": True,
        "status": "ok",
        "authors_dataset": str(authors_path),
        "author_country_rows_seen": author_country_rows,
        "author_institution_rows_seen": author_institution_rows,
        "countries_with_author_affiliation": int(len(author_country)),
        "institutions_with_author_affiliation": int(len(author_institution)),
    }


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    started = time.perf_counter()

    project_root = Path(args.project_root).expanduser().resolve()
    data_root = Path(args.data_root).expanduser().resolve()
    output_dir = Path(args.output_dir).expanduser().resolve()

    if args.top_n < 1 or args.bottom_n < 1:
        raise SystemExit("--top-n and --bottom-n must be positive integers.")
    if args.bottom_min_count < 1:
        raise SystemExit("--bottom-min-count must be >= 1.")

    countries_path = data_root / "silver" / "paper_countries"
    institutions_path = data_root / "silver" / "paper_institutions"

    print("Task 2.3.2 affiliation representation")
    print("project root :", project_root)
    print("data root    :", data_root)
    print("output dir   :", output_dir)
    print("countries    :", countries_path)
    print("institutions :", institutions_path)

    country_counts, country_rows_seen = count_country_papers(countries_path, args.batch_size)
    institution_counts, institution_rows_seen = count_single_column(
        institutions_path, "institution_norm", args.batch_size, min_len=3
    )

    countries = country_frame(country_counts)
    institutions = count_frame(institution_counts, "institution_norm", "paper_count")

    country_top, country_bottom = top_bottom(
        countries, "paper_count", args.top_n, args.bottom_n, args.bottom_min_count
    )
    institution_top, institution_bottom = top_bottom(
        institutions, "paper_count", args.top_n, args.bottom_n, args.bottom_min_count
    )

    output_dir.mkdir(parents=True, exist_ok=True)
    write_csv(countries, output_dir / "country_paper_counts.csv")
    write_csv(institutions, output_dir / "institution_paper_counts.csv")
    write_csv(country_top, output_dir / "best_represented_countries.csv")
    write_csv(country_bottom, output_dir / "worst_represented_countries.csv")
    write_csv(institution_top, output_dir / "best_represented_institutions.csv")
    write_csv(institution_bottom, output_dir / "worst_represented_institutions.csv")

    author_summary = {"enabled": False}
    if args.include_author_counts:
        author_summary = maybe_compute_author_counts(data_root, output_dir, args.batch_size)

    elapsed = round(time.perf_counter() - started, 3)
    summary = {
        "task": "2.3.2",
        "metric": "distinct paper count per affiliation entity",
        "project_root": str(project_root),
        "data_root": str(data_root),
        "output_dir": str(output_dir),
        "inputs": {
            "paper_countries": str(countries_path),
            "paper_institutions": str(institutions_path),
        },
        "parameters": {
            "top_n": args.top_n,
            "bottom_n": args.bottom_n,
            "bottom_min_count": args.bottom_min_count,
            "batch_size": args.batch_size,
        },
        "rows_seen": {
            "paper_countries": country_rows_seen,
            "paper_institutions": institution_rows_seen,
        },
        "results": {
            "countries": int(len(countries)),
            "institutions": int(len(institutions)),
            "best_country": country_top.iloc[0].to_dict() if len(country_top) else None,
            "worst_country": country_bottom.iloc[0].to_dict() if len(country_bottom) else None,
            "best_institution": institution_top.iloc[0].to_dict()
            if len(institution_top)
            else None,
            "worst_institution": institution_bottom.iloc[0].to_dict()
            if len(institution_bottom)
            else None,
        },
        "author_counts": author_summary,
        "elapsed_seconds": elapsed,
    }
    (output_dir / "summary.json").write_text(
        json.dumps(summary, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    write_markdown_summary(
        output_dir / "summary.md",
        country_top,
        country_bottom,
        institution_top,
        institution_bottom,
        args.top_n,
        args.bottom_n,
        args.bottom_min_count,
    )

    print("\nBest represented countries:")
    print(country_top.to_string(index=False))
    print("\nWorst represented countries:")
    print(country_bottom.to_string(index=False))
    print("\nBest represented institutions:")
    print(institution_top.to_string(index=False))
    print("\nWorst represented institutions:")
    print(institution_bottom.to_string(index=False))
    print("\nDone.")
    print("Summary:", output_dir / "summary.json")
    print("Markdown:", output_dir / "summary.md")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
