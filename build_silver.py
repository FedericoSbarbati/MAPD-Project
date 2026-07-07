"""
Build the ENRICHED silver layer from the existing bronze Parquet datasets.
Reads bronze/* (no JSON re-parse) and writes the analysis-ready silver/*:

  silver/papers            + title_norm, title_dup_count, is_title_unique, title_ok
  silver/authors           + country_iso3, country, institution_norm  (keeps raw)
  silver/paragraphs        + is_reference_like  (prefer-pmc already applied)
  silver/paper_countries   distinct (cord_uid, country_iso3, country)   [rollup]
  silver/paper_institutions distinct (cord_uid, institution_norm)       [rollup]

Cleaning principle: fix objective errors + add flags; never make a task's
analysis decision (no dedup of titles, no stopwords, no reference removal).

Usage:  python build_silver.py
"""
import os
import time
import argparse
import pandas as pd
import dask.dataframe as dd

import cord19_convert as C

NPART_PAPERS = 9
NPART_AUTH = 12
NPART_PARA = 48


def dirsize(path):
    return sum(os.path.getsize(os.path.join(path, f))
               for f in os.listdir(path) if f.endswith(".parquet")) / 1e6


def report(name, path, t0):
    print(f"  [OK] {name:24s} {dirsize(path):8.1f} MB  {time.time()-t0:6.1f}s")


def build_papers(root):
    t0 = time.time()
    df = pd.read_parquet(os.path.join(root, "bronze", "papers"))
    s = C.silver_papers(df)          # dedup + year + flags + drop dead cols
    s = C.enrich_papers(s)           # title_norm / dup flags / title_ok / clean abstract
    out = os.path.join(root, "silver", "papers")
    C.write_pandas_parquet(s, out, NPART_PAPERS)
    report("silver/papers", out, t0)
    return s


def build_authors(root):
    t0 = time.time()
    a = pd.read_parquet(os.path.join(root, "bronze", "authors"))
    iso3, name = C.canonicalize_country(a["country_raw"])
    a["country_iso3"] = iso3
    a["country"] = name
    a["institution_norm"] = a["institution"].map(C.norm_institution)
    a = a[["cord_uid", "paper_id", "author_idx", "institution", "institution_norm",
           "country_raw", "country_iso3", "country", "settlement"]]
    out = os.path.join(root, "silver", "authors")
    C.write_pandas_parquet(a, out, NPART_AUTH)
    resolved = a["country_iso3"].notna().sum()
    print(f"       country resolved: {resolved}/{a['country_raw'].notna().sum()} "
          f"({100*resolved/max(a['country_raw'].notna().sum(),1):.2f}% of non-null raw)")
    report("silver/authors", out, t0)

    # rollups: one row per (paper, country) / (paper, institution)
    t0 = time.time()
    pc = (a.loc[a.country_iso3.notna(), ["cord_uid", "country_iso3", "country"]]
          .drop_duplicates().reset_index(drop=True))
    out_pc = os.path.join(root, "silver", "paper_countries")
    C.write_pandas_parquet(pc, out_pc, NPART_PAPERS)
    report("silver/paper_countries", out_pc, t0)

    t0 = time.time()
    pi = (a.loc[a.institution_norm.notna(), ["cord_uid", "institution_norm"]]
          .drop_duplicates().reset_index(drop=True))
    out_pi = os.path.join(root, "silver", "paper_institutions")
    C.write_pandas_parquet(pi, out_pi, NPART_PAPERS)
    report("silver/paper_institutions", out_pi, t0)


def build_paragraphs(root):
    t0 = time.time()
    ddf = dd.read_parquet(os.path.join(root, "bronze", "paragraphs"), engine="pyarrow")
    ddf = C.silver_paragraphs(ddf)   # prefer pmc over pdf
    ddf = ddf.assign(is_reference_like=ddf["section"].fillna("").str.lower()
                     .str.contains(C.REFERENCE_SECTION_RE, regex=True))
    out = os.path.join(root, "silver", "paragraphs")
    C.fresh_dir(out)
    ddf.to_parquet(out, engine="pyarrow", write_index=False,
                   compression="zstd", schema=C.PARA_SILVER_SCHEMA)
    report("silver/paragraphs", out, t0)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default=C.DATA_ROOT)
    ap.add_argument("--skip-paragraphs", action="store_true",
                    help="rebuild only the pandas-written tables (fast)")
    args = ap.parse_args()
    root = args.root
    T0 = time.time()
    build_papers(root)
    build_authors(root)
    if not args.skip_paragraphs:
        build_paragraphs(root)
    print(f"\nSILVER DONE in {time.time()-T0:.1f}s -> {os.path.join(root, 'silver')}")
