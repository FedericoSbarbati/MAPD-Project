"""
Driver: build the CORD-19 Parquet datasets (bronze + silver) with Dask.

Usage (mapd-covid env):
  python build_parquet.py --sample 4000     # dry run on a random subset -> data_sample/
  python build_parquet.py --run             # full conversion            -> data/

Produces:
  <root>/bronze/papers      <root>/silver/papers
  <root>/bronze/paragraphs  <root>/silver/paragraphs
  <root>/bronze/authors     (silver/authors deferred: country canonicalization = task 2)
"""
import os
import sys
import time
import shutil
import random
import argparse

import numpy as np
import pandas as pd
import dask.bag as db
import dask.dataframe as dd
from dask.distributed import Client, LocalCluster

import cord19_convert as C

# partitions tuned for ~100-150 MB/part on the full run; scaled down for samples
NPART_PARA = 48
NPART_AUTH = 12
NPART_PAPERS = 9


def _fresh(path):
    if os.path.exists(path):
        shutil.rmtree(path)
    os.makedirs(path, exist_ok=True)


def _dirsize(path):
    total = 0
    for r, _, files in os.walk(path):
        for f in files:
            total += os.path.getsize(os.path.join(r, f))
    return total


def _report(name, path, t0):
    print(f"  [OK] {name:22s} {_dirsize(path)/1e6:8.1f} MB   {time.time()-t0:6.1f}s   {path}")


def _write_pandas_parquet(df, path, nparts):
    """Write a (small) pandas frame as a directory of nparts Parquet files.
    Pure pandas -> avoids embedding the whole frame in a Dask task graph."""
    _fresh(path)
    nparts = min(nparts, max(len(df), 1))
    for i, ix in enumerate(np.array_split(np.arange(len(df)), nparts)):
        df.iloc[ix].to_parquet(os.path.join(path, f"part.{i}.parquet"),
                               engine="pyarrow", compression="zstd", index=False)


# ----------------------------------------------------------------
def build_papers(root, meta_nrows=None):
    # papers is small (<=425k rows): handle in pure pandas, no Dask graph.
    t0 = time.time()
    df = pd.read_csv(C.META, dtype=str, nrows=meta_nrows)
    bpath = os.path.join(root, "bronze", "papers")
    _write_pandas_parquet(df.astype("string"), bpath, NPART_PAPERS)
    _report("bronze/papers", bpath, t0)

    t0 = time.time()
    s = C.silver_papers(df)
    s = s.astype({c: "string" for c in s.columns if s[c].dtype == object})
    spath = os.path.join(root, "silver", "papers")
    _write_pandas_parquet(s, spath, NPART_PAPERS)
    _report("silver/papers", spath, t0)


def build_paragraphs(root, pdf_items, pmc_items, npart):
    t0 = time.time()
    items = pdf_items + pmc_items
    bpath = os.path.join(root, "bronze", "paragraphs")
    _fresh(bpath)
    bag = db.from_sequence(items, npartitions=npart).map(C.extract_paragraphs).flatten()
    ddf = bag.to_dataframe(meta=C.PARA_META)
    ddf.to_parquet(bpath, engine="pyarrow", write_index=False,
                   compression="zstd", schema=C.PARA_SCHEMA)
    _report("bronze/paragraphs", bpath, t0)
    # silver: prefer pmc over pdf per paper
    t0 = time.time()
    src = dd.read_parquet(bpath, engine="pyarrow")
    silver = C.silver_paragraphs(src)
    spath = os.path.join(root, "silver", "paragraphs")
    _fresh(spath)
    silver.to_parquet(spath, engine="pyarrow", write_index=False,
                      compression="zstd", schema=C.PARA_SCHEMA)
    _report("silver/paragraphs", spath, t0)


def build_authors(root, pdf_items, npart):
    t0 = time.time()
    bpath = os.path.join(root, "bronze", "authors")
    _fresh(bpath)
    bag = db.from_sequence(pdf_items, npartitions=npart).map(C.extract_authors).flatten()
    ddf = bag.to_dataframe(meta=C.AUTH_META)
    ddf.to_parquet(bpath, engine="pyarrow", write_index=False,
                   compression="zstd", schema=C.AUTH_SCHEMA)
    _report("bronze/authors", bpath, t0)
    # silver/authors intentionally deferred (country canonicalization -> task 2)


def main():
    ap = argparse.ArgumentParser()
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--run", action="store_true", help="full conversion -> data/")
    g.add_argument("--sample", type=int, metavar="N",
                   help="dry run on N random files/source -> data_sample/")
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--dashboard-port", type=int, default=8787)
    args = ap.parse_args()

    root = C.DATA_ROOT if args.run else C.DATA_ROOT + "_sample"

    print("building linkage + work-items ...")
    pdf_map, pmc_map = C.build_linkage()
    pdf_items, pmc_items, unresolved = C.build_workitems(pdf_map, pmc_map)
    assert unresolved == 0, f"{unresolved} unresolved files"
    print(f"  pdf={len(pdf_items)} pmc={len(pmc_items)} unresolved=0")

    meta_nrows = None
    npart_para, npart_auth = NPART_PARA, NPART_AUTH
    if args.sample:
        random.seed(0)
        pdf_items = random.sample(pdf_items, min(args.sample, len(pdf_items)))
        pmc_items = random.sample(pmc_items, min(args.sample, len(pmc_items)))
        meta_nrows = args.sample * 3
        npart_para, npart_auth = 8, 4

    cluster = LocalCluster(n_workers=args.workers, threads_per_worker=2,
                           dashboard_address=f":{args.dashboard_port}", processes=True)
    client = Client(cluster)
    print("dask dashboard:", client.dashboard_link)
    T0 = time.time()
    try:
        build_papers(root, meta_nrows=meta_nrows)
        build_paragraphs(root, pdf_items, pmc_items, npart_para)
        build_authors(root, pdf_items, npart_auth)
    finally:
        client.close()
        cluster.close()
    print(f"\nDONE in {time.time()-T0:.1f}s -> {root}")


if __name__ == "__main__":
    main()
