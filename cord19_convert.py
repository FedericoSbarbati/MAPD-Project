"""
CORD-19 JSON/CSV -> Parquet conversion logic (validated).

Design (see project notes):
  - Logical model = relational/normalized; physical = partitioned Parquet.
  - Two layers: BRONZE (faithful extraction, structural gate only) ->
    SILVER (semantic cleaning: dedup, type coercion, country canonicalization).
  - Extraction is file-driven: a Dask Bag over on-disk paths, with cord_uid(s)
    embedded per work-item (built from metadata path columns) so no big dict is
    broadcast to workers.

Three tables, all keyed on cord_uid:
  papers      (1/paper,       from metadata.csv)        -> tasks 3/4 (titles)
  paragraphs  (1/paragraph,   from pmc_json | pdf_json) -> task 1  (word-count)
  authors     (1/(paper,auth),from pdf_json only)       -> task 2  (countries/institutes)

All functions here are top-level and picklable so they run under Dask workers.
"""
import os
import re
import json
import unicodedata
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# ---------------------------------------------------------------- paths
ARCH = "/Users/federicosbarbati/Developer/MAPD-Project/archive"
META = os.path.join(ARCH, "metadata.csv")
PDF_DIR = os.path.join(ARCH, "document_parses", "pdf_json")
PMC_DIR = os.path.join(ARCH, "document_parses", "pmc_json")

# output root for the derived Parquet datasets (git-ignored)
DATA_ROOT = "/Users/federicosbarbati/Developer/MAPD-Project/data"


def out_path(*parts):
    return os.path.join(DATA_ROOT, *parts)


# ---------------------------------------------------------------- IO
def load_json(path):
    """One pretty-printed JSON object per file (NOT json-lines)."""
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


# ---------------------------------------------------------------- linkage
def build_linkage(meta_path=META):
    """Return {filename -> [cord_uid, ...]} for pdf and pmc, from the metadata
    path columns. cord_uids deduped per file (metadata has duplicate rows)."""
    df = pd.read_csv(meta_path, dtype=str,
                     usecols=["cord_uid", "pdf_json_files", "pmc_json_files"])
    pdf_map, pmc_map = {}, {}
    for cu, pdfs, pmcs in zip(df.cord_uid, df.pdf_json_files, df.pmc_json_files):
        if isinstance(pdfs, str):
            for p in pdfs.split(";"):
                p = p.strip()
                if p:
                    pdf_map.setdefault(os.path.basename(p), set()).add(cu)
        if isinstance(pmcs, str):
            for p in pmcs.split(";"):
                p = p.strip()
                if p:
                    pmc_map.setdefault(os.path.basename(p), set()).add(cu)
    pdf_map = {k: sorted(v) for k, v in pdf_map.items()}
    pmc_map = {k: sorted(v) for k, v in pmc_map.items()}
    return pdf_map, pmc_map


def build_workitems(pdf_map, pmc_map, pdf_dir=PDF_DIR, pmc_dir=PMC_DIR):
    """Self-contained work-items [(filename, [cord_uid,...], source), ...].
    Only the filename is stored (path rebuilt in the worker via _resolve) to keep
    the Dask task graph small and portable across cluster nodes."""
    pdf_items, pmc_items, unresolved = [], [], 0
    for fn in os.listdir(pdf_dir):
        cus = pdf_map.get(fn)
        if cus is None:
            unresolved += 1
            continue
        pdf_items.append((fn, cus, "pdf"))
    for fn in os.listdir(pmc_dir):
        cus = pmc_map.get(fn)
        if cus is None:
            unresolved += 1
            continue
        pmc_items.append((fn, cus, "pmc"))
    return pdf_items, pmc_items, unresolved


def _resolve(filename, source):
    return os.path.join(PDF_DIR if source == "pdf" else PMC_DIR, filename)


# ---------------------------------------------------------------- extraction
def _clean(s):
    if not isinstance(s, str):
        return None
    s = s.strip()
    return s if s else None


def extract_paragraphs(item):
    """item=(filename,[cord_uid...],source) -> list of paragraph rows.
    Structural gate: unparseable file -> []. Empty-text paragraphs dropped."""
    filename, cord_uids, source = item
    try:
        rec = load_json(_resolve(filename, source))
    except Exception:
        return []
    pid = rec.get("paper_id")
    rows = []
    for i, para in enumerate(rec.get("body_text", []) or []):
        text = para.get("text")
        if not isinstance(text, str) or not text.strip():
            continue
        section = _clean(para.get("section"))
        for cu in cord_uids:
            rows.append({"cord_uid": cu, "paper_id": pid, "source": source,
                         "para_idx": i, "section": section, "text": text})
    return rows


def extract_authors(item):
    """item=(filename,[cord_uid...],source) -> list of author rows. pdf_json only
    (pmc affiliations are 0% populated). Keeps authors even w/o affiliation."""
    filename, cord_uids, source = item
    try:
        rec = load_json(_resolve(filename, source))
    except Exception:
        return []
    pid = rec.get("paper_id")
    rows = []
    for idx, a in enumerate(rec.get("metadata", {}).get("authors", []) or []):
        aff = a.get("affiliation") or {}
        loc = aff.get("location") or {}
        for cu in cord_uids:
            rows.append({"cord_uid": cu, "paper_id": pid, "author_idx": idx,
                         "institution": _clean(aff.get("institution")),
                         "country_raw": _clean(loc.get("country")),
                         "settlement": _clean(loc.get("settlement"))})
    return rows


# ---------------------------------------------------------------- schemas / meta
PARA_META = {"cord_uid": "object", "paper_id": "object", "source": "object",
             "para_idx": "int64", "section": "object", "text": "object"}
AUTH_META = {"cord_uid": "object", "paper_id": "object", "author_idx": "int64",
             "institution": "object", "country_raw": "object", "settlement": "object"}

# Explicit Arrow schemas -> passed to to_parquet so Dask pins types and does not
# infer the schema from the bag meta.
PARA_SCHEMA = pa.schema([("cord_uid", pa.string()), ("paper_id", pa.string()),
                         ("source", pa.string()), ("para_idx", pa.int32()),
                         ("section", pa.string()), ("text", pa.string())])
AUTH_SCHEMA = pa.schema([("cord_uid", pa.string()), ("paper_id", pa.string()),
                         ("author_idx", pa.int32()), ("institution", pa.string()),
                         ("country_raw", pa.string()), ("settlement", pa.string())])


# ---------------------------------------------------------------- papers (silver)
DEAD_COLS = ["mag_id", "arxiv_id", "who_covidence_id"]


def silver_papers(df):
    """Dedup on cord_uid (prefer rows with a parse) + year + flags + drop dead cols."""
    df = df.copy()
    df["year"] = pd.to_numeric(df["publish_time"].str.slice(0, 4),
                               errors="coerce").astype("Int16")
    df["has_pdf"] = df["pdf_json_files"].notna()
    df["has_pmc"] = df["pmc_json_files"].notna()
    df = df.drop(columns=[c for c in DEAD_COLS if c in df.columns])
    df["_rank"] = df["has_pdf"].astype(int) + df["has_pmc"].astype(int)
    df = (df.sort_values("_rank", ascending=False)
            .drop_duplicates("cord_uid", keep="first")
            .drop(columns=["_rank"]))
    return df


# ---------------------------------------------------------------- paragraphs (silver)
def silver_paragraphs(paragraphs_ddf):
    """Prefer pmc over pdf per paper: keep all pmc rows, plus pdf rows whose
    cord_uid has no pmc parse. Returns a filtered dask DataFrame."""
    pmc_uids = (paragraphs_ddf[paragraphs_ddf["source"] == "pmc"]
                ["cord_uid"].unique().compute())
    pmc_set = set(pmc_uids)
    keep = ((paragraphs_ddf["source"] == "pmc")
            | (~paragraphs_ddf["cord_uid"].isin(pmc_set)))
    return paragraphs_ddf[keep]


# section labels are free-text and un-canonicalizable (~1M distinct); we only
# flag paragraphs whose section looks like a non-body / boilerplate section.
REFERENCE_SECTION_RE = (r"(referen|bibliograph|acknowledg|author contrib|"
                        r"conflict|competing interest|funding|declarat|"
                        r"supplement|copyright)")

PARA_SILVER_SCHEMA = pa.schema([("cord_uid", pa.string()), ("paper_id", pa.string()),
                                ("source", pa.string()), ("para_idx", pa.int32()),
                                ("section", pa.string()), ("text", pa.string()),
                                ("is_reference_like", pa.bool_())])


# ---------------------------------------------------------------- write utils
def fresh_dir(path):
    import shutil
    if os.path.exists(path):
        shutil.rmtree(path)
    os.makedirs(path, exist_ok=True)


def _downcast_large_string(table):
    """large_string -> string, for uniform types across all datasets."""
    fields = [pa.field(f.name, pa.string() if f.type == pa.large_string() else f.type)
              for f in table.schema]
    return table.cast(pa.schema(fields))


def write_pandas_parquet(df, path, nparts):
    """Write a (small) pandas frame as a directory of nparts Parquet files,
    normalizing large_string -> string so schemas are uniform."""
    fresh_dir(path)
    nparts = min(nparts, max(len(df), 1))
    for i, ix in enumerate(np.array_split(np.arange(len(df)), nparts)):
        t = _downcast_large_string(pa.Table.from_pandas(df.iloc[ix], preserve_index=False))
        pq.write_table(t, os.path.join(path, f"part.{i}.parquet"), compression="zstd")


# ---------------------------------------------------------------- country canon
# ISO3 patch for values country_converter's regex misses (foreign-language
# names, abbreviations). Keys are accent-folded, lowercased, letters-only.
_COUNTRY_ALIASES = {
    "deutschland": "DEU", "espana": "ESP", "brasil": "BRA", "mexico": "MEX",
    "osterreich": "AUT", "uae": "ARE", "uk": "GBR", "scotland": "GBR",
    "england": "GBR", "wales": "GBR", "northern ireland": "GBR",
    "the netherlands": "NLD", "usa": "USA",
}


def _akey(s):
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    return re.sub(r"[^a-z ]", " ", s.lower()).strip()


def _first_valid(v, iso3_set):
    """coco returns a str (1 match), a list (multi -> take first valid), or a
    passthrough of the input (no match -> not in iso3_set -> None)."""
    if isinstance(v, list):
        return next((x for x in v if x in iso3_set), None)
    return v if v in iso3_set else None


def canonicalize_country(country_raw):
    """pandas Series country_raw -> (iso3 Series, name Series). 3 passes on the
    distinct values: coco regex -> alias patch -> first token retry."""
    import logging
    import country_converter as coco
    logging.getLogger("country_converter").setLevel(logging.CRITICAL)
    cc = coco.CountryConverter()
    iso3_set = set(cc.data.ISO3.dropna())
    iso3_name = dict(zip(cc.data.ISO3, cc.data.name_short))

    distinct = list(pd.Index(country_raw.dropna().unique()))
    conv = cc.convert(distinct, to="ISO3", not_found=None)  # vectorized pass 1
    mapping, todo = {}, []
    for raw, v in zip(distinct, conv):
        iso = _first_valid(v, iso3_set)
        (mapping.__setitem__(raw, iso) if iso else todo.append(raw))
    for raw in todo:
        iso = _COUNTRY_ALIASES.get(_akey(raw))
        if iso is None:  # pass 3: first comma/semicolon token, letters only
            head = re.sub(r"[^A-Za-z ]", " ", re.split(r"[,;/]", raw)[0]).strip()
            if head:
                iso = _first_valid(cc.convert(head, to="ISO3", not_found=None), iso3_set)
                if iso is None:
                    iso = _COUNTRY_ALIASES.get(_akey(head))
        mapping[raw] = iso
    iso3 = country_raw.map(mapping)
    return iso3, iso3.map(iso3_name)


def norm_institution(s):
    """Light normalization only (full institute disambiguation is out of scope):
    unicode-normalize, collapse whitespace, strip surrounding punctuation."""
    if not isinstance(s, str):
        return None
    s = re.sub(r"\s+", " ", unicodedata.normalize("NFKC", s)).strip(" ,;.-\t")
    return s or None


# ---------------------------------------------------------------- papers enrich
def enrich_papers(df):
    """Add title flags for tasks 3/4 (does NOT drop anything). title_norm for
    matching; dup flags (exact-duplicate titles -> trivial cosine); title_ok."""
    df = df.copy()
    norm = (df["title"].fillna("").str.strip().str.lower()
            .str.replace(r"\s+", " ", regex=True))
    df["title_norm"] = norm.where(norm != "", other=None)
    vc = df["title_norm"].value_counts()
    df["title_dup_count"] = df["title_norm"].map(vc).astype("Int32")
    df["is_title_unique"] = df["title_dup_count"].eq(1)
    df["title_ok"] = df["title"].notna() & (df["title"].str.strip().str.len() >= 3)
    if "abstract" in df.columns:
        df["abstract"] = df["abstract"].str.replace(r"\s+", " ", regex=True).str.strip()
    return df
