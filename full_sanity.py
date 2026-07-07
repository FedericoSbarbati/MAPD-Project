"""Full-corpus sanity check on the produced Parquet datasets (data/)."""
import os
import pandas as pd
import dask.dataframe as dd

ROOT = "/Users/federicosbarbati/Developer/MAPD-Project/data"


def d(name):
    return os.path.join(ROOT, name)


def dirstat(path):
    parts = [f for f in os.listdir(path) if f.endswith(".parquet")]
    size = sum(os.path.getsize(os.path.join(path, f)) for f in parts)
    return len(parts), size / 1e6


def banner(m):
    print("\n" + "=" * 68 + f"\n{m}\n" + "=" * 68)


# ------------------------------------------------------------ sizes
banner("DATASETS (part-files, size)")
for name in ["bronze/papers", "silver/papers", "bronze/paragraphs",
             "silver/paragraphs", "bronze/authors"]:
    n, mb = dirstat(d(name))
    print(f"  {name:20s} {n:3d} parts  {mb:9.1f} MB")

# ------------------------------------------------------------ papers
banner("PAPERS (silver)")
ps = pd.read_parquet(d("silver/papers"),
                     columns=["cord_uid", "title", "year", "has_pdf", "has_pmc"])
print("rows:", len(ps), "| unique cord_uid:", ps.cord_uid.is_unique)
assert len(ps) == 406211, "expected 406211 unique papers"
assert ps.cord_uid.is_unique
print("year peak:", int(ps.year.value_counts().idxmax()), "->", int(ps.year.value_counts().max()))
print("has_pdf:", int(ps.has_pdf.sum()), "| has_pmc:", int(ps.has_pmc.sum()),
      "| title null:", int(ps.title.isna().sum()))
papers_set = set(ps.cord_uid)

# ------------------------------------------------------------ authors
banner("AUTHORS (bronze, task 2)")
au = pd.read_parquet(d("bronze/authors"),
                     columns=["cord_uid", "institution", "country_raw"])
print("rows:", len(au), "| distinct papers:", au.cord_uid.nunique())
print("institution non-null: {} ({:.1f}%) | country non-null: {} ({:.1f}%)".format(
    int(au.institution.notna().sum()), 100 * au.institution.notna().mean(),
    int(au.country_raw.notna().sum()), 100 * au.country_raw.notna().mean()))
print("top 8 country_raw (dirty):")
print(au.country_raw.value_counts().head(8).to_string())
auth_set = set(au.cord_uid.unique())
assert auth_set <= papers_set, "authors reference cord_uid not in papers!"
print("referential integrity authors.cord_uid ⊆ papers.cord_uid: OK")

# ------------------------------------------------------------ paragraphs (dask)
banner("PARAGRAPHS (bronze vs silver, task 1)")
pb = dd.read_parquet(d("bronze/paragraphs"), columns=["cord_uid", "source"])
sp = dd.read_parquet(d("silver/paragraphs"), columns=["cord_uid", "source"])

pb_n = int(pb.shape[0].compute())
pb_src = pb.source.value_counts().compute().to_dict()
pb_uids = int(pb.cord_uid.nunique().compute())
print(f"bronze: rows={pb_n}  by source={pb_src}  distinct papers={pb_uids}")

sp_n = int(sp.shape[0].compute())
sp_src = sp.source.value_counts().compute().to_dict()
print(f"silver: rows={sp_n}  by source={sp_src}")
assert set(sp_src) <= {"pdf", "pmc"}, "unexpected source value"

pmc_uids = set(sp[sp.source == "pmc"].cord_uid.unique().compute())
pdf_uids = set(sp[sp.source == "pdf"].cord_uid.unique().compute())
overlap = pmc_uids & pdf_uids
print(f"silver distinct papers: pmc={len(pmc_uids)} pdf-only={len(pdf_uids)} "
      f"total={len(pmc_uids | pdf_uids)}")
assert len(overlap) == 0, f"prefer-pmc violated: {len(overlap)} papers keep both sources"
print("prefer-pmc invariant (no paper keeps both sources): OK")

sil_para_uids = pmc_uids | pdf_uids
assert sil_para_uids <= papers_set, "paragraphs reference cord_uid not in papers!"
print("referential integrity paragraphs.cord_uid ⊆ papers.cord_uid: OK")

cov = int((ps.has_pdf | ps.has_pmc).sum())
print(f"coverage: {len(sil_para_uids)} papers with body text "
      f"(of {cov} papers flagged with ≥1 parse; gap = parses with empty body)")

banner("ALL SANITY CHECKS PASSED")
