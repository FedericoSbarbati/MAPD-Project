"""Task 2.3.4 - cosine similarity between all the pairs of titles.

Normalising the vectors ONCE makes both denominators 1, so "all the pairs" is not a
double loop: it is the matrix product S = X @ X.T, which NumPy hands to BLAS. The N rows
are cut into k blocks, and the TILE (i, j) is X[i] @ X[j].T - a task that needs nobody
else. S is symmetric, so only the tiles with i <= j are computed: k(k+1)/2 tasks.

The result is BIGGER than the input: at 100,000 titles the vectors are 120 MB but S
would be 40 GB. It is not written and it is not needed - the assignment asks to IDENTIFY
a few extreme pairs, not to keep them all. So every tile REDUCES IN PLACE (top 20,
bottom 20, histogram) and gives back 40 rows instead of millions of numbers. It is
exact: the globally most similar pair is necessarily the most similar one of its tile.

    python nicco_scripts/cosine.py nicco_scripts/embeddings --titoli 100000 --out ~/mapd-out/2_3_4
"""

import argparse
import os
import sys
import time
from pathlib import Path

# ONE BLAS thread per task. BLAS auto-parallelizes and by default grabs all cores:
# with 4 workers multiplying together that would be 16 threads on 4 cores tripping
# over each other, and the process/thread comparison would measure that chaos instead of Dask.
# The parallelism here is done by Dask, not BLAS. It must be set BEFORE numpy loads the
# library: for the worker processes, which are spawned later, `single_thread_blas()` takes care of it.
for _variabile in ("OPENBLAS_NUM_THREADS", "OMP_NUM_THREADS",
                   "MKL_NUM_THREADS", "VECLIB_MAXIMUM_THREADS"):
    os.environ.setdefault(_variabile, "1")

import numpy as np           # noqa: E402
import pandas as pd          # noqa: E402
from dask import delayed     # noqa: E402

DEFAULT_INPUT = "nicco_scripts/embeddings"
DEFAULT_PAPERS = "data/silver/papers"
DEFAULT_OUTPUT = "~/mapd-out/2_3_4"

# The fraction of the corpus being worked on. The cost scales as N^2: 969,021 titles are
# 4.7e11 pairs, measurable once but not dozens of times in a campaign. 100,000 (roughly N/10)
# cost 69.6 s on a Mac core and ~20-30 s on the full cluster: enough to measure
# actual computation rather than coordination overhead.
DEFAULT_TITLES = 100_000

# How many blocks the rows are cut into. It's NOT just parallelism granularity: it's
# also the memory peak per task, which scales as (N/k)^2. See `top_block`.
#
# 32 was MEASURED on the cluster (2026-09-12, 6 runs), but not for the time: at k=16 it
# gains 3.4% at 2.1 sigma, which alone wouldn't settle anything. What decides it is the
# peak per task, four times lower (0.12 GB vs 0.47), because at 100,000 titles k=8 and k=4
# blow up and kill the workers. The default costs the same and sits TWO steps from the wall instead of one.
DEFAULT_BLOCKS = 32

TOP_N = 20
BINS = 100
SEED = 0

VETTORE = [f"v{i}" for i in range(300)]


def single_thread_blas():
    """The BLAS variables for the worker processes, which are spawned after us.

Same hook and same reason as `configure_memory()` in `cluster.py`: they need to
reach the worker WHEN IT'S SPAWNED, before numpy loads the library, and
`worker_options={"env": ...}` arrives too late. `configure_memory` does `update` on
the same dictionary, so calling this BEFORE `get_client` doesn't take anything away
from that one and doesn't force touching `cluster.py`, which is shared across the four tasks.
"""
    import dask
    import distributed  # noqa: F401  la chiave nanny nasce quando distributed si importa

    environ = dict(dask.config.get("distributed.nanny.pre-spawn-environ"))
    environ.update({v: "1" for v in ("OPENBLAS_NUM_THREADS", "OMP_NUM_THREADS",
                                     "MKL_NUM_THREADS", "VECLIB_MAXIMUM_THREADS")})
    dask.config.set({"distributed.nanny.pre-spawn-environ": environ})


# Reading and preparation. Costs a few seconds and is identical at every point of the
# curves: it's OUTSIDE the timer, like the CSV writing in 2.3.2.


def embedding_files(path):
    """I part.<n>.parquet ordinati per n, non alfabeticamente (part.10 dopo part.9)."""
    def numero(file):
        pezzi = file.stem.split(".")
        return int(pezzi[-1]) if pezzi[-1].isdigit() else -1

    return sorted(Path(path).glob("*.parquet"), key=lambda f: (numero(f), f.name))


def eligible_uids(papers):
    """The cord_uid of papers with a UNIQUE title, as an Arrow array.

Converted ONCE here and not per file: `pc.is_in` with an Arrow `value_set` uses
the vectorized kernel, while a Python `set` would get reconverted on every partition.
It's the hotspot named and shamed in `docs/MEMORY_LEAK_REPORT.md` (~170x).
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    tavola = pq.read_table(papers, columns=["cord_uid", "is_title_unique"])
    unici = tavola.filter(tavola.column("is_title_unique"))
    return pa.array(unici.column("cord_uid").to_pylist())


def load_vectors(path, n_titles=DEFAULT_TITLES, seed=SEED, min_words=0, unici=None):
    """A sample of n_titles titles -> (cord_uid, X).

    `min_words` discards titles for which the model recognized fewer than N words:
    at N=1 it discards nothing (the minimum in the data is 1), at N=2 it's 2.24%, at N=3 it's 4.96%.
    `unici` (Arrow array from `eligible_uids`) keeps only non-duplicate titles.
    Both are off by default: they are ANALYSIS DECISIONS, and the silver layer flags
    without deciding. They're meant to measure what changes, not to change it silently.

    A QUOTA FROM EACH FILE instead of the first n_titles: the first 100,000 would all
    sit inside part.0, and the file order is the one 2.3.3 wrote them in, not a
    guarantee of shuffling. The seed makes the sample reproducible, which is the
    condition for two benchmark measurements to be comparable.

    Files are read one at a time and only its quota is kept: the peak is one file (146 MB),
    not the billion-one-hundred of the whole folder.
    """
    import pyarrow.compute as pc
    import pyarrow.parquet as pq

    files = embedding_files(path)
    generatore = np.random.default_rng(seed)
    uid, blocchi = [], []

    for posizione, file in enumerate(files):
        quota = (posizione + 1) * n_titles // len(files) - posizione * n_titles // len(files)
        tabella = pq.read_table(file)

        # Filters BEFORE sampling: you choose among the eligible titles, you don't
        # discard ones already chosen - otherwise the sample would shrink and the
        # timings would no longer be comparable across configurations.
        ammessi = np.flatnonzero(tabella.column("n_words").to_numpy() >= min_words)
        if unici is not None:
            tenere = pc.is_in(tabella.column("cord_uid"), value_set=unici).to_numpy(
                zero_copy_only=False)
            ammessi = ammessi[tenere[ammessi]]

        righe = min(quota, len(ammessi))
        scelte = np.sort(generatore.choice(ammessi, size=righe, replace=False))

        uid.append(np.asarray(tabella.column("cord_uid").to_pylist())[scelte])
        blocchi.append(np.column_stack([tabella.column(c).to_numpy() for c in VETTORE])[scelte])

    return np.concatenate(uid), np.ascontiguousarray(np.concatenate(blocchi))


def normalize(X):
    """X / ||X||, so that cosine similarity is the pure dot product.

    No guard against division by zero: on the real data the minimum norm is 0.36 and
    `n_words` is never 0 (verified). The repo's rule is to add cleanup after
    measuring that it's actually needed.
    """
    return X / np.linalg.norm(X, axis=1, keepdims=True)


# The core: the tiles.


def split_rows(n, k):
    """The n rows in k contiguous slices -> [(start, end), ...]."""
    k = max(1, min(int(k), n))
    return [(i * n // k, (i + 1) * n // k) for i in range(k)]


def block_pairs(k):
    """The tiles to compute: only i <= j, because S is symmetric. Half the work."""
    return [(i, j) for i in range(k) for j in range(i, k)]


def top_block(A, B, offset_a, offset_b, top=TOP_N, bins=BINS):
    """A tile: compute it and reduce it immediately -> (table of 2*top rows, histogram).

    The reduction is the reason the task is feasible: the tile is millions of
    numbers, what comes out is 40 rows and 100 counts.

    On the DIAGONAL (offset_a == offset_b) only the narrow upper triangle is kept: the
    true diagonal is every title with itself (1.0 guaranteed) and the lower triangle is
    a repeat of the same thing. Values are extracted instead of zeroed out, because the
    zeros would end up in the histogram and in the minimums - and the real minimums are
    close to zero.

    MEMORY PEAK, ~12-14 (N/k)^2 bytes: the matrix (4 bytes/value) plus the indices
    that `argpartition` and `triu_indices` produce (8 bytes each). At 100,000 titles that's
    ~550 MB at k=16 and ~140 MB at k=32, and they need to be MULTIPLIED by the worker's
    threads, which compute different tiles at the same time. It's the wall that makes
    low k values unmeasurable.
    """
    S = A @ B.T

    if offset_a == offset_b:
        righe, colonne = np.triu_indices(S.shape[0], k=1)
        valori = S[righe, colonne]
    else:
        valori = S.ravel()

    # The cosine of two normalized vectors lies in [-1, 1] by definition, but in float32
    # two identical vectors give 1.0000001: without this clamp those pairs fall OUTSIDE
    # the histogram range and disappear from the count. In-place, so it's free.
    np.clip(valori, -1.0, 1.0, out=valori)
    conteggi = np.histogram(valori, bins=bins, range=(-1.0, 1.0))[0]

    quanti = min(top, valori.size)                 # little tiles for local tries
    alti = np.argpartition(valori, -quanti)[-quanti:]
    bassi = np.argpartition(valori, quanti - 1)[:quanti]
    scelte = np.concatenate([alti, bassi])

    # From the tile's LOCAL indices to the sample's global ones: it's the only place
    # where the blocks need to know where they stood.
    if offset_a == offset_b:
        r, c = righe[scelte], colonne[scelte]
    else:
        r, c = np.unravel_index(scelte, S.shape)

    return pd.DataFrame({"a": offset_a + r, "b": offset_b + c, "sim": valori[scelte]}), conteggi


def merge(pezzi, top=TOP_N):
    """The reduce: the rankings of the tiles -> the global ranking.

    Exact and not approximated: a pair that is not among the first `top` of its
    tile cannot be among the first `top` of all.
    """
    tabella = pd.concat([frame for frame, _ in pezzi], ignore_index=True)
    conteggi = np.sum([c for _, c in pezzi], axis=0)
    return (tabella.nlargest(top, "sim").reset_index(drop=True),
            tabella.nsmallest(top, "sim").reset_index(drop=True),
            conteggi)


def build(client, X, k=DEFAULT_BLOCKS, top=TOP_N, bins=BINS):
    """The complete graph. Lazy: nothing is calculated here.

    `scatter` sends the k blocks to the workers ONCE; each tile then reuses two.
    Without this, the same megabytes would travel through the graph with every task.
    """
    tagli = split_rows(len(X), k)
    blocchi = client.scatter([X[a:b] for a, b in tagli])
    pezzi = [delayed(top_block)(blocchi[i], blocchi[j], tagli[i][0], tagli[j][0], top, bins)
             for i, j in block_pairs(len(tagli))]
    return delayed(merge)(pezzi, top)


# The handoff: on the client, on 40 rows. Outside the timer.

def attach_titles(coppie, uid, papers):
    """Indici del campione -> cord_uid -> titoli. Senza, l'output e' due codici opachi."""
    import pyarrow.parquet as pq

    titoli = (pq.read_table(papers, columns=["cord_uid", "title"]).to_pandas()
                .drop_duplicates("cord_uid").set_index("cord_uid")["title"])
    frame = pd.DataFrame({"cord_uid_a": uid[coppie["a"].to_numpy()],
                          "cord_uid_b": uid[coppie["b"].to_numpy()],
                          "similarity": coppie["sim"].to_numpy()})
    frame["title_a"] = frame["cord_uid_a"].map(titoli)
    frame["title_b"] = frame["cord_uid_b"].map(titoli)
    return frame


def histogram_plot(conteggi, path, titolo):
    """The task's only figure: where the mass of the similarities lies, and how far
apart the tails we delivered are."""
    import matplotlib

    matplotlib.use("Agg")                      # nessuno schermo sulla VM
    import matplotlib.pyplot as plt

    bordi = np.linspace(-1.0, 1.0, len(conteggi) + 1)
    centri = (bordi[:-1] + bordi[1:]) / 2
    fig, ax = plt.subplots(figsize=(9, 4))
    ax.bar(centri, conteggi, width=bordi[1] - bordi[0], color="#2f6f73")
    ax.set_yscale("log")                       # le code sono 6 ordini di grandezza sotto
    ax.set_xlabel("similarita' coseno")
    ax.set_ylabel("coppie (scala log)")
    ax.set_title(titolo)
    ax.grid(axis="y", alpha=0.25)
    fig.tight_layout()
    fig.savefig(path, dpi=150)
    plt.close(fig)


# CLI arguments for the cosine-similarity task: sample size, block count for
# partitioning, and the optional title-quality filters (min-parole, solo-unici).


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("input", nargs="?", default=DEFAULT_INPUT,
                        help=f"cartella degli embedding (default {DEFAULT_INPUT})")
    parser.add_argument("--papers", default=DEFAULT_PAPERS,
                        help=f"silver/papers, da cui si ripescano i titoli (default {DEFAULT_PAPERS})")
    parser.add_argument("--out", default=DEFAULT_OUTPUT,
                        help=f"dove scrivere i risultati (default {DEFAULT_OUTPUT})")
    parser.add_argument("--titoli", type=int, default=DEFAULT_TITLES,
                        help=f"quanti titoli (default {DEFAULT_TITLES}, circa N/10). Il "
                             "costo va come il QUADRATO di questo numero")
    parser.add_argument("--blocchi", type=int, default=DEFAULT_BLOCKS,
                        help=f"in quanti blocchi tagliare le righe (default {DEFAULT_BLOCKS}). "
                             "E' il pomello della curva obbligatoria sulle partizioni, ed e' "
                             "anche il picco di memoria per task: vedi top_block")
    parser.add_argument("--top", type=int, default=TOP_N,
                        help=f"quante coppie in cima e in fondo (default {TOP_N})")
    parser.add_argument("--seed", type=int, default=SEED,
                        help=f"il seed del campione (default {SEED}): due run confrontabili "
                             "devono guardare gli stessi titoli")
    parser.add_argument("--min-parole", type=int, default=0, metavar="N",
                        help="tiene solo i titoli con almeno N parole riconosciute dal "
                             "modello (default 0, nessun filtro). Con N=1 il vettore del "
                             "titolo E' il vettore di quella parola, e titoli che non "
                             "c'entrano niente risultano identici al 100%%")
    parser.add_argument("--solo-unici", action="store_true",
                        help="tiene solo i titoli non duplicati (is_title_unique in "
                             "silver/papers). Il 28%% dei paper ha un titolo ripetuto, e "
                             "quelle coppie valgono 1,0000 per costruzione")
    return parser.parse_args()


def main():
    args = parse_args()

    # Relative paths are resolved against the repo root, not the folder you launch from
    repo = Path(__file__).resolve().parent.parent
    sys.path.insert(0, str(repo))
    from cluster import get_client

    def risolvi(percorso):
        percorso = Path(percorso).expanduser()
        return percorso if percorso.is_absolute() else repo / percorso

    source, papers, out = risolvi(args.input), risolvi(args.papers), risolvi(args.out)
    if not embedding_files(source):
        raise SystemExit(f"Nessun file .parquet in {source}")
    # The titles are only needed at the end, but the check happens NOW: on the cluster the
    # data doesn't live inside the repo, and the relative default doesn't exist there.
    # Finding this out after the computation would mean throwing the computation away.  
    if not list(Path(papers).glob("*.parquet")):
        raise SystemExit(f"Nessun file .parquet in {papers}\n"
                         "Sul cluster passa --papers ~/mapd-data/silver/papers")
    out.mkdir(parents=True, exist_ok=True)

    unici = eligible_uids(papers) if args.solo_unici else None
    uid, X = load_vectors(source, args.titoli, args.seed, args.min_parole, unici)
    X = normalize(X)
    coppie = len(X) * (len(X) - 1) // 2
    print("input     :", source)
    print("output    :", out)
    print(f"filtri    : min_parole={args.min_parole}  solo_unici={args.solo_unici}")
    print(f"titoli    : {len(X):,}  ({X.nbytes / 1e6:.0f} MB)  ->  {coppie:,} coppie")
    if len(X) < args.titoli:
        print(f"            ATTENZIONE: chiesti {args.titoli:,}, i filtri ne lasciano "
              f"{len(X):,}. I tempi non sono confrontabili con un run non filtrato")

    single_thread_blas()                       # prima che nasca qualunque worker
    client, cluster = get_client(repo_root=repo)
    lato = len(X) // args.blocchi
    print(f"blocchi   : {args.blocchi} da {lato} righe -> "
          f"{args.blocchi * (args.blocchi + 1) // 2} piastrelle da {lato}x{lato}")

    # Inside the timer: `scatter` of the blocks + all the tiles + the reduce. The
    # scatter belongs inside on purpose - it's distributed work and its cost grows with
    # the number of workers, which is exactly what the worker curve is meant to show.
    inizio = time.perf_counter()
    try:
        simili, dissimili, conteggi = client.compute(
            build(client, X, args.blocchi, args.top, BINS), sync=True)
        elapsed = time.perf_counter() - inizio
    finally:
        client.close()
        if cluster is not None:
            cluster.close()

    simili = attach_titles(simili, uid, papers)
    dissimili = attach_titles(dissimili, uid, papers)
    simili.to_csv(out / "most_similar.csv", index=False)
    dissimili.to_csv(out / "most_dissimilar.csv", index=False)
    np.savetxt(out / "histogram.csv", conteggi, fmt="%d")
    histogram_plot(conteggi, out / "histogram.png",
                   f"distribuzione delle similarita' - {len(X):,} titoli, {coppie:,} coppie")

    def mostra(frame, etichetta):
        print(f"\n=== {etichetta} ===")
        for _, riga in frame.iterrows():
            print(f"  {riga['similarity']:+.4f}  {str(riga['title_a'])[:58]:58}  ||  "
                  f"{str(riga['title_b'])[:58]}")

    mostra(simili, f"le {args.top} coppie piu' SIMILI")
    mostra(dissimili, f"le {args.top} coppie piu' DISSIMILI")
    # L'istogramma conta OGNI coppia una volta sola: se questi due numeri coincidono, la
    # divisione in piastrelle non ha perso ne' duplicato niente. E' l'invariante del task.
    print(f"\nsimilarita': min {dissimili['similarity'].min():+.4f}  "
          f"max {simili['similarity'].max():+.4f}")
    print(f"coppie      : {conteggi.sum():,} contate su {coppie:,} attese  "
          f"{'OK' if conteggi.sum() == coppie else 'DISALLINEATE'}")
    print(f"elapsed: {elapsed:.1f} s")
    print("scritti :", out)


if __name__ == "__main__":
    main()
