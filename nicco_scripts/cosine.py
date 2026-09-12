"""Task 2.3.4 - similarita' coseno fra tutte le coppie di titoli.

    python nicco_scripts/cosine.py nicco_scripts/embeddings --titoli 100000 --out ~/mapd-out/2_3_4

L'IDEA IN TRE RIGHE. La similarita' coseno fra due vettori e' il loro prodotto scalare
diviso per le due lunghezze; se si normalizzano i vettori UNA VOLTA (costo N x 300, cioe'
niente), i denominatori diventano 1 e la similarita' e' il solo prodotto scalare. Allora
"tutte le coppie" non e' un doppio ciclo: e' la moltiplicazione di matrici S = X @ X.T,
che NumPy passa a BLAS (C/Fortran, centinaia di miliardi di operazioni al secondo).

COME SI DISTRIBUISCE. Le N righe si tagliano in k blocchi; la PIASTRELLA (i,j) e'
X[i] @ X[j].T, un task che non ha bisogno di nessun altro. S e' simmetrica, quindi si
calcolano solo le piastrelle con i <= j: k(k+1)/2 task.

IL PUNTO CHE DA' FORMA A TUTTO IL FILE: il risultato e' piu' grande dell'input. A 100.000
titoli i vettori sono 120 MB ma S sarebbe 40 GB, e sul corpus intero 3,7 TB. Non si scrive
e non serve: la consegna chiede di IDENTIFICARE alcune coppie estreme, non di conservarle
tutte. Quindi ogni piastrella RIDUCE SUL POSTO - top 20, bottom 20, istogramma - e
restituisce 40 righe invece di milioni di numeri. E' un Map/Reduce, ed e' ESATTO: la
coppia globalmente piu' simile e' per forza la piu' simile della propria piastrella.

Conseguenza pratica: qui i dati si REPLICANO e si distribuisce il CALCOLO (l'opposto del
word count), quindi gli embedding servono solo sulla macchina da cui si lancia.

Perche' `delayed` e non le altre collezioni: un DataFrame farebbe "tutte le coppie" con un
cross join, cioe' materializzando 470 miliardi di righe; un Bag avrebbe come elementi
coppie di indici, non dati, e aggiungerebbe una manopola (`npartitions`) che nei benchmark
si confonde con k; `dask.array` calcolerebbe tutte e k^2 le piastrelle, perche' non sa che
S e' simmetrica. -> nicco_scripts/README.md
"""

import argparse
import os
import sys
import time
from pathlib import Path

# UN thread BLAS per task. BLAS si auto-parallelizza e di default si prende tutti i core:
# con 4 worker che moltiplicano insieme sarebbero 16 thread su 4 core a pestarsi i piedi,
# e il confronto processi/thread misurerebbe quel caos invece di Dask. Il parallelismo qui
# lo fa Dask, non BLAS. Va impostato PRIMA che numpy carichi la libreria: per i processi
# worker, che nascono dopo, ci pensa `single_thread_blas()`.
for _variabile in ("OPENBLAS_NUM_THREADS", "OMP_NUM_THREADS",
                   "MKL_NUM_THREADS", "VECLIB_MAXIMUM_THREADS"):
    os.environ.setdefault(_variabile, "1")

import numpy as np           # noqa: E402
import pandas as pd          # noqa: E402
from dask import delayed     # noqa: E402

DEFAULT_INPUT = "nicco_scripts/embeddings"
DEFAULT_PAPERS = "data/silver/papers"
DEFAULT_OUTPUT = "~/mapd-out/2_3_4"

# La frazione di corpus su cui si lavora. Il costo va come N^2: 969.021 titoli sono 4,7e11
# coppie, misurabili una volta ma non decine di volte in una campagna. 100.000 (circa N/10)
# costano 69,6 s su un core del Mac e ~20-30 s sul cluster pieno: abbastanza per misurare
# calcolo vero e non coordinamento.
DEFAULT_TITLES = 100_000

# In quanti blocchi si tagliano le righe. NON e' solo granularita' di parallelismo: e'
# anche il picco di memoria per task, che va come (N/k)^2. Vedi `top_block`.
DEFAULT_BLOCKS = 16

TOP_N = 20
BINS = 100
SEED = 0

VETTORE = [f"v{i}" for i in range(300)]


def single_thread_blas():
    """Le variabili BLAS ai processi worker, che nascono dopo di noi.

    Stesso aggancio e stessa ragione di `configure_memory()` in `cluster.py`: devono
    arrivare al worker QUANDO NASCE, prima che numpy carichi la libreria, e
    `worker_options={"env": ...}` arriva tardi. `configure_memory` fa `update` sullo
    stesso dizionario, quindi chiamare questa PRIMA di `get_client` non toglie niente a
    quella e non obbliga a toccare `cluster.py`, che e' condiviso fra i quattro task.
    """
    import dask
    import distributed  # noqa: F401  la chiave nanny nasce quando distributed si importa

    environ = dict(dask.config.get("distributed.nanny.pre-spawn-environ"))
    environ.update({v: "1" for v in ("OPENBLAS_NUM_THREADS", "OMP_NUM_THREADS",
                                     "MKL_NUM_THREADS", "VECLIB_MAXIMUM_THREADS")})
    dask.config.set({"distributed.nanny.pre-spawn-environ": environ})


# ----------------------------------------------------------------------------------
# Lettura e preparazione. Costa pochi secondi ed e' identica in ogni punto delle curve:
# sta FUORI dal cronometro, come la scrittura dei CSV nel 2.3.2.
# ----------------------------------------------------------------------------------


def embedding_files(path):
    """I part.<n>.parquet ordinati per n, non alfabeticamente (part.10 dopo part.9)."""
    def numero(file):
        pezzi = file.stem.split(".")
        return int(pezzi[-1]) if pezzi[-1].isdigit() else -1

    return sorted(Path(path).glob("*.parquet"), key=lambda f: (numero(f), f.name))


def load_vectors(path, n_titles=DEFAULT_TITLES, seed=SEED):
    """Un campione di n_titles titoli -> (cord_uid, X).

    Una QUOTA DA OGNI FILE invece dei primi n_titles: i primi 100.000 starebbero tutti
    dentro part.0, e l'ordine dei file e' quello con cui il 2.3.3 li ha scritti, non una
    garanzia di mescolamento. Il seed rende il campione riproducibile, che e' la
    condizione perche' due misure del benchmark siano confrontabili.

    Si legge un file per volta e si tiene solo la sua quota: il picco e' un file (146 MB),
    non il miliardo e cento dell'intera cartella.
    """
    import pyarrow.parquet as pq

    files = embedding_files(path)
    generatore = np.random.default_rng(seed)
    uid, blocchi = [], []

    for posizione, file in enumerate(files):
        quota = (posizione + 1) * n_titles // len(files) - posizione * n_titles // len(files)
        tabella = pq.read_table(file)
        righe = min(quota, tabella.num_rows)
        scelte = np.sort(generatore.choice(tabella.num_rows, size=righe, replace=False))

        uid.append(np.asarray(tabella.column("cord_uid").to_pylist())[scelte])
        blocchi.append(np.column_stack([tabella.column(c).to_numpy() for c in VETTORE])[scelte])

    return np.concatenate(uid), np.ascontiguousarray(np.concatenate(blocchi))


def normalize(X):
    """X / ||X||, cosi' la similarita' coseno e' il puro prodotto scalare.

    Nessuna guardia sulla divisione per zero: sui dati veri la norma minima e' 0,36 e
    `n_words` non e' mai 0 (verificato). La regola del repo e' aggiungere pulizia dopo
    averne misurato la necessita'.
    """
    return X / np.linalg.norm(X, axis=1, keepdims=True)


# ----------------------------------------------------------------------------------
# Il cuore: le piastrelle.
# ----------------------------------------------------------------------------------


def split_rows(n, k):
    """Le n righe in k fette contigue -> [(inizio, fine), ...]."""
    k = max(1, min(int(k), n))
    return [(i * n // k, (i + 1) * n // k) for i in range(k)]


def block_pairs(k):
    """Le piastrelle da calcolare: solo i <= j, perche' S e' simmetrica. Meta' lavoro."""
    return [(i, j) for i in range(k) for j in range(i, k)]


def top_block(A, B, offset_a, offset_b, top=TOP_N, bins=BINS):
    """UNA piastrella: la calcola e la riduce subito -> (tabella di 2*top righe, istogramma).

    La riduzione e' il motivo per cui il task e' fattibile: la piastrella e' milioni di
    numeri, quello che esce sono 40 righe e 100 conteggi.

    Sulla DIAGONALE (offset_a == offset_b) si tiene solo il triangolo alto stretto: la
    diagonale vera e' ogni titolo con se stesso (1,0 garantito) e il triangolo basso e'
    la ripetizione dello stesso. Si estraggono i valori invece di azzerarli, perche' gli
    zeri finirebbero nell'istogramma e nei minimi - e i minimi veri sono vicini a zero.

    PICCO DI MEMORIA, ~12-14 (N/k)^2 byte: la matrice (4 byte/valore) piu' gli indici
    che `argpartition` e `triu_indices` producono (8 byte l'uno). A 100.000 titoli sono
    ~550 MB a k=16 e ~140 MB a k=32, e vanno MOLTIPLICATI per i thread del worker, che
    calcolano piastrelle diverse insieme. E' il muro che rende i k bassi non misurabili.
    """
    S = A @ B.T

    if offset_a == offset_b:
        righe, colonne = np.triu_indices(S.shape[0], k=1)
        valori = S[righe, colonne]
    else:
        valori = S.ravel()

    # Il coseno di due vettori normalizzati sta in [-1, 1] per definizione, ma in float32
    # due vettori identici danno 1.0000001: senza questo taglio quelle coppie cadono FUORI
    # dal range dell'istogramma e spariscono dal conteggio. In-place, quindi gratis.
    np.clip(valori, -1.0, 1.0, out=valori)
    conteggi = np.histogram(valori, bins=bins, range=(-1.0, 1.0))[0]

    quanti = min(top, valori.size)                 # piastrelle minuscole nelle prove locali
    alti = np.argpartition(valori, -quanti)[-quanti:]
    bassi = np.argpartition(valori, quanti - 1)[:quanti]
    scelte = np.concatenate([alti, bassi])

    # Dagli indici LOCALI della piastrella a quelli globali del campione: e' l'unico posto
    # in cui i blocchi hanno bisogno di sapere dove stavano.
    if offset_a == offset_b:
        r, c = righe[scelte], colonne[scelte]
    else:
        r, c = np.unravel_index(scelte, S.shape)

    return pd.DataFrame({"a": offset_a + r, "b": offset_b + c, "sim": valori[scelte]}), conteggi


def merge(pezzi, top=TOP_N):
    """Il reduce: le classifiche delle piastrelle -> la classifica globale.

    Esatto e non approssimato: una coppia che non e' fra le prime `top` della sua
    piastrella non puo' essere fra le prime `top` di tutte.
    """
    tabella = pd.concat([frame for frame, _ in pezzi], ignore_index=True)
    conteggi = np.sum([c for _, c in pezzi], axis=0)
    return (tabella.nlargest(top, "sim").reset_index(drop=True),
            tabella.nsmallest(top, "sim").reset_index(drop=True),
            conteggi)


def build(client, X, k=DEFAULT_BLOCKS, top=TOP_N, bins=BINS):
    """Il grafo completo. Lazy: niente viene calcolato qui.

    `scatter` spedisce i k blocchi ai worker UNA VOLTA; ogni piastrella poi ne riusa due.
    Senza, gli stessi megabyte viaggerebbero dentro il grafo a ogni task.
    """
    tagli = split_rows(len(X), k)
    blocchi = client.scatter([X[a:b] for a, b in tagli])
    pezzi = [delayed(top_block)(blocchi[i], blocchi[j], tagli[i][0], tagli[j][0], top, bins)
             for i, j in block_pairs(len(tagli))]
    return delayed(merge)(pezzi, top)


# ----------------------------------------------------------------------------------
# La consegna: sul client, su 40 righe. Fuori dal cronometro.
# ----------------------------------------------------------------------------------


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
    """L'unica figura del task: dove sta la massa delle similarita', e quanto sono
    lontane le code che abbiamo consegnato."""
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


# ----------------------------------------------------------------------------------


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
    return parser.parse_args()


def main():
    args = parse_args()

    # I percorsi relativi si risolvono sulla radice della repo, non sulla cartella da cui lanci
    repo = Path(__file__).resolve().parent.parent
    sys.path.insert(0, str(repo))
    from cluster import get_client

    def risolvi(percorso):
        percorso = Path(percorso).expanduser()
        return percorso if percorso.is_absolute() else repo / percorso

    source, papers, out = risolvi(args.input), risolvi(args.papers), risolvi(args.out)
    if not embedding_files(source):
        raise SystemExit(f"Nessun file .parquet in {source}")
    # I titoli servono solo alla fine, ma si controlla ADESSO: sul cluster i dati non stanno
    # dentro la repo, e il default relativo non esiste la'. Scoprirlo dopo il calcolo
    # significherebbe buttare il calcolo.
    if not list(Path(papers).glob("*.parquet")):
        raise SystemExit(f"Nessun file .parquet in {papers}\n"
                         "Sul cluster passa --papers ~/mapd-data/silver/papers")
    out.mkdir(parents=True, exist_ok=True)

    uid, X = load_vectors(source, args.titoli, args.seed)
    X = normalize(X)
    coppie = len(X) * (len(X) - 1) // 2
    print("input     :", source)
    print("output    :", out)
    print(f"titoli    : {len(X):,}  ({X.nbytes / 1e6:.0f} MB)  ->  {coppie:,} coppie")

    single_thread_blas()                       # prima che nasca qualunque worker
    client, cluster = get_client(repo_root=repo)
    lato = len(X) // args.blocchi
    print(f"blocchi   : {args.blocchi} da {lato} righe -> "
          f"{args.blocchi * (args.blocchi + 1) // 2} piastrelle da {lato}x{lato}")

    # Dentro il cronometro: `scatter` dei blocchi + tutte le piastrelle + il reduce. Lo
    # scatter ci sta dentro apposta - e' lavoro distribuito e il suo costo cresce col
    # numero di worker, che e' esattamente quello che la curva sui worker deve mostrare.
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
