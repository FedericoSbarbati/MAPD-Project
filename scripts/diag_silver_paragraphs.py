#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Diagnostica dello step `silver/paragraphs` (quello che satura la RAM dei worker).

NON e' una pipeline: e' uno strumento di misura. Gira sul bronze GIA' scritto
(data/bronze/paragraphs) e risponde a UNA domanda: perche' il silver/paragraphs
esaurisce la memoria? La "smoke run" (fase E) e' pensata per NON completare: processa
solo le prime DIAG_N partizioni su una cartella usa-e-getta e osserva il picco di RAM.

--------------------------------------------------------------------------------
Come si usa (sullo scheduler della VM, dentro l'env mapd-covid):

  # 1) collegandosi allo scheduler gia' avviato dal notebook (consigliato):
  DASK_SCHEDULER=tcp://<ip_sched>:8786 \
      python scripts/diag_silver_paragraphs.py

  # 2) oppure facendo tirare su il cluster allo script (come il notebook, via SSH):
  CORD19_HOSTS="ip_sched,ip_sched,ip_w1,ip_w2,ip_w3" \
  CORD19_SSH_KEY=~/.ssh/id_rsa \
      python scripts/diag_silver_paragraphs.py

  # 3) locale, per provare lo SCRIPT (non la VM): LocalCluster piccolo
  CORD19_DATA=./data CORD19_WORKERS=2 CORD19_WORKER_MEMORY_LIMIT=2GB \
      python scripts/diag_silver_paragraphs.py

Env utili (oltre a quelle del notebook: DASK_SCHEDULER / CORD19_HOSTS / CORD19_SSH_KEY /
CORD19_WORKERS / CORD19_THREADS_PER_WORKER / CORD19_WORKER_MEMORY_LIMIT / CORD19_DATA):
  DIAG_PHASES   sottoinsieme di "ABCDE" da eseguire            (default "ABCDE")
  DIAG_N        n. partizioni per la smoke run (fase E)        (default 24)
  DIAG_RG_FILES quanti file bronze aprire per la fase A        (default 48)
  DIAG_NFS_MB   MB (uncompressed) del test di throughput D     (default 128)

Fasi (ognuna stampa da sola, si possono disabilitare con DIAG_PHASES):
  A  struttura row-group del bronze  -> la MISURA DECISIVA (row-group piccoli o monster?)
  B  npartitions del read del silver + distribuzione taglia partizioni (da metadata)
  C  probe RAM di UNA partizione: quanto pesa in pandas la partizione piu' grande
  D  throughput di scrittura NFS vs disco locale (quantifica l'ipotesi "NFS lento")
  E  smoke run limitata (prime DIAG_N partizioni) del transform reale, con MemorySampler
--------------------------------------------------------------------------------
"""
import os
import sys
import time
import glob
import pickle

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# ------------------------------------------------------------------ config / paths
REPO      = os.getcwd()
DATA_ROOT = os.environ.get("CORD19_DATA", os.path.join(REPO, "data"))
BRONZE    = os.path.join(DATA_ROOT, "bronze", "paragraphs")
DIAG_ROOT = os.path.join(DATA_ROOT, "_diag")          # tutto l'output usa-e-getta finisce qui
REPORTS   = os.path.join(REPO, "reports")
os.makedirs(DIAG_ROOT, exist_ok=True)
os.makedirs(REPORTS, exist_ok=True)

PHASES      = os.environ.get("DIAG_PHASES", "ABCDEF").upper()
DIAG_N      = int(os.environ.get("DIAG_N", "24"))
RG_FILES    = int(os.environ.get("DIAG_RG_FILES", "48"))
NFS_MB      = int(os.environ.get("DIAG_NFS_MB", "128"))

# cluster (stessa logica del notebook)
DASK_SCHEDULER     = os.environ.get("DASK_SCHEDULER")
CORD19_HOSTS       = os.environ.get("CORD19_HOSTS")
CORD19_SSH_KEY     = os.environ.get("CORD19_SSH_KEY")
N_WORKERS          = int(os.environ.get("CORD19_WORKERS", "4"))
THREADS_PER_WORKER = int(os.environ.get("CORD19_THREADS_PER_WORKER", "4"))
WORKER_MEM         = os.environ.get("CORD19_WORKER_MEMORY_LIMIT", "7GB")

# il transform reale (copiato VERBATIM dal notebook, cosi' la diagnosi riflette il codice vero)
REFERENCE_SECTION_RE = (r"(?:referen|bibliograph|acknowledg|author contrib|"
                        r"conflict|competing interest|funding|declarat|"
                        r"supplement|copyright)")
PARA_SILVER_SCHEMA = pa.schema([("cord_uid", pa.string()), ("paper_id", pa.string()),
                                ("source", pa.string()), ("para_idx", pa.int32()),
                                ("section", pa.string()), ("text", pa.string()),
                                ("is_reference_like", pa.bool_())])


def hr(msg):
    print("\n" + "=" * 78 + f"\n{msg}\n" + "=" * 78)


def human(n_bytes):
    for u in ("B", "KB", "MB", "GB", "TB"):
        if n_bytes < 1024 or u == "TB":
            return f"{n_bytes:6.1f}{u}"
        n_bytes /= 1024


# ============================================================ FASE A: row-group bronze
# VERIFICA (non la causa sulla VM: la' i gruppi sono ~ok). split_row_groups=True fa 1 partizione
# per row-group: se i gruppi sono piccoli (~ROW_GROUP righe) le partizioni silver sono piccole e
# l'OOM NON viene dalla taglia -> vai a C/D/E. Se invece fossero "monster" (dump vecchio senza
# row_group_size) sarebbe quello. Blocco senza cluster: solo i metadata parquet.
def phase_A():
    hr("FASE A  --  struttura row-group del bronze (misura decisiva)")
    files = sorted(glob.glob(os.path.join(BRONZE, "*.parquet")))
    if not files:
        print(f"!! nessun file in {BRONZE} -- salto A"); return None
    # getsize e' cheap anche su NFS: lo faccio su TUTTI per la distribuzione di taglia su disco
    sizes = [(f, os.path.getsize(f)) for f in files]
    sizes.sort(key=lambda t: t[1])
    tot_disk = sum(s for _, s in sizes)
    print(f"{len(files)} file, {human(tot_disk)} su disco (compressi)")
    print(f"  taglia file su disco: min={human(sizes[0][1])} "
          f"med={human(sizes[len(sizes)//2][1])} max={human(sizes[-1][1])}")

    # apro i metadata di un campione che copre l'intervallo di taglia (i piu' piccoli, la mediana,
    # i piu' grandi): i file 'monster' sono quelli che fanno male, voglio vederli di sicuro.
    idx = sorted(set(
        list(range(min(RG_FILES // 3, len(files)))) +                       # piu' piccoli
        [len(files) // 2] +                                                  # mediano
        list(range(max(0, len(files) - RG_FILES // 3 * 2), len(files)))      # piu' grandi
    ))
    rows_tot = rg_tot = 0
    rg_rows_all = []
    rg_bytes_all = []
    print(f"\n  apro i metadata di {len(idx)} file (campione su taglia):")
    print(f"  {'file':>18s} {'rows':>9s} {'#rg':>4s} {'rg_rows max':>12s} {'rg_uncompr max':>14s}")
    for i in idx:
        f = sizes[i][0]
        md = pq.ParquetFile(f).metadata
        nrg = md.num_row_groups
        rr = [md.row_group(k).num_rows for k in range(nrg)]
        rb = [md.row_group(k).total_byte_size for k in range(nrg)]
        rows_tot += md.num_rows; rg_tot += nrg
        rg_rows_all += rr; rg_bytes_all += rb
        print(f"  {os.path.basename(f):>18s} {md.num_rows:9d} {nrg:4d} "
              f"{max(rr):12d} {human(max(rb)):>14s}")

    mx_rows = max(rg_rows_all); mx_bytes = max(rg_bytes_all)
    med_rows = sorted(rg_rows_all)[len(rg_rows_all)//2]
    print(f"\n  --> row-group nel campione: {rg_tot} rg su {rows_tot} righe")
    print(f"      righe/rg   : med={med_rows}  max={mx_rows}")
    print(f"      uncompr/rg : max={human(mx_bytes)}   (questa e' ~la taglia di UNA partizione silver, in Arrow)")
    print(f"      in pandas (text=object) la stessa partizione pesa ~2-4x -> ~{human(mx_bytes*3)}")

    ROW_GROUP = int(os.environ.get("CORD19_ROW_GROUP", "20000"))
    if mx_rows > ROW_GROUP * 2:
        print(f"\n  [DIAGNOSI A] row-group MONSTER: max {mx_rows} righe >> ROW_GROUP={ROW_GROUP}.")
        print(f"              row_group_size NON e' stato applicato a questo bronze (dump vecchio,")
        print(f"              o scritto con codice/dask che non lo onorava). split_row_groups=True")
        print(f"              produce partizioni da ~{human(mx_bytes*3)} in RAM: e' la causa dell'OOM.")
        print(f"              -> RISCRIVI il bronze con row_group_size, o ripartiziona il silver.")
    else:
        print(f"\n  [DIAGNOSI A] row-group piccoli (max {mx_rows} <= ~2xROW_GROUP): OK.")
        print(f"              Le partizioni del silver sono piccole: l'OOM NON viene dalla taglia")
        print(f"              partizione. Guarda le fasi C/D/E (RAM effettiva, NFS, backpressure).")
    return mx_bytes


# ============================================================ FASE B: npartitions silver
def phase_B():
    hr("FASE B  --  read del silver: npartitions + taglia partizioni")
    import dask.dataframe as dd
    para = dd.read_parquet(BRONZE, engine="pyarrow", split_row_groups=True)
    print(f"  dd.read_parquet(split_row_groups=True) -> npartitions = {para.npartitions}")
    print(f"  colonne: {list(para.columns)}")
    print(f"  con {N_WORKERS} worker x {THREADS_PER_WORKER} thread = {N_WORKERS*THREADS_PER_WORKER} partizioni IN VOLO insieme.")
    print(f"  (se una partizione pesa P MB in RAM, il picco parte da ~{4*THREADS_PER_WORKER}xP MB")
    print(f"   PRIMA di contare i buffer di scrittura zstd trattenuti durante l'I/O su NFS.)")
    print(f"  (NB: {N_WORKERS} = CORD19_WORKERS; sul cluster reale conta i worker della dashboard.)")
    return para.npartitions


# ============================================================ FASE C: RAM di 1 partizione
# Carico la partizione PEGGIORE (il row-group piu' grande) come pandas e misuro memory_usage(deep):
# e' il numero che spiega l'OOM. La taglia "uncompressed" del parquet (fase A) sottostima la RAM
# vera perche' le stringhe object in pandas hanno un overhead enorme per-oggetto.
def phase_C():
    hr("FASE C  --  RAM reale di UNA partizione (worst case)")
    files = sorted(glob.glob(os.path.join(BRONZE, "*.parquet")), key=os.path.getsize)
    if not files:
        print(f"!! nessun file in {BRONZE} -- salto C"); return
    f = files[-1]                                  # file piu' grande su disco
    pfm = pq.ParquetFile(f)
    md = pfm.metadata
    # row-group piu' grande dentro il file piu' grande
    k = max(range(md.num_row_groups), key=lambda i: md.row_group(i).total_byte_size)
    uncompr = md.row_group(k).total_byte_size
    nrows = md.row_group(k).num_rows
    print(f"  carico {os.path.basename(f)} row-group {k}: {nrows} righe, "
          f"{human(uncompr)} uncompressed (Arrow)")
    t0 = time.time()
    pdf = pfm.read_row_group(k).to_pandas()
    dt = time.time() - t0
    deep = pdf.memory_usage(deep=True).sum()
    text_bytes = pdf["text"].memory_usage(deep=True) if "text" in pdf else 0
    print(f"  -> pandas in RAM: {human(deep)}  (di cui text: {human(text_bytes)})   [read {dt:.1f}s]")
    print(f"  -> multiplo Arrow->pandas: {deep/max(uncompr,1):.1f}x")
    conc = N_WORKERS * THREADS_PER_WORKER
    print(f"\n  [DIAGNOSI C] con {conc} partizioni in volo: ~{human(deep*conc)} di picco 'nudo',")
    print(f"              spalmato su {N_WORKERS} worker = ~{human(deep*conc/max(N_WORKERS,1))}/worker (limite {WORKER_MEM}).")
    del pdf
    return deep


# ============================================================ FASE D: throughput NFS vs locale
# Scrive lo STESSO parquet (zstd, come il codice vero) su NFS e su /tmp locale e cronometra.
# Se NFS << locale, la scrittura e' il collo di bottiglia: le partizioni finite restano in RAM
# in attesa di essere scritte (overproduction) -> memoria che sale = la tua ipotesi.
def phase_D():
    hr("FASE D  --  throughput scrittura: NFS (volume dati) vs disco locale")
    import numpy as np
    # ~NFS_MB di dati 'realistici': stringhe di testo tipo paragrafo
    nrows = max(1, NFS_MB * 1024 * 1024 // 200)
    pdf = pd.DataFrame({
        "cord_uid": ["abcd1234"] * nrows,
        "text": ["lorem ipsum dolor sit amet " * 6] * nrows,
    })
    tbl = pa.Table.from_pandas(pdf, preserve_index=False)
    in_mem = pdf.memory_usage(deep=True).sum()
    print(f"  payload: {nrows} righe, ~{human(in_mem)} in RAM")

    def _bench(path, label):
        t0 = time.time()
        pq.write_table(tbl, path, compression="zstd")
        # forza il flush su disco/rete: senza fsync misuro solo la cache di pagina
        fd = os.open(path, os.O_RDONLY); os.fsync(fd); os.close(fd)
        dt = time.time() - t0
        disk = os.path.getsize(path)
        print(f"  {label:10s} {dt:6.2f}s  ->  {human(disk)} su disco  |  "
              f"{disk/1e6/dt:6.1f} MB/s (compressi)")
        os.remove(path)
        return dt

    nfs_path   = os.path.join(DIAG_ROOT, "nfs_probe.parquet")
    local_path = os.path.join("/tmp", "diag_local_probe.parquet")
    try:
        t_nfs   = _bench(nfs_path,   "NFS")
        t_local = _bench(local_path, "locale/tmp")
        ratio = t_nfs / max(t_local, 1e-6)
        print(f"\n  [DIAGNOSI D] NFS e' {ratio:.1f}x {'PIU LENTO' if ratio>1.5 else 'circa uguale'} del locale.")
        if ratio > 1.5:
            print(f"              La scrittura su NFS e' il collo di bottiglia: mentre un worker scrive,")
            print(f"              lo scheduler produce altre partizioni che restano in RAM in attesa")
            print(f"              (backpressure). Con partizioni grandi (fasi A/C) = OOM. Conferma la tua ipotesi.")
    except Exception as e:
        print(f"  !! test D fallito: {type(e).__name__}: {e}")


# ------------------------------------------------------------- helper memoria worker
def _rss_per_worker(client):
    """RSS di processo per worker (via psutil sul worker: e' l'RSS 'vero', quello che il
    nanny confronta col memory_limit)."""
    def _rss():
        import os as _os, psutil
        return psutil.Process(_os.getpid()).memory_info().rss
    try:
        return client.run(_rss)
    except Exception as e:
        print("  psutil non disponibile sui worker:", e); return {}


def _managed_per_worker(client):
    """(managed, spilled) per worker dai metrics dello scheduler. 'managed' = dati Dask che il
    worker SA di tenere; RSS - managed - spilled = UNMANAGED (frammentazione, pool arrow, leak)."""
    out = {}
    try:
        winfo = client.scheduler_info().get("workers", {})
    except Exception:
        winfo = {}
    for addr, w in winfo.items():
        m = (w.get("metrics") or {})
        managed = m.get("managed_bytes", m.get("managed", 0)) or 0
        sp = m.get("spilled_bytes", 0)
        if isinstance(sp, dict):
            sp = (sp.get("disk", 0) or 0) + (sp.get("memory", 0) or 0)
        out[addr] = (managed, sp or 0)
    return out


def _trim_workers(client):
    """malloc_trim(0) + release del pool pyarrow su OGNI worker; ritorna l'RSS DOPO. Se qui l'RSS
    CROLLA, la memoria 'persa' era UNMANAGED (frammentazione glibc / pool arrow non restituito):
    la cura e' MALLOC_TRIM_THRESHOLD_=0 (+ eventuale release) nell'ambiente dei worker."""
    def _trim():
        import os as _os, ctypes, ctypes.util, psutil
        try:
            libc = ctypes.CDLL(ctypes.util.find_library("c") or "libc.so.6")
            libc.malloc_trim(0)
        except Exception:
            pass
        try:
            import pyarrow as pa
            pa.default_memory_pool().release_unused()
        except Exception:
            pass
        return psutil.Process(_os.getpid()).memory_info().rss
    try:
        return client.run(_trim)
    except Exception as e:
        print("  malloc_trim non disponibile sui worker:", e); return {}


def _fmt_workers(rss, managed):
    """Riga per worker: RSS | managed | unmanaged (= RSS - managed - spilled)."""
    lines = []
    for addr in sorted(rss):
        r = rss[addr]
        mg, sp = managed.get(addr, (0, 0))
        unm = max(r - mg - sp, 0)
        lines.append(f"    {addr[-24:]:>24s}  RSS {human(r)}  managed {human(mg)}  "
                     f"UNMANAGED {human(unm)}  spill {human(sp)}")
    return "\n".join(lines)


# ============================================================ FASE E: smoke + forensics RAM
# Il transform reale del silver, SOLO sulle prime DIAG_N partizioni, su cartella usa-e-getta.
# Oltre a VEDERE il picco salire (MemorySampler + performance_report), scompone la memoria in
# managed/unmanaged e fa il TEST DECISIVO: malloc_trim(0) su ogni worker. Non tocca mai il silver.
def phase_E(client):
    hr(f"FASE E  --  smoke run + forensics memoria (prime {DIAG_N} partizioni, NON completa)")
    import dask.dataframe as dd
    from dask.distributed import performance_report
    from distributed.diagnostics import MemorySampler

    out = os.path.join(DIAG_ROOT, "silver_paragraphs_smoke")
    para_full = dd.read_parquet(BRONZE, engine="pyarrow", split_row_groups=True)
    n = min(DIAG_N, para_full.npartitions)
    para = para_full.partitions[:n]
    print(f"  processo {n}/{para_full.npartitions} partizioni -> {out} (usa-e-getta)")
    print(f"  (piu' alto DIAG_N = piu' partizioni in sequenza = creep unmanaged piu' evidente)")

    # pmc_uids come nel notebook, ma calcolato SOLO sul sottoinsieme smoke (bounded): la memoria
    # del transform e' per-partizione e non dipende dall'esattezza del set.
    t0 = time.time()
    pmc_uids = set(para[para["source"] == "pmc"]["cord_uid"].dropna().unique().compute())
    print(f"  pmc_uids (subset): {len(pmc_uids)} id  |  pickle {human(len(pickle.dumps(pmc_uids)))}  "
          f"[compute {time.time()-t0:.1f}s]")

    def _keep_prefer_pmc(pdf):
        return pdf[(pdf["source"] == "pmc") | (~pdf["cord_uid"].isin(pmc_uids))]

    para = para.map_partitions(_keep_prefer_pmc, meta=para._meta)
    para = para.assign(is_reference_like=para["section"].fillna("").str.lower()
                       .str.contains(REFERENCE_SECTION_RE, regex=True))

    # (1) RSS a riposo, PRIMA della smoke (baseline)
    rss0 = _rss_per_worker(client)
    base = sum(rss0.values())
    print(f"\n  RSS worker a RIPOSO (baseline): {human(base)} totale")

    # (2) smoke sotto MemorySampler + performance_report
    ms = MemorySampler()
    rep = os.path.join(REPORTS, "diag_silver_paragraphs_smoke.html")
    t0 = time.time(); ok = True
    try:
        with performance_report(filename=rep), ms.sample("smoke"):
            para.to_parquet(out, engine="pyarrow", write_index=False, compression="zstd",
                            overwrite=True, schema=PARA_SILVER_SCHEMA)
    except Exception as e:
        ok = False
        print(f"  !! smoke INTERROTTA da {type(e).__name__}: {e}")
    dt = time.time() - t0
    print(f"  smoke {'completata' if ok else 'FALLITA'} in {dt:.1f}s  |  report: {rep}")

    # (3) memoria SUBITO DOPO la scrittura (prima di qualunque trim): qui si vede l'accumulo
    rss1 = _rss_per_worker(client)
    mgd1 = _managed_per_worker(client)
    print(f"\n  RSS worker DOPO la smoke: {human(sum(rss1.values()))} totale "
          f"(+{human(sum(rss1.values())-base)} vs baseline)")
    print(_fmt_workers(rss1, mgd1))

    # (4) TEST DECISIVO: malloc_trim(0) + release pool arrow su ogni worker
    rss2 = _trim_workers(client)
    grew = sum(rss1.values()) - base
    managed_tot = sum(mg for mg, _sp in mgd1.values())
    unmanaged_tot = max(sum(rss1.values()) - managed_tot - sum(sp for _m, sp in mgd1.values()), 0)
    if rss2:
        freed = sum(rss1.get(a, 0) - rss2.get(a, 0) for a in rss2)
        after = sum(rss2.values())
        freed_s = ("+" + human(-freed)) if freed < 0 else human(freed)
        print(f"\n  dopo malloc_trim(0)+arrow.release: {human(after)} totale  (liberati {freed_s})")
        f_managed = managed_tot / grew if grew > 0 else 0
        f_trim = freed / grew if grew > 0 else 0
        # TRE casi distinti (non due): il discriminante non e' solo se trim libera, ma DOVE sta la RAM.
        if grew <= 0:
            print(f"  [DIAGNOSI E] nessuna crescita apprezzabile su {n} partizioni: alza DIAG_N per stressare.")
        elif f_managed > 0.4:
            print(f"  [DIAGNOSI E] la crescita e' MANAGED ({100*f_managed:.0f}%): sono dati Dask di")
            print(f"              partizioni FINITE ma non ancora scritte, tenuti insieme in RAM ->")
            print(f"              backpressure/overproduction. Sink NFS lento (fase D) la amplifica.")
            print(f"              CURA: riduci la concorrenza (meno thread/worker), worker-saturation=1.0,")
            print(f"              o scrivi su scratch LOCALE e sposta a fine step.")
        elif f_trim > 0.4:
            print(f"  [DIAGNOSI E] la crescita e' UNMANAGED e malloc_trim l'ha restituita ({100*f_trim:.0f}%):")
            print(f"              e' frammentazione dell'allocatore glibc (tipico con milioni di stringhe).")
            print(f"              CURA sui worker: MALLOC_TRIM_THRESHOLD_=0  MALLOC_ARENA_MAX=2 nell'env,")
            print(f"              es. worker_options={{'env':{{'MALLOC_TRIM_THRESHOLD_':'0','MALLOC_ARENA_MAX':'2'}}}}.")
        else:
            print(f"  [DIAGNOSI E] la crescita e' UNMANAGED ma malloc_trim NON l'ha restituita")
            print(f"              (managed {100*f_managed:.0f}%, trim {100*f_trim:.0f}%): non e' frammentazione")
            print(f"              ne' dati Dask -> working-set vivo (partizioni in lavorazione, buffer")
            print(f"              arrow/lettura NFS, comm). E' concorrenza x taglia partizione:")
            print(f"              riduci thread/worker e/o partizioni in volo. Guarda la timeline e il task-stream.")
        print(f"  (ripartizione RAM dopo la smoke: managed {human(managed_tot)} | unmanaged {human(unmanaged_tot)})")

    # (5) picco dal MemorySampler + scan dei log worker (Paused/Restarting/spill = pressione RAM)
    try:
        s = ms.to_pandas()
        print(f"\n  picco RAM nel campione (somma worker): {human(s.sum(axis=1).max())}")
        ms.to_pandas().to_csv(os.path.join(REPORTS, "diag_memory_timeline.csv"))
    except Exception as e:
        print(f"  MemorySampler senza campioni: {e}")
    try:
        logs = client.get_worker_logs()
        keys = ("Restarting", "Paused", "paused", "spill", "Spill", "Worker is at",
                "unmanaged", "Nanny", "memory")
        hits = {}
        for entries in logs.values():
            for _lvl, m in entries:
                for kkey in keys:
                    if kkey in m:
                        hits[kkey] = hits.get(kkey, 0) + 1
        logpath = os.path.join(REPORTS, "diag_worker_logs.txt")
        with open(logpath, "w") as fh:
            for w, entries in logs.items():
                fh.write(f"\n===== {w} =====\n")
                for lvl, m in entries:
                    fh.write(f"{lvl}\t{m}\n")
        print(f"  eventi nei log worker: {hits or 'nessuno'}  ->  {logpath}")
        if hits.get("Paused") or hits.get("paused") or hits.get("Restarting"):
            print(f"  NB: worker in pausa/riavvio gia' su {n} partizioni -> sul full run crolla.")
    except Exception as e:
        print(f"  get_worker_logs fallito: {e}")


# ============================================================ FASE F: set pmc_uids nel graph
# Verifica se il set (~105k id) viene INLINATO nel graph per-task (cattura nella closure) invece
# di essere scatterato una volta. Se il graph 'inline' e' molto piu' grande dello 'scatter', il
# set moltiplica per numero di partizioni -> pressione su scheduler e worker. Cura: client.scatter.
def phase_F(client):
    hr("FASE F  --  il set pmc_uids nel task graph: inline (closure) vs scatter")
    import dask.dataframe as dd
    import cloudpickle
    para = dd.read_parquet(BRONZE, engine="pyarrow", split_row_groups=True)
    n = min(DIAG_N, para.npartitions)
    sub = para.partitions[:n]
    fake = {f"id{i:07d}" for i in range(105000)}          # taglia realistica del set reale
    print(f"  set realistico: {len(fake)} id  |  pickle {human(len(pickle.dumps(fake)))}  su {n} partizioni")

    def keep(pdf, s):
        return pdf[(pdf["source"] == "pmc") | (~pdf["cord_uid"].isin(s))]

    def _graph_bytes(coll):
        try:
            return len(cloudpickle.dumps(dict(coll.__dask_graph__())))
        except Exception as e:
            return f"errore ({type(e).__name__})"

    g_inline = sub.map_partitions(keep, fake, meta=sub._meta)          # set catturato nel graph
    sz_inline = _graph_bytes(g_inline)
    fut = client.scatter(fake, broadcast=True)                        # set scatterato una volta
    g_sc = sub.map_partitions(keep, fut, meta=sub._meta)
    sz_sc = _graph_bytes(g_sc)
    print(f"  graph con set INLINE : {human(sz_inline) if isinstance(sz_inline,int) else sz_inline}")
    print(f"  graph con set SCATTER: {human(sz_sc) if isinstance(sz_sc,int) else sz_sc}")
    if isinstance(sz_inline, int) and isinstance(sz_sc, int):
        if sz_inline > sz_sc * 2:
            print(f"  [DIAGNOSI F] il set e' inlinato piu' volte (~{sz_inline/max(sz_sc,1):.0f}x): "
                  f"usa client.scatter(pmc_uids, broadcast=True) nel silver_paragraphs.")
        else:
            print(f"  [DIAGNOSI F] il set NON e' duplicato in modo significativo: non e' questa la causa.")
    try:
        client.cancel(fut)
    except Exception:
        pass


# ================================================================================ main
def build_client():
    """Stessa logica del notebook: DASK_SCHEDULER -> Client; CORD19_HOSTS -> SSHCluster;
    altrimenti LocalCluster. Ritorna (client, cluster) o (None, None) se non servono worker."""
    from dask.distributed import Client, LocalCluster
    if DASK_SCHEDULER:
        print(f"cluster: Client({DASK_SCHEDULER})")
        return Client(DASK_SCHEDULER), None
    if CORD19_HOSTS:
        from dask.distributed import SSHCluster
        hosts = [h.strip() for h in CORD19_HOSTS.split(",") if h.strip()]
        connect = {"known_hosts": None}
        if CORD19_SSH_KEY:
            connect["client_keys"] = [CORD19_SSH_KEY]
        print(f"cluster: SSHCluster({hosts}) mem={WORKER_MEM} thr={THREADS_PER_WORKER}")
        cl = SSHCluster(hosts, connect_options=connect,
                        worker_options={"nthreads": THREADS_PER_WORKER, "memory_limit": WORKER_MEM},
                        scheduler_options={"port": 8786, "dashboard_address": ":8787"})
        return Client(cl), cl
    print(f"cluster: LocalCluster(n_workers={N_WORKERS}, mem={WORKER_MEM}, thr={THREADS_PER_WORKER})")
    cl = LocalCluster(n_workers=N_WORKERS, threads_per_worker=THREADS_PER_WORKER,
                      memory_limit=WORKER_MEM, processes=True)
    return Client(cl), cl


def main():
    print(f"REPO   : {REPO}")
    print(f"BRONZE : {BRONZE}")
    print(f"DIAG   : {DIAG_ROOT}  (output usa-e-getta)")
    print(f"phases : {PHASES}   DIAG_N={DIAG_N}  RG_FILES={RG_FILES}  NFS_MB={NFS_MB}")
    if not os.path.isdir(BRONZE):
        sys.exit(f"!! {BRONZE} non esiste. Imposta CORD19_DATA alla root che contiene bronze/paragraphs.")

    # A/B/C/D non hanno bisogno del cluster (solo driver + accesso file). E si'.
    if "A" in PHASES:
        phase_A()
    if "B" in PHASES:
        phase_B()
    if "C" in PHASES:
        phase_C()
    if "D" in PHASES:
        phase_D()

    if "E" in PHASES or "F" in PHASES:
        client, cluster = build_client()
        try:
            print("dashboard:", getattr(client, "dashboard_link", "n/d"))
            if "F" in PHASES:
                phase_F(client)
            if "E" in PHASES:
                phase_E(client)
        finally:
            client.close()
            if cluster is not None:
                cluster.close()
            print("\ncluster chiuso.")

    hr("FINE DIAGNOSTICA")
    print("Report salvati in reports/: diag_silver_paragraphs_smoke.html, diag_worker_logs.txt")
    print("Ricorda: la cartella data/_diag/ e' usa-e-getta, puoi cancellarla.")


if __name__ == "__main__":
    main()
