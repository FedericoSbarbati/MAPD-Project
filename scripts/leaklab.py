#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
leaklab.py — riproduzione LOCALE del creep di RAM di silver/paragraphs + A/B delle cure.

CONTESTO (docs/PROJECT_CONTEXT.md §7, Atto 3): sul cluster Cloud Veneto un worker
long-lived che macina migliaia di partizioni testuali accumula RSS *unmanaged*
(managed≈0) in modo cumulativo, fino all'OOM. La cura adottata nel notebook e'
blocchi + client.restart(). QUESTO script serve a trovare una cura che NON richieda
il restart, per la fase benchmark dei task (i colleghi ripetono computazioni pesanti
in loop e un restart tra le misure e' lento e sporca il cluster).

Cosa fa: rifa' il transform silver/paragraphs VERO (prefer-pmc isin + regex
is_reference_like + to_parquet zstd) sul bronze REALE scaricato dalla VM
(data/bronze/paragraphs, 23.1M righe, 1979 row-group), processando N partizioni a
chunk; a ogni chunk campiona RSS/managed/unmanaged per worker -> curva
"memoria vs partizioni processate". A fine run tre probe in sequenza
(gc -> release pool arrow -> trim allocatore nativo) per scomporre DOVE sta la
memoria trattenuta. L'output parquet e' usa-e-getta (data/_diag/leaklab, cancellato).

VARIANTI (--variants, lista separata da virgola; "a+b" combina due cure):
  base          nessuna cura: la curva del leak cosi' com'e'
  sweep         a fine chunk il DRIVER fa client.run(sweep): gc + release pool arrow
                + trim nativo. E' il pattern "igiene tra un benchmark e l'altro".
  hygiene       WorkerPlugin periodico (ogni --every s) che fa lo stesso sweep
                DURANTE il run, senza cooperazione del driver.
  arrow-system  ARROW_DEFAULT_MEMORY_POOL=system nell'env dei worker (pre-spawn):
                i buffer Arrow passano dall'allocatore di sistema -> un solo heap
                da trimmare (invece di mimalloc/jemalloc che tengono pagine per se').
  mimalloc-purge MIMALLOC_PURGE_DELAY=0 nell'env dei worker: mimalloc (pool arrow
                di default qui) restituisce subito all'OS le pagine liberate.
  pymalloc-off  PYTHONMALLOC=malloc nell'env dei worker: oggetti Python
                nell'allocatore nativo (niente arene pymalloc frammentate).
  fast-isin     cura ALLA FONTE: prefer-pmc con pc.is_in su pa.Array precostruito
                (niente riconversione del set 315k per ogni partizione).
  lifetime      worker con lifetime="--lifetime"s + stagger: il nanny RICICLA i
                worker da solo, senza client.restart() (fallback estremo).
  trimenv       (solo glibc/VM, no-op su macOS) pre-spawn-environ con
                MALLOC_TRIM_THRESHOLD_=0 + MALLOC_ARENA_MAX=2.

Esempi:
  # curva baseline, 768 partizioni
  python scripts/leaklab.py --variants base --n 768
  # campagna A/B completa
  python scripts/leaklab.py --variants base,sweep,hygiene,arrow-system,arrow-system+sweep
  # sulla VM (glibc): aggiungere trimenv e i combo
  python scripts/leaklab.py --variants base,trimenv,trimenv+sweep

Output: reports/leaklab/  (CSV campioni + chunk, PNG di confronto). Gitignored.
"""
import argparse
import glob
import json
import logging
import os
import shutil
import sys
import threading
import time

import pyarrow as pa

# ------------------------------------------------------------------ path & costanti
REPO      = os.getcwd()
DATA_ROOT = os.environ.get("CORD19_DATA", os.path.join(REPO, "data"))
BRONZE    = os.path.join(DATA_ROOT, "bronze", "paragraphs")
OUT_ROOT  = os.path.join(DATA_ROOT, "_diag", "leaklab")     # output parquet usa-e-getta
REP_DIR   = os.path.join(REPO, "reports", "leaklab")        # csv/png dei risultati

# transform copiato VERBATIM dal notebook (conversion_sanification.ipynb)
REFERENCE_SECTION_RE = (r"(?:referen|bibliograph|acknowledg|author contrib|"
                        r"conflict|competing interest|funding|declarat|"
                        r"supplement|copyright)")
PARA_SILVER_SCHEMA = pa.schema([("cord_uid", pa.string()), ("paper_id", pa.string()),
                                ("source", pa.string()), ("para_idx", pa.int32()),
                                ("section", pa.string()), ("text", pa.string()),
                                ("is_reference_like", pa.bool_())])


def human(n):
    for u in ("B", "KB", "MB", "GB", "TB"):
        if abs(n) < 1024 or u == "TB":
            return f"{n:7.1f}{u}"
        n /= 1024


# ------------------------------------------------------------------ sweep sui worker
# Unica funzione di "igiene" usata sia dal WorkerPlugin (hygiene) sia da client.run
# (sweep): restituisce all'OS quello che si puo' restituire SENZA riavviare niente.
#  1) gc.collect()                       -> cicli Python morti
#  2) pa.default_memory_pool().release_unused() -> pagine dirty tenute da
#     mimalloc/jemalloc per conto di Arrow (i buffer delle stringhe arrow-backed!)
#  3) trim dell'allocatore NATIVO: glibc malloc_trim(0) su Linux,
#     malloc_zone_pressure_relief(NULL, 0) su macOS.
def sweep_worker():
    import ctypes
    import ctypes.util
    import gc
    import platform

    import psutil

    out = {"freed_relief": 0, "arrow_alloc": -1}
    gc.collect()
    try:
        pool = pa.default_memory_pool()
        out["arrow_alloc"] = pool.bytes_allocated()
        pool.release_unused()
    except Exception as e:                                    # pragma: no cover
        out["arrow_err"] = repr(e)
    try:
        if platform.system() == "Linux":
            libc = ctypes.CDLL(ctypes.util.find_library("c") or "libc.so.6")
            libc.malloc_trim(0)
        elif platform.system() == "Darwin":
            lib = ctypes.CDLL("/usr/lib/libSystem.B.dylib")
            fn = lib.malloc_zone_pressure_relief
            fn.argtypes = [ctypes.c_void_p, ctypes.c_size_t]
            fn.restype = ctypes.c_size_t
            out["freed_relief"] = int(fn(None, 0))            # NULL = tutte le zone
    except Exception as e:                                    # pragma: no cover
        out["trim_err"] = repr(e)
    out["rss"] = psutil.Process(os.getpid()).memory_info().rss
    return out


# probe SEPARATE per la scomposizione post-run (una alla volta, in ordine)
def probe_gc():
    import gc

    import psutil
    gc.collect()
    return psutil.Process(os.getpid()).memory_info().rss


def probe_arrow():
    import psutil
    try:
        pa.default_memory_pool().release_unused()
    except Exception:
        pass
    return psutil.Process(os.getpid()).memory_info().rss


def probe_trim():
    import ctypes
    import ctypes.util
    import platform

    import psutil
    try:
        if platform.system() == "Linux":
            libc = ctypes.CDLL(ctypes.util.find_library("c") or "libc.so.6")
            libc.malloc_trim(0)
        elif platform.system() == "Darwin":
            lib = ctypes.CDLL("/usr/lib/libSystem.B.dylib")
            fn = lib.malloc_zone_pressure_relief
            fn.argtypes = [ctypes.c_void_p, ctypes.c_size_t]
            fn.restype = ctypes.c_size_t
            fn(None, 0)
    except Exception:
        pass
    return psutil.Process(os.getpid()).memory_info().rss


def state_probe():
    """Scomposizione della RAM del worker: dove stanno i byte?
      rss         RSS del processo (quello che il nanny confronta col limit)
      py_blocks   blocchi vivi di pymalloc (sys.getallocatedblocks): se cresce,
                  ci sono OGGETTI Python vivi in piu' (vero leak Python)
      arrow_alloc byte VIVI nel memory pool Arrow (buffer di stringhe arrow ecc.)
      arrow_max   high-water mark del pool Arrow
      heap_use    byte VIVI nell'allocatore nativo (malloc zones / glibc heap)
      heap_alloc  byte che l'allocatore nativo TIENE dall'OS (use+free trattenuta)
    RSS - heap_alloc - (memoria del pool arrow se mimalloc/jemalloc, che mmappa
    per conto suo e NON compare nelle zone) = resto (codice, stack, page cache)."""
    import ctypes
    import ctypes.util
    import platform
    import sys as _sys

    import psutil

    out = {"rss": psutil.Process(os.getpid()).memory_info().rss,
           "py_blocks": _sys.getallocatedblocks(),
           "arrow_alloc": -1, "arrow_max": -1, "heap_use": -1, "heap_alloc": -1}
    try:
        pool = pa.default_memory_pool()
        out["arrow_alloc"] = pool.bytes_allocated()
        out["arrow_max"] = pool.max_memory()
    except Exception:
        pass
    try:
        if platform.system() == "Darwin":
            class _MS(ctypes.Structure):
                _fields_ = [("blocks_in_use", ctypes.c_uint),
                            ("size_in_use", ctypes.c_size_t),
                            ("max_size_in_use", ctypes.c_size_t),
                            ("size_allocated", ctypes.c_size_t)]
            lib = ctypes.CDLL("/usr/lib/libSystem.B.dylib")
            lib.malloc_zone_statistics.argtypes = [ctypes.c_void_p,
                                                   ctypes.POINTER(_MS)]
            st = _MS()
            lib.malloc_zone_statistics(None, ctypes.byref(st))  # NULL = tutte le zone
            out["heap_use"] = int(st.size_in_use)
            out["heap_alloc"] = int(st.size_allocated)
        elif platform.system() == "Linux":
            class _MI(ctypes.Structure):
                _fields_ = [(n, ctypes.c_size_t) for n in
                            ("arena", "ordblks", "smblks", "hblks", "hblkhd",
                             "usmblks", "fsmblks", "uordblks", "fordblks",
                             "keepcost")]
            libc = ctypes.CDLL(ctypes.util.find_library("c") or "libc.so.6")
            libc.mallinfo2.restype = _MI
            mi = libc.mallinfo2()
            out["heap_use"] = int(mi.uordblks)
            out["heap_alloc"] = int(mi.arena + mi.hblkhd)
    except Exception:
        pass
    return out


def worker_env_info():
    """Preflight: com'e' fatto DAVVERO l'ambiente dentro il worker."""
    import platform

    import pandas as pd
    return {
        "pool": pa.default_memory_pool().backend_name,
        "arrow_env": os.environ.get("ARROW_DEFAULT_MEMORY_POOL", ""),
        "malloc_trim_thr": os.environ.get("MALLOC_TRIM_THRESHOLD_", ""),
        "pandas": pd.__version__, "system": platform.system(),
    }


# ------------------------------------------------------------------ plugin periodico
from distributed.diagnostics.plugin import WorkerPlugin


class HygienePlugin(WorkerPlugin):
    """sweep_worker() ogni every_s secondi sul loop del worker (come il trim-plugin
    della diagnostica VM, ma con in piu' il release del pool Arrow — sulla VM il
    plugin solo-malloc_trim NON bastava: l'ipotesi e' che mancasse il pool Arrow)."""
    name = "leaklab-hygiene"

    def __init__(self, every_s):
        self.every_s = every_s

    def setup(self, worker):
        from tornado.ioloop import PeriodicCallback
        pc = PeriodicCallback(sweep_worker, self.every_s * 1000)
        worker._leaklab_pc = pc
        worker.loop.add_callback(pc.start)

    def teardown(self, worker):
        try:
            worker._leaklab_pc.stop()
        except Exception:
            pass


# ------------------------------------------------------------------ campionatore RAM
class Sampler(threading.Thread):
    """Ogni ~1.5s legge scheduler_info() (dati di heartbeat: RSS, managed, spilled
    per worker — nessun client.run, zero carico sui worker) e accoda righe CSV."""

    def __init__(self, client, state, rows, every=1.5):
        super().__init__(daemon=True)
        self.client, self.state, self.rows, self.every = client, state, rows, every
        self.stop_ev = threading.Event()

    def run(self):
        while not self.stop_ev.is_set():
            try:
                info = self.client.scheduler_info()
                now = time.time()
                for addr, w in (info.get("workers") or {}).items():
                    m = w.get("metrics") or {}
                    rss = m.get("memory", 0) or 0
                    managed = m.get("managed_bytes", m.get("managed", 0)) or 0
                    sp = m.get("spilled_bytes", 0)
                    if isinstance(sp, dict):
                        sp = (sp.get("memory", 0) or 0) + (sp.get("disk", 0) or 0)
                    self.rows.append(dict(
                        t=round(now - self.state["t0"], 1),
                        phase=self.state["phase"], parts=self.state["parts"],
                        worker=addr, rss=rss, managed=managed, spilled=sp or 0,
                        unmanaged=max(rss - managed - (sp or 0), 0)))
            except Exception:
                pass                                          # cluster in teardown
            self.stop_ev.wait(self.every)


def totals(client):
    """(rss_tot, managed_tot, unmanaged_tot, rss_max_worker) dai metrics correnti."""
    info = client.scheduler_info()
    rss = mgd = spl = mx = 0
    for w in (info.get("workers") or {}).values():
        m = w.get("metrics") or {}
        r = m.get("memory", 0) or 0
        g = m.get("managed_bytes", m.get("managed", 0)) or 0
        s = m.get("spilled_bytes", 0)
        if isinstance(s, dict):
            s = (s.get("memory", 0) or 0) + (s.get("disk", 0) or 0)
        rss += r; mgd += g; spl += s or 0; mx = max(mx, r)
    return rss, mgd, max(rss - mgd - spl, 0), mx


# ------------------------------------------------------------------ pmc_uids (driver)
def build_pmc_uids():
    """Set GLOBALE dei cord_uid con parse pmc, come nel notebook, ma costruito nel
    processo driver in streaming (pyarrow.dataset, 2 colonne): i worker non vengono
    gonfiati dal compute del set e ogni variante parte dallo stesso identico stato."""
    import pyarrow.compute as pc
    import pyarrow.dataset as ds
    d = ds.dataset(BRONZE, format="parquet")
    s = set()
    scan = d.scanner(columns=["cord_uid", "source"],
                     filter=ds.field("source") == "pmc", batch_size=1 << 17)
    for b in scan.to_batches():
        s.update(pc.unique(b.column("cord_uid")).to_pylist())
    return s


# ------------------------------------------------------------------ varianti
def variant_config(name, args):
    """Config cumulativa per 'a+b+c'. Ritorna dict con env/dask_config/worker_kwargs/
    plugin_every/sweep_each_chunk."""
    cfg = dict(env={}, dask_config={}, worker_kwargs={}, plugin_every=None,
               sweep_each_chunk=False, fast_isin=False)
    for part in name.split("+"):
        if part == "base":
            pass
        elif part == "sweep":
            cfg["sweep_each_chunk"] = True
        elif part == "hygiene":
            cfg["plugin_every"] = args.every
        elif part == "arrow-system":
            cfg["env"]["ARROW_DEFAULT_MEMORY_POOL"] = "system"
        elif part == "mimalloc-purge":
            # mimalloc (pool arrow di default su macOS/conda) legge MIMALLOC_* all'init:
            # purge_delay=0 = restituisce all'OS le pagine libere SUBITO invece di tenerle
            cfg["env"]["MIMALLOC_PURGE_DELAY"] = "0"
        elif part == "pymalloc-off":
            # PYTHONMALLOC=malloc: niente arene pymalloc (mmap da 1MB che la
            # frammentazione tiene vive e che nessun trim tocca) -> gli oggetti
            # Python passano dall'allocatore nativo, che si puo' trimmare
            cfg["env"]["PYTHONMALLOC"] = "malloc"
        elif part == "fast-isin":
            # cura ALLA FONTE (micro-probe: isin(set) riconverte 315k stringhe
            # Python in Arrow A OGNI partizione: ~0.7s + churn per task).
            # Il set si converte UNA volta sul driver in pa.Array e il filtro
            # diventa pc.is_in arrow-nativo: zero churn per-partizione.
            cfg["fast_isin"] = True
        elif part == "lifetime":
            cfg["worker_kwargs"].update(
                lifetime=f"{args.lifetime}s",
                lifetime_stagger=f"{max(args.lifetime // 4, 5)}s",
                lifetime_restart=True)
        elif part == "trimenv":
            import dask
            cur = dict(dask.config.get("distributed.nanny.pre-spawn-environ") or {})
            cur.update({"MALLOC_TRIM_THRESHOLD_": 0, "MALLOC_ARENA_MAX": 2})
            cfg["dask_config"]["distributed.nanny.pre-spawn-environ"] = cur
        else:
            sys.exit(f"variante sconosciuta: {part!r}")
    return cfg


def run_variant(name, args, pmc_uids, ts):
    import dask
    import dask.dataframe as dd
    from dask.distributed import Client, LocalCluster

    cfg = variant_config(name, args)
    vdir = os.path.join(OUT_ROOT, name.replace("+", "_"))
    os.makedirs(vdir, exist_ok=True)

    # env PRIMA dello spawn dei worker (multiprocessing 'spawn' eredita os.environ)
    saved_env = {k: os.environ.get(k) for k in cfg["env"]}
    os.environ.update(cfg["env"])

    rows, chunks = [], []
    state = {"t0": time.time(), "phase": "startup", "parts": 0}
    print(f"\n{'='*78}\nVARIANTE {name}  (n={args.n}, chunk={args.chunk}, "
          f"{args.workers}w x {args.threads}t, mem {args.memlimit})\n{'='*78}")

    with dask.config.set(cfg["dask_config"]):
        cluster = LocalCluster(
            n_workers=args.workers, threads_per_worker=args.threads,
            memory_limit=args.memlimit, processes=True,
            dashboard_address=":0", silence_logs=logging.WARNING,
            **cfg["worker_kwargs"])
        client = Client(cluster)
        try:
            envs = client.run(worker_env_info)
            one = next(iter(envs.values()))
            print(f"  worker: pool arrow={one['pool']}"
                  f"{' (env='+one['arrow_env']+')' if one['arrow_env'] else ''}"
                  f"  pandas={one['pandas']}  MALLOC_TRIM_THRESHOLD_="
                  f"{one['malloc_trim_thr'] or 'n/d'}")
            if cfg["plugin_every"]:
                client.register_plugin(HygienePlugin(cfg["plugin_every"]))
                print(f"  hygiene-plugin ON (sweep ogni {cfg['plugin_every']}s)")

            sampler = Sampler(client, state, rows)
            sampler.start()

            # baseline a riposo
            time.sleep(2.5)
            b_rss, _, b_unm, _ = totals(client)
            b_state = client.run(state_probe)
            b_blocks = sum(v["py_blocks"] for v in b_state.values())
            print(f"  baseline: RSS {human(b_rss)}  unmanaged {human(b_unm)}  "
                  f"pyblocks {b_blocks/1e6:.1f}M")

            para = dd.read_parquet(BRONZE, engine="pyarrow", split_row_groups=True)
            n = min(args.n, para.npartitions)

            if cfg["fast_isin"]:
                # set -> pa.Array UNA volta (driver); nel worker solo kernel arrow
                pmc_arr = pa.array(sorted(pmc_uids), type=pa.string())

                def _keep_prefer_pmc(pdf):
                    import pyarrow.compute as pc
                    cu = pa.array(pdf["cord_uid"], from_pandas=True)
                    in_pmc = pc.is_in(cu, value_set=pmc_arr)\
                        .to_numpy(zero_copy_only=False)
                    keep = (pdf["source"].to_numpy() == "pmc") | ~in_pmc
                    return pdf[keep]
            else:
                def _keep_prefer_pmc(pdf):
                    return pdf[(pdf["source"] == "pmc") |
                               (~pdf["cord_uid"].isin(pmc_uids))]

            state["phase"] = "run"
            t_run = time.time()
            for c0 in range(0, n, args.chunk):
                c1 = min(c0 + args.chunk, n)
                sub = para.partitions[c0:c1]
                sub = sub.map_partitions(_keep_prefer_pmc, meta=para._meta)
                sub = sub.assign(
                    is_reference_like=sub["section"].fillna("").str.lower()
                    .str.contains(REFERENCE_SECTION_RE, regex=True))
                sub.to_parquet(os.path.join(vdir, f"chunk_{c0:05d}"),
                               engine="pyarrow", write_index=False,
                               compression="zstd", schema=PARA_SILVER_SCHEMA,
                               write_metadata_file=False)
                if cfg["sweep_each_chunk"]:
                    client.run(sweep_worker)
                state["parts"] = c1
                rss, mgd, unm, mxw = totals(client)
                st = client.run(state_probe)                  # scomposizione heap
                agg = {k: sum(v[k] for v in st.values())
                       for k in ("py_blocks", "arrow_alloc", "arrow_max",
                                 "heap_use", "heap_alloc")}
                chunks.append(dict(variant=name, parts=c1,
                                   t=round(time.time() - t_run, 1), rss=rss,
                                   managed=mgd, unmanaged=unm, rss_max_worker=mxw,
                                   **agg))
                print(f"    part {c1:4d}/{n}  RSS {human(rss)}  unmanaged "
                      f"{human(unm)}  maxW {human(mxw)}  | heap use/held "
                      f"{human(agg['heap_use'])}/{human(agg['heap_alloc'])}"
                      f"  arrow {human(max(agg['arrow_alloc'], 0))}"
                      f"  pyblk {agg['py_blocks']/1e6:.1f}M"
                      f"  [{time.time()-t_run:5.0f}s]")
            dt_run = time.time() - t_run

            # ---- probe di scomposizione: gc -> arrow release -> trim nativo
            state["phase"] = "probe"
            time.sleep(1.5)
            r_before = sum(client.run(
                lambda: __import__("psutil").Process(os.getpid())
                .memory_info().rss).values())
            deltas = {}
            for label, fn in (("gc", probe_gc), ("arrow_release", probe_arrow),
                              ("native_trim", probe_trim)):
                after = sum(client.run(fn).values())
                deltas[label] = r_before - after
                r_before = after
            state["phase"] = "done"

            summary = dict(
                variant=name, n=n, secs=round(dt_run, 1),
                parts_per_s=round(n / dt_run, 1),
                baseline_rss=b_rss,
                final_rss=chunks[-1]["rss"], final_unmanaged=chunks[-1]["unmanaged"],
                growth=chunks[-1]["rss"] - b_rss,
                growth_per_256=(chunks[-1]["rss"] - b_rss) / n * 256,
                rss_max_worker=max(c["rss_max_worker"] for c in chunks),
                probe_gc=deltas["gc"], probe_arrow=deltas["arrow_release"],
                probe_trim=deltas["native_trim"],
                rss_after_probes=after)
            print(f"  run: {dt_run:.0f}s ({summary['parts_per_s']}/s)  "
                  f"crescita {human(summary['growth'])} "
                  f"(~{human(summary['growth_per_256'])}/256part)")
            print(f"  probe: gc {human(deltas['gc'])} | arrow "
                  f"{human(deltas['arrow_release'])} | trim "
                  f"{human(deltas['native_trim'])}  ->  RSS finale {human(after)} "
                  f"(baseline {human(b_rss)})")

            # eventi di pressione nei log worker (pause/restart sporcano la misura)
            try:
                logs = client.get_worker_logs()
                ev = {}
                for entries in logs.values():
                    for _l, msg in entries:
                        for k in ("Restarting", "Paused", "paused", "spill"):
                            if k in msg:
                                ev[k] = ev.get(k, 0) + 1
                if ev:
                    print(f"  ATTENZIONE eventi memoria nei log: {ev}")
                summary["events"] = ev
            except Exception:
                pass

            sampler.stop_ev.set()
            sampler.join(timeout=5)
        finally:
            try:
                client.close(timeout=10)
            except Exception:
                pass
            try:
                cluster.close(timeout=10)
            except Exception:
                pass
            for k, v in saved_env.items():                    # ripristina env parent
                if v is None:
                    os.environ.pop(k, None)
                else:
                    os.environ[k] = v
            shutil.rmtree(vdir, ignore_errors=True)           # output usa-e-getta

    # dump CSV
    import csv
    sp = os.path.join(REP_DIR, f"samples_{name.replace('+','_')}_{ts}.csv")
    with open(sp, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
        w.writeheader(); w.writerows(rows)
    cp = os.path.join(REP_DIR, f"chunks_{name.replace('+','_')}_{ts}.csv")
    with open(cp, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(chunks[0].keys()))
        w.writeheader(); w.writerows(chunks)
    print(f"  csv: {sp}\n       {cp}")
    return summary, chunks


# ------------------------------------------------------------------ main
def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--variants", default="base",
                    help="lista ,-separata; '+' combina (es. arrow-system+sweep)")
    ap.add_argument("--n", type=int, default=768, help="partizioni da processare")
    ap.add_argument("--chunk", type=int, default=64, help="partizioni per chunk")
    ap.add_argument("--workers", type=int,
                    default=int(os.environ.get("CORD19_WORKERS", "4")))
    ap.add_argument("--threads", type=int,
                    default=int(os.environ.get("CORD19_THREADS_PER_WORKER", "4")))
    ap.add_argument("--memlimit",
                    default=os.environ.get("CORD19_WORKER_MEMORY_LIMIT", "4GB"))
    ap.add_argument("--every", type=float, default=2.0,
                    help="cadenza sweep del plugin hygiene (s)")
    ap.add_argument("--lifetime", type=int, default=90,
                    help="lifetime worker (s) per la variante lifetime")
    args = ap.parse_args()

    if not os.path.isdir(BRONZE):
        sys.exit(f"!! {BRONZE} non esiste (CORD19_DATA?)")
    os.makedirs(REP_DIR, exist_ok=True)
    os.makedirs(OUT_ROOT, exist_ok=True)
    ts = time.strftime("%Y%m%d_%H%M%S")

    print(f"bronze : {BRONZE}")
    t0 = time.time()
    pmc_uids = build_pmc_uids()
    print(f"pmc_uids globali: {len(pmc_uids):,}  [{time.time()-t0:.0f}s, driver-side]")

    summaries, all_chunks = [], []
    for name in [v.strip() for v in args.variants.split(",") if v.strip()]:
        s, ch = run_variant(name, args, pmc_uids, ts)
        summaries.append(s); all_chunks.extend(ch)

    # ---------------- riepilogo comparativo
    print(f"\n{'='*78}\nRIEPILOGO ({args.n} partizioni, {args.workers}w x "
          f"{args.threads}t, mem {args.memlimit})\n{'='*78}")
    hdr = (f"{'variante':<24s} {'part/s':>7s} {'crescita':>10s} {'per256':>10s} "
           f"{'maxW':>10s} {'dopo probe':>11s}")
    print(hdr); print("-" * len(hdr))
    for s in summaries:
        print(f"{s['variant']:<24s} {s['parts_per_s']:>7.1f} "
              f"{human(s['growth']):>10s} {human(s['growth_per_256']):>10s} "
              f"{human(s['rss_max_worker']):>10s} {human(s['rss_after_probes']):>11s}")
    with open(os.path.join(REP_DIR, f"summary_{ts}.json"), "w") as fh:
        json.dump(summaries, fh, indent=2)

    # ---------------- plot comparativo (unmanaged totale vs partizioni)
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        fig, ax = plt.subplots(figsize=(9, 5))
        for s in summaries:
            pts = [c for c in all_chunks if c["variant"] == s["variant"]]
            ax.plot([c["parts"] for c in pts],
                    [c["unmanaged"] / 2**30 for c in pts],
                    marker="o", ms=3, label=s["variant"])
        ax.set_xlabel("partizioni processate")
        ax.set_ylabel("RAM unmanaged totale (GiB)")
        ax.set_title(f"leaklab · silver/paragraphs · {args.workers}w x "
                     f"{args.threads}t · mem {args.memlimit}")
        ax.legend(); ax.grid(alpha=.3)
        png = os.path.join(REP_DIR, f"leaklab_{ts}.png")
        fig.tight_layout(); fig.savefig(png, dpi=120)
        print(f"\nplot: {png}")
    except Exception as e:
        print(f"plot saltato: {e}")


if __name__ == "__main__":
    main()
