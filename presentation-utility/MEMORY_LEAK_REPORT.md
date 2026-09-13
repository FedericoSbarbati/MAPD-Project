# Il "memory leak" di silver/paragraphs — riproduzione locale, causa vera e cura

> **Atto 4 del diario OOM** (gli Atti 1–3 sono in `docs/PROJECT_CONTEXT.md` §7).
> Sessione di analisi del 2026-07-11, eseguita in locale (Mac) sul **bronze reale
> scaricato dalla VM**. Obiettivo: riprodurre il creep di RAM osservato su Cloud
> Veneto, capirlo a fondo e trovare una strategia che permetta i **benchmark dei
> 4 task senza `client.restart()`** tra le misure.

**TL;DR.** Il creep si riproduce in locale ed è stato root-causato: **non è un leak
e non è (solo) frammentazione dell'allocatore — è churn di oggetti Python generato
dal codice del transform**, che l'allocatore di turno poi trattiene. Il singolo
colpevole nel silver step è `Series.isin(set_da_315k_stringhe)`, che con pandas 3
riconverte l'intero set in Arrow **a ogni partizione** (~0,7 s/task solo di
conversione). La cura ("fast-isin": set → `pa.Array` una volta sola sul driver,
filtro con `pc.is_in`) elimina il creep **e rende lo step ~170× più veloce**:
tutte le 1979 partizioni processate in 16 s con RSS a plateau stabile
(~0,9 GB/worker), contro ~47 min stimati e RSS senza tetto della versione attuale.
Per i benchmark è distillata una ricetta in 5 punti (§7): pattern no-churn,
env glibc allo spawn via `pre-spawn-environ`, sweep tra le misure, monitoraggio
dell'unmanaged, riciclo `lifetime` solo come fallback. **I dati già generati sono
validi e non vanno toccati**; il fix del notebook serve solo se il silver venisse
mai rigenerato.

---

## 1 · Contesto e domanda

Durante il run completo sul corpus VM (~100 GB), lo step che trasforma
`bronze/paragraphs` (23,1 M righe di testo) in `silver/paragraphs` mandava i worker
(4 VM × 8 GB, `memory_limit` 7 GB) in OOM ripetuti. La diagnosi sul cluster
(2026-07-10, `scripts/diag_silver_paragraphs.py`) aveva stabilito che:

- la RSS dei worker cresce **tutta unmanaged** (il dashboard mostra managed ≈ 0:
  non sono partizioni finite tenute da Dask, non è backpressure NFS);
- la crescita è **cumulativa e senza plateau**: +0,9 GB/worker dopo 256 partizioni,
  +1,65 GB/worker dopo 768 → estrapolata sfonda i 7 GB;
- un `malloc_trim(0)` sui worker restituiva subito **il 54%** della crescita
  (1,2 GB su 2,3 GB) → letta come "frammentazione glibc";
- né `MALLOC_TRIM_THRESHOLD_=0` né un plugin di trim periodico bastavano.

Il workaround che ha portato a casa il run: scrittura **a blocchi** da 448
partizioni con `client.restart(wait_for_workers=True)` tra un blocco e l'altro.
Funziona, ma per la fase benchmark è inaccettabile: i colleghi devono ripetere
computazioni in loop (tempo vs n. partizioni / n. worker) e un restart tra le
misure è lento, sporca i tempi e su SSHCluster è fragile. Da qui la domanda di
questa sessione: **si può tenere la RAM sotto controllo senza mai riavviare?**

## 2 · Setup della riproduzione locale

Strumento: `scripts/leaklab.py` (laboratorio locale, per ora git-ignored; i
risultati grezzi sono in `reports/leaklab/`, anch'essi locali).

- **Dati veri**: il `data/bronze/paragraphs` locale è l'output del run VM completo
  (1024 file, 4,39 GB zstd, 23.110.668 righe, 1979 row-group da ~11,7k righe) —
  letto in sola lettura; gli output di test vanno in cartelle usa-e-getta poi
  cancellate.
- **Workload verbatim**: stesso transform della cella silver del notebook —
  `dd.read_parquet(..., split_row_groups=True)` → filtro prefer-pmc
  (`isin` su set broadcast di 315.653 `cord_uid`) → flag `is_reference_like`
  (regex su `section`) → `to_parquet` zstd con schema esplicito.
- **Topologia gemella della VM**: `LocalCluster` 4 worker × 4 thread,
  `memory_limit` 4 GB (Mac 24 GB / 12 core).
- **Misure**: N partizioni processate a chunk; a ogni chunk, per worker: RSS,
  managed, spilled (metrics dello scheduler) **più una scomposizione forense** via
  `client.run`: byte vivi/trattenuti nelle malloc zone native
  (`malloc_zone_statistics` su macOS, `mallinfo2` su Linux), byte vivi nel memory
  pool Arrow, blocchi pymalloc (`sys.getallocatedblocks`). A fine run tre probe di
  recupero in sequenza: `gc.collect()` → `pa.default_memory_pool().release_unused()`
  → trim nativo (`malloc_trim(0)` / `malloc_zone_pressure_relief`).
- **Ambiente**: python 3.11.15, dask/distributed 2026.6.0, pandas 3.0.3 (stringhe
  **arrow-backed** di default), pyarrow 24.0.0 (pool di default: **mimalloc** su
  macOS, jemalloc sulle wheel Linux). Nota: distributed 2026.6 imposta già di suo
  `MALLOC_TRIM_THRESHOLD_=65536` nel `pre-spawn-environ` dei nanny.

## 3 · Risultato 1: il creep si riproduce

Variante `base` (il codice attuale del notebook), 160/1979 partizioni:

| momento | RSS totale (4 worker) | managed | note |
|---|---:|---:|---|
| baseline a riposo | 0,55 GB | ~0 | |
| dopo 32 part | 4,0 GB | ~50 MB | salto iniziale di working-set |
| dopo 160 part | **5,0 GB** | ~75 MB | poi **+0,35 GB ogni 32 part**, senza tetto |

Stessa firma della VM: crescita tutta unmanaged, cumulativa. Throughput: **0,7
partizioni/s** (chunk da 32 in ~50 s). I probe di recupero a fine run restituiscono
briciole: gc ≈ 0, `release_unused` Arrow = **0 byte**, trim nativo 0,1–0,6 GB;
dopo i probe restano **~3,5 GB sopra la baseline** che nessuno sa restituire.

## 4 · Risultato 2: la scomposizione forense (dove NON sta la memoria)

A 160 partizioni, con RSS totale ~5 GB:

- **zone malloc native**: ~0,36 GB vivi / ~0,8 GB trattenuti → spiegano < 1 GB;
- **pool Arrow**: **0 byte vivi** (i buffer delle stringhe vengono liberati);
- **blocchi pymalloc**: crescono di ~2–3 M tra un chunk e l'altro ma un gc li
  riassorbe (sono cicli collezionabili, ~centinaia di MB);
- **⇒ ~3 GB non compaiono in nessuna contabilità**: sono pagine che gli allocatori
  "invisibili" tengono per sé dopo che gli oggetti sono già stati liberati —
  arene pymalloc di CPython (mmap da 1 MB, mai restituite se frammentate) e
  segmenti di mimalloc (il pool Arrow mmappa per conto suo, fuori dalle zone).
  Su macOS nessun trim li tocca; `release_unused()` è inefficace perché
  `mi_collect` agisce sul heap del *thread chiamante* (l'event loop), non su
  quelli dei thread di compute.

Conclusione intermedia: inseguire l'allocatore è una battaglia persa — bisogna
chiedersi **chi genera tutte queste micro-allocazioni**.

## 5 · Risultato 3: la sorgente è UNA riga (micro-probe per singola operazione)

Ripetendo 300 volte ogni singola operazione del transform sulla stessa partizione
(processo singolo, fuori dal cluster):

| operazione (×300) | ΔRSS | tempo | verdetto |
|---|---:|---:|---|
| lettura row-group → pandas | +34 MB | 0,4 s | pulita |
| **filtro `isin(set 315k)`** | **+78÷128 MB** | **206–219 s** | **churn: ~0,7 s/chiamata** |
| regex su `section` | +1 MB | 0,1 s | pulita |
| `to_parquet` zstd | +4 MB | 1,2 s | pulita |
| filtro "fast-isin" (vedi §6) | **−3 MB** | **3,0 s** | **pulito e 70× più veloce** |

Il meccanismo: con pandas 3 le colonne testo sono arrow-backed, e
`Series.isin(python_set)` converte il set in un array Arrow **a ogni chiamata**.
Il nostro set prefer-pmc ha 315.653 stringhe: ogni task di partizione paga ~0,7 s
di conversione (GIL-bound: le 4 thread del worker si serializzano — era anche il
collo di bottiglia dell'intero step) e genera centinaia di migliaia di allocazioni
temporanee. Liberate subito, ma su heap ormai frammentati: l'allocatore trattiene.

**Il "leak" ha quindi due stadi**: (1) *sorgente* = churn per-partizione nel codice;
(2) *manifestazione* = retention dell'allocatore, diversa per piattaforma — su
glibc (VM) l'heap nativo (per questo `malloc_trim` recuperava il 54%), su macOS
arene pymalloc + libmalloc + mimalloc (non recupera quasi nulla). L'Atto 3 aveva
diagnosticato correttamente lo stadio 2, ma la leva vera sta nello stadio 1.

## 6 · Risultato 4: A/B delle cure e la cura vera

Tutte le varianti su 160 partizioni, 4w×4t, mem 4 GB (campagne complete e CSV in
`reports/leaklab/`):

| variante | part/s | crescita RSS | RSS dopo i probe | verdetto |
|---|---:|---:|---:|---|
| `base` (codice attuale) | 0,7 | +3,9 GB | 3,7 GB | il creep |
| `sweep` (gc+release+trim a ogni chunk) | 0,7 | +4,5 GB | 4,1 GB | ✗ non tocca la retention |
| `hygiene` (idem, plugin ogni 2 s) | 0,7 | +4,6 GB | 4,4 GB | ✗ |
| `mimalloc-purge` (`MIMALLOC_PURGE_DELAY=0`) | 0,7 | +4,5 GB | 4,0 GB | ✗ |
| `pymalloc-off` (`PYTHONMALLOC=malloc`) | 0,7 | +4,9 GB | 4,8 GB | ✗ su macOS **peggiora** |
| `arrow-system` (pool Arrow → allocatore di sistema) | 0,6 | +3,7 GB | 2,8 GB | ~ marginale |
| `lifetime` (riciclo automatico dei worker) | **0,3** | +1,5 GB | **1,0 GB** | ~ bounded ma vedi sotto |
| **`fast-isin`** | **68,6** | +2,2 GB | 2,9 GB | ✓ **la cura** |
| `fast-isin+sweep` | 59,2 | +2,2 GB | 2,8 GB | ✓ (sweep ridondante qui) |

Note sui fallback: `lifetime` tiene la RAM bounded nel lungo periodo ma **dimezza
il throughput, sporca i tempi** (task ricalcolate a ogni riciclo) e soprattutto
**alza i picchi istantanei** (maxW 2,1 GB contro 1,4 del base: i worker superstiti
assorbono dati e task di quello che si ritira) — su worker da 7 GB vicini al limite
è un rischio, non una cura.

### La cura: "fast-isin"

Convertire il set **una volta sola sul driver** e filtrare con il kernel Arrow:

```python
# PRIMA (cella silver del notebook) — riconverte il set A OGNI partizione:
def _keep_prefer_pmc(pdf):
    return pdf[(pdf["source"] == "pmc") | (~pdf["cord_uid"].isin(pmc_uids))]

# DOPO — set -> pa.Array UNA volta (driver); nel worker solo kernel arrow:
pmc_arr = pa.array(sorted(pmc_uids), type=pa.string())

def _keep_prefer_pmc(pdf):
    import pyarrow.compute as pc
    cu = pa.array(pdf["cord_uid"], from_pandas=True)
    in_pmc = pc.is_in(cu, value_set=pmc_arr).to_numpy(zero_copy_only=False)
    return pdf[(pdf["source"].to_numpy() == "pmc") | ~in_pmc]
```

Output verificato **identico riga per riga** al filtro originale su partizioni di
test. Validazione su scala piena (tutte le **1979 partizioni** = l'intero silver
step, stesso hardware):

- **16 secondi totali** (124,6 part/s) contro ~47 minuti stimati del `base`
  (0,7 part/s): **~170×**;
- RSS: sale al plateau (~3,5 GB totali, ~0,9 GB/worker) entro ~400 partizioni e
  poi **resta piatto fino all'ultima** — il plateau è il watermark del working-set
  concorrente, non un leak: maxW 970 MB, mai vicino al limit;
- picco per-worker circa **dimezzato** rispetto al base (0,97 vs 1,4 GB a 160 part).

## 7 · Ricetta per i benchmark (senza `client.restart()`)

Per chi scrive i 4 task e i benchmark obbligatori sul cluster:

1. **Niente churn nel codice per-partizione (LA cura).** Ogni oggetto Python
   grosso catturato nel closure di `map_partitions`/`map` (set, liste, dict) viene
   pagato **a ogni partizione**. Pre-convertirlo sul driver in `pa.Array`/numpy e
   usare kernel vettoriali (`pc.is_in`, `.str.*`). Sintomi che è stato dimenticato:
   task lente e GIL-bound + unmanaged che sale linearmente con le partizioni.
2. **Env glibc allo spawn dei worker** — sul driver, **prima** di creare il
   cluster (si propaga da sé ai nodi: SSHCluster serializza la config in
   `DASK_INTERNAL_INHERIT_CONFIG`):

   ```python
   import dask
   env = dict(dask.config.get("distributed.nanny.pre-spawn-environ"))
   env.update({"MALLOC_TRIM_THRESHOLD_": 0, "MALLOC_ARENA_MAX": 2})
   dask.config.set({"distributed.nanny.pre-spawn-environ": env})
   cluster = SSHCluster(...)          # solo DOPO il config.set
   ```

   `worker_options={"env": ...}` **non** funziona per queste variabili: viene
   applicato a processo già partito, dopo l'init di glibc (verificato: è il motivo
   del fallimento del tentativo dell'Atto 3).
3. **Sweep tra una misura e l'altra**, mai dentro le regioni cronometrate:

   ```python
   def sweep():
       import gc, ctypes, ctypes.util
       import pyarrow as pa
       gc.collect()
       pa.default_memory_pool().release_unused()
       try:
           ctypes.CDLL(ctypes.util.find_library("c")).malloc_trim(0)
       except Exception:
           pass
   client.run(sweep)
   ```

   Su glibc recupera la parte trattenuta dall'heap nativo (il 54% misurato sul
   cluster); costa ~nulla.
4. **Monitorare l'unmanaged tra le ripetizioni** (dai metrics dello scheduler:
   RSS − managed − spilled per worker). Se sale **linearmente** con le ripetizioni
   c'è un churn hotspot nel task → tornare al punto 1. Se oscilla attorno a un
   plateau, è working-set: va bene così.
5. **Fallback, in ordine**: worker `lifetime` con stagger (solo tra batch di
   misure: picchi più alti e tempi sporchi); `client.restart()` solo tra
   *configurazioni* di benchmark, mai tra ripetizioni della stessa misura.

## 8 · Cosa cambia (e cosa NO) per la pipeline

- **`data/` resta com'è**: il silver del run VM è valido, gli invarianti sono
  verificati, niente va rigenerato. Il workaround blocchi+restart era legittimo e
  ha fatto il suo lavoro.
- **Se il silver venisse mai rigenerato**: portare fast-isin nella cella silver
  del notebook. Con ~170× di throughput lo step diventa I/O-bound e, molto
  probabilmente, blocchi e restart diventano superflui (da riverificare su glibc).
- **Caveat onesto**: la controprova su glibc non è ancora stata fatta. Meccanismo
  ed evidenza locale dicono che trasferisce; la conferma costa ~20 minuti alla
  prossima sessione cluster: `leaklab.py --variants base,fast-isin,trimenv` sulla
  VM e confronto delle curve (lo script gira invariato lì).

## 9 · Artefatti

| cosa | dove | tracked? |
|---|---|---|
| questo report | `docs/MEMORY_LEAK_REPORT.md` | ✓ |
| laboratorio di riproduzione/A-B | `scripts/leaklab.py` | per ora no (git-ignored) |
| campagne: log, CSV per-campione e per-chunk, plot | `reports/leaklab/` | no (git-ignored) |
| diagnostica VM originale (Atti 1–3) | `scripts/diag_silver_paragraphs.py` | ✓ |
| diario OOM Atti 1–3 | `docs/PROJECT_CONTEXT.md` §7 | ✓ |
