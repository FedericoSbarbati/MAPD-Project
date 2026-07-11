# MAPD-B · CORD-19 — Contesto di progetto, schemi dati e diario dei problemi

> **A cosa serve questo documento.** Il codice si scrive sul Mac ma gira "per davvero"
> sulla VM di Cloud Veneto ([userguide](https://userguide.cloudveneto.it/en/latest/)):
> due ambienti con dati, risorse e vincoli **diversi**. Chi scrive codice (persona o
> agente) deve leggere questo file per capire il contesto reale della VM prima di
> toccare la pipeline. Documenti gemelli: `docs/SETUP_CLOUDVENETO.md` (accesso SSH
> sicuro al cluster), `DATA_DICTIONARY.md` (dizionario colonna-per-colonna dei Parquet).

**Stato al 2026-07-11:** la fase di generazione dati è **COMPLETATA** — il run completo
sul corpus da ~100 GB è andato a buon fine sulla VM e l'output (`data/`, ~9 GB) è stato
scaricato in locale. Prossima fase: i 4 task di analisi + benchmark obbligatori.

---

## 1 · I due ambienti (leggere PRIMA di scrivere codice)

| | **Mac (sviluppo)** | **VM Cloud Veneto (esecuzione reale)** |
|---|---|---|
| Ruolo | scrivere codice, dry-run su campione | run completi sul corpus vero |
| Dump raw CORD-19 | `archive/` ~28 GB — **versione VECCHIA** del dataset (metadata: 425.796 righe → 406.211 `cord_uid`) | volume `/data` — dump **completo** Kaggle v111 (~100 GB estratti, 18,4 GB compressi; metadata: 1.056.660 righe → 970.836 `cord_uid`) |
| Ambiente Python | conda `mapd-covid` (py 3.11, dask 2026.6.0) | venv `~/pyvenv` (dask 2026.6.0 + asyncssh), Ubuntu 22.04 |
| Cluster | `LocalCluster` (default 4 worker) | `SSHCluster` multi-nodo: node0 scheduler+NFS + worker da 8 GB |
| Output pipeline | `data_sample/` (dry-run con `CORD19_SAMPLE=N`) | `data/` sul volume persistente |
| `data/` locale | ⚠️ **copia SCARICATA dell'output VM** (run completo) — NON rigenerabile in locale | l'originale, su `/data` |
| Dashboard Dask | `localhost:8787` | `node0:8787` (tunnel via gate) |

**Workflow:** codice sul Mac → push su GitHub → pull sulla VM (repo sul volume) →
esecuzione headless su copia usa-e-getta → output su `/data` → (una tantum) download
di `data/` sul Mac.

**Conseguenza da non dimenticare:** i numeri di un run locale e di un run VM **non
coincidono mai** (dump diversi). Non hard-codare conteggi assoluti negli assert (il
numero magico `406211` è già stato rimosso una volta); le garanzie vere sono
strutturali (unicità di `cord_uid`, integrità referenziale, invariante prefer-pmc).

---

## 2 · Cosa è stato fatto finora (cronologia logica)

1. **Esplorazione** (`Initial Exploration/`: 3 notebook eseguiti) → schema reale dei
   JSON e di `metadata.csv`, con tutti i gotcha di §3.
2. **Decisione architetturale:** pre-convertire JSON/CSV in **Parquet partizionato**
   (bronze → silver), modello relazionale keyed su `cord_uid`. Scartato un DB server
   (anti-pattern per un esercizio Dask distribuito). Il deliverable del nostro
   sottogruppo è il **dataset silver + documentazione**; i 4 task di analisi li fanno
   i compagni sopra il silver.
3. **Pipeline in `.py`**, validata end-to-end sul dump locale (~98 s, ~4,2 GB).
4. **Consolidamento in un unico notebook** `conversion_sanification.ipynb` guidato da
   env var (stesso file gira invariato su Mac e VM) e **riscrittura fully-Dask**
   (zero pandas lato driver: il corpus VM non ci starebbe).
5. **Setup Cloud Veneto:** VM scheduler, snapshot per clonare i worker, `SSHCluster`,
   volume dati da 200 GB, NFS, modello di accesso a chiavi per il gruppo (§5 e
   `docs/SETUP_CLOUDVENETO.md`).
6. **Run sul corpus completo** → serie di OOM sui worker → tre round di
   diagnosi & fix (il "memory leak", §7) → run completato, output scaricato.

---

## 3 · Schema dei dati ORIGINALI (CORD-19 raw)

Un **catalogo + due sorgenti di full-text indipendenti**:

- **`metadata.csv`** — il catalogo, una riga per record bibliografico (19 colonne).
  `cord_uid` identifica il paper ma **non è unico** (righe duplicate → dedup
  obbligatorio prima di ogni statistica per-paper). Contiene `title`, `abstract`,
  `authors`, `journal`, `publish_time`, `doi`, `url`, `license` anche quando **non
  esiste nessun JSON** di full-text (≈65% delle righe sul dump locale).
- **`document_parses/pdf_json/`** — full-text estratto dal **PDF** (GROBID/S2ORC).
  Nome file = `<sha>.json` (colonna `sha`).
- **`document_parses/pmc_json/`** — full-text dall'**XML PubMed Central** (più pulito
  e completo del parse PDF). Nome file = `<pmcid>.xml.json` (colonna `pmcid`).
- `cord_19_embeddings/` — embeddings precomputati, **non usati** (scelta esplicita).

**Linking:** dalle colonne `pdf_json_files` / `pmc_json_files` del catalogo ai path
dei JSON; una riga può referenziare **più** parse PDF (`;`-separati), mai più di un
PMC. Linkage verificato quasi perfetto: 0 orfani in entrambe le direzioni.

### Gotcha dei file JSON (imparati a caro prezzo)

- Ogni file è **UN oggetto JSON pretty-printed multi-riga**, NON json-lines →
  leggere file interi (`db.from_sequence(paths).map(json.load)`), mai
  `db.read_text().map(json.loads)`.
- Lo `json_schema.txt` in bundle **mente**: nei file veri `metadata` contiene solo
  `title` e `authors`; `abstract`, `body_text`, `bib_entries`, `ref_entries`,
  `back_matter` sono chiavi **top-level**.
- I file `pmc_json` **non hanno la chiave `abstract`** → `record.get('abstract', [])`.
- Le **affiliazioni** (`metadata.authors[i].affiliation`) sono popolate ~50% nei
  `pdf_json` e **~0% nei `pmc_json`** → per paesi/istituti si usa SOLO il ramo PDF.
- Dati sporchi ovunque: autori vuoti, titoli mancanti, paesi scritti in 10 modi
  (`USA` / `United States` / `United States of America`…).

---

## 4 · Schema dei dati DERIVATI (Parquet bronze/silver)

Due layer, tutti Parquet (zstd), tutti keyed su `cord_uid`
(dettagli colonna-per-colonna in `DATA_DICTIONARY.md`):

- **`data/bronze/`** — estrazione fedele dal raw, solo gate strutturale (file
  imparsabile / chiave mancante ⇒ skip). Riproducibile, non ripulito.
- **`data/silver/`** — pulito e canonicalizzato, **analysis-ready**: è quello che i
  task leggono. Principio: correggiamo errori oggettivi e **aggiungiamo flag**
  (`is_reference_like`, `is_title_unique`…), NON prendiamo decisioni di analisi
  (duplicati flaggati e non rimossi, niente tokenizzazione).

| Dataset | Grain | Sorgente | Task servito |
|---|---|---|---|
| `silver/papers` | 1 riga / paper | `metadata.csv` (dedup, prefer riga con full-text) | 3–4 (titoli: `cord_uid`, `title`, `title_norm`, `is_title_unique`) |
| `silver/paragraphs` | 1 riga / paragrafo | `pmc_json` **preferito**, `pdf_json` fallback (mai entrambi per lo stesso paper) | 1 (word-count su `text`) |
| `silver/authors` | 1 riga / (paper, autore) | solo `pdf_json` | 2 (`country_iso3`, `institution_norm`) |
| `silver/paper_countries` | rollup: paese distinto per paper | da authors | 2 |
| `silver/paper_institutions` | rollup: istituto distinto per paper | da authors | 2 |

Colonne chiave di `silver/paragraphs` (schema Arrow esplicito, pinnato in scrittura):
`cord_uid` (FK), `paper_id` (sha o pmcid), `source` (`'pmc'`/`'pdf'`), `para_idx`,
`section` (raw, sporchissima — non usarla come categoria), `text`,
`is_reference_like` (bool).

### Conteggi: run locale (dump vecchio) vs run VM (corpus completo)

| Dataset | Run locale ~28 GB | **Run VM ~100 GB** (= `data/` attuale) | File |
|---|---:|---:|---:|
| `bronze/papers` | 425.796 | **1.056.660** | 9 |
| `bronze/paragraphs` | 8.075.476 | **23.110.668** | 1.024 |
| `bronze/authors` | 1.019.793 | **2.943.737** | 192 |
| `silver/papers` | 406.211 | **970.836** | 9 |
| `silver/paragraphs` | 4.719.311 | **12.445.234** | 1.979 |
| `silver/authors` | 1.019.793 | **2.943.737** | 192 |
| `silver/paper_countries` | 102.431 | **284.042** | 64 |
| `silver/paper_institutions` | 184.818 | **517.911** | 48 |

⚠️ `DATA_DICTIONARY.md` riporta ancora i conteggi del run locale: lo **schema** è
identico, i **numeri** no. `silver/paragraphs` ha 1.979 file perché è scritto a
blocchi da partizioni-per-row-group (vedi §7): tante partizioni piccole e uniformi
sono volute, non un incidente.

### Garanzie di integrità (verificate dai sanity check di §11 del notebook)

- `papers.cord_uid` unico; `cord_uid` di authors/paragraphs/rollup ⊆ papers.
- **Prefer-pmc:** nessun paper compare in `paragraphs` con entrambe le sorgenti.
- Canonicalizzazione paesi: ~99% dei `country_raw` non nulli risolti a ISO3
  (`country_converter` + dizionario alias custom).

---

## 5 · Cluster Cloud Veneto e gestione del volume dati

### Topologia (decisa dopo gli OOM: pochi worker grossi, non tanti piccoli)

```
                    gate.cloudveneto.it  (ProxyJump, accesso umano)
                            │
   node0 = scheduler + NFS server (NON worker)      rete interna 10.67.22.x
   │  • volume Ceph "mapd-data" 200 GB → mount /data
   │  • esporta /data via NFS alla subnet
   │  • la sua RAM fa da page-cache NFS per tutti
   ├── worker-1  (8 GB RAM, clone dello snapshot, monta /data via NFS)
   ├── worker-2  (idem)
   └── worker-N  (run finale: 4 worker × 8 GB; memory_limit 7 GB, 4 thread)
```

Scelte chiave e perché:

- **node0 NON è un worker:** possiede il volume e serve NFS; tenerlo fuori dal pool
  (a) evita che diventi lo straggler, (b) lascia la sua RAM come cache NFS. In
  `SSHCluster` il **primo host della lista è solo scheduler**; un host diventa anche
  worker solo se ripetuto → in `cluster.txt` node0 compare **una volta sola**.
- **Worker clonati da SNAPSHOT** dello scheduler: ereditano venv Dask e chiave
  macchina del cluster → zero configurazione per-VM. I ruoli sono solo runtime
  (l'ordine in `cluster.txt`), le VM sono identiche.
- **SSHCluster dal notebook:** `known_hosts=None` (gli IP interni vengono riciclati),
  `remote_python=~/pyvenv/bin/python` (senza, Dask non trova il venv sui nodi),
  `worker_options={"nthreads": 4, "memory_limit": "7GB"}`. Il security group del
  corso `pod-students` già permette il traffico intra-cluster: **non modificarlo mai**.

### Il volume dati (`mapd-data`, 200 GB Ceph) e l'NFS

Un volume OpenStack si attacca a **una sola VM per volta** → lo attacchiamo a node0 e
condividiamo via NFS. Contiene TUTTO ciò che deve sopravvivere alle VM: il raw
scaricato con kagglehub (`/data/kagglehub/.../CORD-19-research-challenge/versions/111`
= input della pipeline), il clone del repo e quindi l'output `data/`.

Lo script **idempotente** `scripts/cluster_storage_up.sh` (da lanciare su node0) fa
tutto in un colpo: monta `/dev/vdb` su `/data` (**MAI `mkfs`** — si fa solo la
primissima volta, riformattare = cancellare i dati), installa/esporta NFS verso
`10.67.22.0/24`, e via SSH monta `/data` **sullo stesso path** su ogni worker
(IP letti da `cluster.txt`), verificando che i dati si vedano. Teardown speculare:
`bash scripts/cluster_storage_up.sh --down`.

### Ciclo di vita di una sessione di lavoro (il progetto OpenStack è CONDIVISO col corso)

1. Lancia le VM dallo snapshot (dashboard) → aggiorna `cluster.txt` con gli IP nuovi.
2. Attacca il volume `mapd-data` a node0 (dashboard) → `bash scripts/cluster_storage_up.sh`.
3. Lavora (notebook via nbconvert su copia throwaway, vedi §6).
4. Fine sessione: `cluster_storage_up.sh --down` → detach del volume → **CANCELLA le
   VM** (la quota è condivisa con tutta la coorte). Volume e snapshot persistono:
   ricreare il cluster costa minuti.

Vincolo del corso: **niente Docker** — cluster installato e gestito a mano.
Caveat accettato: l'NFS incanala tutto l'I/O sul singolo node0 — va bene perché la
conversione è un one-shot I/O-bound; i **benchmark obbligatori** (tempo vs partizioni
/ vs worker) si fanno sui 4 task che leggono Parquet (CPU-bound, scalano davvero).

---

## 6 · Contratto di esecuzione del notebook

`conversion_sanification.ipynb` è l'**unica** pipeline (i vecchi `.py` sono stati
consolidati lì). Gira invariato su Mac e VM perché tutto passa da env var:

| Env var | Default | Significato |
|---|---|---|
| `CORD19_ARCHIVE` | `./archive` | root del dump raw (sulla VM: la cartella kagglehub v111) |
| `CORD19_DATA` | `./data` | root output Parquet |
| `CORD19_HOSTS` / `cluster.txt` | — | IP del cluster: `sched,worker1,...`; **primo host = solo scheduler**; ripetilo per farlo anche worker. Assente ⇒ `LocalCluster` |
| `DASK_SCHEDULER` | — | scheduler già avviato (`tcp://host:8786`) ⇒ solo `Client` |
| `CORD19_SAMPLE` | `0` | `N` = dry-run su N file/sorgente → scrive in `data_sample/` (mai sopra i dati veri) |
| `CORD19_WORKERS` | `4` | n. worker del LocalCluster |
| `CORD19_THREADS_PER_WORKER` | `4` | era 8: causava OOM da kernel (§7) |
| `CORD19_WORKER_MEMORY_LIMIT` | `7GB` | tetto nanny su VM da 8 GB (1 GB a OS+NFS) |
| `CORD19_NPART_PARA` / `_AUTH` / `_PAPERS` | `1024` / `192` / `9` | granularità di **estrazione** (quanto è grossa la lista costruita in RAM da UNA task) |
| `CORD19_ROW_GROUP` | `20000` | righe per row-group del bronze paragraphs → partizioni **uniformi** in rilettura (§7) |
| `CORD19_PARA_BATCH` | `448` | partizioni per blocco nella scrittura batched del silver/paragraphs (§7) |
| `CORD19_SSH_KEY` | `~/.ssh/id_rsa` | chiave privata per SSHCluster |

**Sulla VM si esegue headless su una copia usa-e-getta**, così il file tracciato resta
byte-pulito e `git pull` non confligge mai (il notebook committato è output-free):

```bash
jupyter nbconvert --to notebook --execute \
  --ExecutePreprocessor.kernel_name=<kernel> \
  --output /tmp/executed_conversion.ipynb  conversion_sanification.ipynb
```

Monitoraggio integrato: le fasi pesanti sono avvolte in `performance_report(...)` +
`MemorySampler` → `reports/` (git-ignored) raccoglie gli HTML Bokeh, la timeline RAM
(`memory_timeline.png/csv`) e i log dei worker con il conteggio dei restart
(0 = run pulito).

---

## 7 · Il "memory leak" di silver/paragraphs — diario in tre atti

La fase che trasforma `bronze/paragraphs` (23,1M righe di testo) in silver mandava i
worker in OOM. Tre cause DIVERSE, scoperte in sequenza; tutte e tre le mitigazioni
sono nel notebook attuale. Diagnosi riproducibile con `scripts/diag_silver_paragraphs.py`
(fasi A–E isolate, eseguito sul cluster VERO).

> **Atto 4 (2026-07-11): il creep è stato riprodotto IN LOCALE e root-causato** —
> la sorgente è il churn per-partizione di `isin(set 315k)` (pandas 3 riconverte il
> set in Arrow a ogni task), l'allocatore trattiene solo di conseguenza; la cura
> "fast-isin" (`pa.Array` sul driver + `pc.is_in`) elimina il creep ed è ~170× più
> veloce. Report completo, A/B delle cure e ricetta anti-leak per i benchmark:
> **`docs/MEMORY_LEAK_REPORT.md`**.

### Atto 1 — Picco per-task × troppe thread (kernel OOM-killer)

**Sintomo:** worker uccisi con `signal 9` senza il log "95% memory budget" (= OOM
killer del kernel, il nanny non fa in tempo) oppure `signal 15` dopo il budget (= il
nanny li riavvia). Ogni kill perde i risultati in RAM → ricompute → più letture NFS →
più OOM: una **spirale della morte** che rendeva il run anche più lento.
**Causa:** ogni task di estrazione materializza l'intera partizione come lista Python
+ DataFrame + buffer Arrow (0,5–1 GB di picco), e con 8 thread/worker giravano fino a
8 task simultanee. **Lezione fondamentale: Dask spilla solo i RISULTATI FINITI, mai
una lista in costruzione dentro una task in corso** → le uniche leve sono partizioni
più piccole e meno thread; il tuning delle soglie di spill non serve a niente qui.
**Fix:** `THREADS_PER_WORKER` 8→4, `NPART_PARA` alzato (poi 1024), `memory_limit`
esplicito 7 GB, e il join prefer-pmc riscritto da `merge` (shufflava l'intera colonna
`text` sulla rete) a **broadcast**: set dei `cord_uid` pmc raccolto sul driver +
`map_partitions` + `.isin`.

### Atto 2 — Skew delle partizioni (la partizione "mostro")

**Sintomo:** OOM ancora, nonostante l'Atto 1. **Causa:** i paper full-text sono
clusterizzati all'INIZIO di `metadata.csv`, quindi il `repartition(320)` per range di
righe scaricava un grumo denso nelle prime partizioni: misurata la partizione 0 a
222k righe / 229 MB contro <5 MB delle altre. Il transform silver amplifica ×3–4
(Arrow→pandas→regex→Arrow) → una singola task da svariati GB, indipendente da thread
e trim. **Vicolo cieco:** `repartition(partition_size="24MB")` **si impianta** sotto
un Client distribuito (deve materializzare per stimare le taglie — pitfall #3 di
dask-expr, vedi §8). **Fix:** il bronze paragraphs si scrive con
`row_group_size=20000` e il silver lo rilegge con `split_row_groups=True` → una
partizione per row-group, **uniforme a prescindere dallo skew dei file**. Caveat
operativo: un bronze scritto SENZA row-group ha un solo row-group per file e lo split
non può spezzarlo → bronze da rigenerare (o ri-chunkare una volta).

### Atto 3 — Frammentazione dell'allocatore glibc (il vero "leak")

**Sintomo:** con partizioni piccole e uniformi, la RSS dei worker cresce comunque,
tutta **unmanaged** (il dashboard mostra managed≈0: non è Dask che accumula, non è
NFS). **Diagnosi sul cluster vero:** processando 256/1979 partizioni la RSS sale di
~2,3 GB; un `malloc_trim(0)` sui worker ne restituisce subito 1,2 GB (54%) — è glibc
che TRATTIENE la memoria liberata dopo migliaia di allocazioni testuali. E la
crescita è **cumulativa, senza plateau** (256 part → 0,9 GB/worker; 768 → 1,65 GB):
estrapolata sull'intero run sfonda i 7 GB. Né `MALLOC_TRIM_THRESHOLD_=0` né un plugin
che chiama `malloc_trim` bastano da soli. **Fix definitivo (nel notebook):** la cella
silver/paragraphs scrive **a blocchi** da `CORD19_PARA_BATCH=448` partizioni con
`client.restart(wait_for_workers=True)` tra un blocco e l'altro → la RAM torna a
baseline a ogni giro, completamento garantito su worker da 8 GB. Dettagli che contano:
`pmc_uids` si calcola UNA volta sola sull'intera tabella (lettura narrow di
`source`+`cord_uid`) e, siccome quel compute da solo gonfia i worker di ~2–3 GB, c'è
un **restart anche prima del primo blocco**; i blocchi scrivono in una cartella
piatta con `name_function` a offset (`part.<b0+i>.parquet`), `overwrite` solo al
primo blocco, `write_metadata_file=False` (niente `_metadata` condiviso da pestarsi).
Verificato: output batched ≡ non-batched; il run completo VM è passato.

**Morale per il codice futuro:** su questo cluster, un worker long-lived che macina
milioni di stringhe accumula RSS unmanaged per frammentazione. Se un task di analisi
mostra lo stesso profilo (crescita unmanaged lineare, managed≈0), il pattern è:
lavoro a blocchi + `client.restart` tra i blocchi; leve extra `THREADS_PER_WORKER=2`
o worker da 16 GB.

---

## 8 · Regole e invarianti per chi scrive codice (checklist per gli agenti)

1. **Non rigenerare `data/` in locale**: quello che c'è ora è l'output del run VM
   completo; il dump locale è più vecchio e piccolo. Per provare la pipeline:
   `CORD19_SAMPLE=N` → scrive in `data_sample/`.
2. **Niente numeri assoluti negli assert** (i due dump differiscono): solo garanzie
   strutturali; i check quantitativi sono già guardati da `if not SAMPLE`.
3. **Zero pandas lato driver** nella pipeline: il corpus VM non entra nel driver.
   Uniche raccolte driver ammesse (piccole e motivate): mappe di linkage, set
   `pmc_uids`, valori distinti dei paesi.
4. **Pitfall di dask-expr 2026.6** (tutti verificati sulla nostra versione):
   `value_counts().reset_index()` dentro un merge → `KeyError None` (usare
   `groupby.size().reset_index()`); `groupby.transform` dopo shuffle → errore di
   reindex (usare size+merge); `repartition(partition_size=...)` sotto un Client
   distribuito → **hang** (usare `row_group_size` + `split_row_groups`).
5. **Modello di memoria:** una lista costruita dentro una task NON è spillabile →
   il picco per-task si controlla con la granularità di estrazione (`NPART_*`) e le
   thread, non con le soglie di spill. Testo su worker long-lived → pattern
   blocchi+restart (§7, Atto 3).
6. **Portabilità Mac↔VM:** mai hard-codare path o IP; tutto via env var / `cluster.txt`
   (git-ignored); il notebook committato resta output-free; sulla VM si esegue su
   copia throwaway.
7. **Grafo piccolo:** i work-item del Bag sono **solo filename** (il path si
   ricostruisce nel worker); gli schemi Arrow si passano **espliciti** a `to_parquet`
   (aggira l'inferenza sulle partizioni all-null).
8. **Cloud Veneto:** non toccare il security group condiviso `pod-students`; VM
   cancellate a fine sessione; mai `mkfs` sul volume; NFS = tutto l'I/O raw passa da
   node0 (accettato per la conversione one-shot, non per i benchmark).

---

## 9 · Stato attuale e prossimi passi

**Fatto:** esplorazione; architettura bronze/silver; pipeline consolidata e
fully-Dask; cluster + volume + NFS operativi e scriptati; tre round di fix OOM;
**run completo sul corpus VM riuscito**; output scaricato in `data/` sul Mac.

**In sospeso / prossimi passi:**
- Committare il diff pendente di `conversion_sanification.ipynb` (restart
  post-`pmc_uids` + rimozione assert 406211): è la versione che ha completato il run.
- Aggiornare i conteggi in `DATA_DICTIONARY.md` ai numeri del run completo (§4).
- Accesso SSH dei compagni alle VM (runbook pronto in `docs/SETUP_CLOUDVENETO.md`).
- I 4 task di analisi (compagni) sopra il silver + **benchmark obbligatori**
  (tempo vs n. partizioni e vs n. worker) — senza benchmark l'analisi è incompleta.
  Per i benchmark seguire la ricetta anti-memory-creep di `docs/MEMORY_LEAK_REPORT.md`
  §7 (niente churn broadcast, `pre-spawn-environ`, sweep tra le misure, NO
  `client.restart()` tra le ripetizioni).
- Alla prossima sessione cluster: controprova su glibc dell'Atto 4 (~20 min,
  `scripts/leaklab.py --variants base,fast-isin,trimenv` sulla VM).
