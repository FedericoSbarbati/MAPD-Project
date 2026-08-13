# MAPD-B · CORD-19 — Contesto di progetto, schemi dati e diario dei problemi

> **A cosa serve questo documento.** Il codice si scrive sul Mac ma gira "per davvero"
> sul cluster di Cloud Veneto ([userguide](https://userguide.cloudveneto.it/en/latest/index.html),
> fonte autorevole): due ambienti con dati, risorse e vincoli **diversi**. Qui sta il
> **perché** delle scelte; i passi operativi stanno in `docs/SETUP_CLOUDVENETO.md`, le
> decisioni già prese in `docs/DECISIONI.md`, le colonne dei Parquet in
> `DATA_DICTIONARY.md`.

**Stato al 2026-08-13.** I dati sono **fatti e definitivi**: la conversione del corpus da
~100 GB è andata a buon fine e il `silver/` è oggi **replicato su ogni macchina** del
cluster (e scaricato in locale in `data/`). Il **task 1 — word count — è finito e
validato** sul corpus intero. Restano i task 2, 3, 4 e i **benchmark obbligatori**, la cui
impalcatura va rifatta da zero (§9).

> ⚠️ **L'architettura del cluster è cambiata ad agosto 2026.** Il volume condiviso via
> NFS non si usa più: adesso **ogni persona ha il suo cluster** e i dati sono replicati
> sul disco di ogni macchina. Se trovi da qualche parte istruzioni su come montare
> `/data` o lanciare `scripts/cluster_storage_up.sh`, è materiale della fase precedente.

---

## 1 · I due ambienti (leggere PRIMA di scrivere codice)

| | **Mac (sviluppo)** | **Cluster Cloud Veneto (esecuzione reale)** |
|---|---|---|
| Ruolo | scrivere codice, provare su un campione | run completi sul corpus vero, benchmark |
| Dati usati dai task | `data/silver/` — copia scaricata dal run di conversione | `~/mapd-data/silver/` — **una copia su OGNI macchina**, dallo snapshot |
| Ambiente Python | conda `mapd-covid` (py 3.11, dask 2026.6.0) | venv `~/pyvenv` (dask 2026.6.0 + asyncssh), Ubuntu 22.04 |
| Cluster | `LocalCluster`, dimensionato sui core del Mac | `SSHCluster`: 3–5 macchine uguali, la prima solo scheduler |
| Output | `reports/` (in migrazione verso `~/mapd-out/`) | `~/mapd-out/` — **fuori dalla repo**, che è usa-e-getta |
| Come si lancia | file `.py` da terminale | file `.py` da terminale, via SSH dal gate |
| Dashboard Dask | `localhost:8787` | `<ip scheduler>:8787` (tunnel via gate) |

**Workflow:** codice sul Mac → push su GitHub → sul cluster si clona la repo → si scrive
`cluster.txt` → si lancia il `.py` → i risultati in `~/mapd-out/` → si scaricano sul Mac.
I passi esatti sono in `docs/SETUP_CLOUDVENETO.md` §2.

**Niente notebook in esecuzione.** Si lavora da terminale: i file `.py` sono ciò che gira,
i notebook servono a **spiegare** il codice, non a eseguirlo. Non c'è VS Code remoto e non
si apre Jupyter sulle VM.

**Il dump grezzo non serve più.** `archive/` sul Mac (~28 GB, versione vecchia) e i ~100 GB
sul volume di rete servivano alla conversione JSON→Parquet, che è finita e **non fa parte
dell'assignment**. Restano come archivio. I task leggono solo il `silver/`.

**Conseguenza da non dimenticare:** i numeri di un run sul dump locale vecchio e quelli
del corpus completo **non coincidono mai**. Non hard-codare conteggi assoluti negli assert
(il numero magico `406211` è già stato rimosso una volta); le garanzie vere sono
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
5. **Setup Cloud Veneto, prima versione:** VM scheduler, snapshot per clonare i worker,
   `SSHCluster`, volume dati da 200 GB condiviso via NFS.
6. **Run di conversione sul corpus completo** → serie di OOM sui worker → tre round di
   diagnosi & fix (il "memory leak", §7) → run completato, output scaricato.
7. **Task 1 (word count) rifatto da zero** (cartella `Giulia/`, diario in
   `local/NOTES.md`): il codice precedente non era spiegabile all'orale e il suo
   "benchmark" misurava la cosa sbagliata. Sei versioni, ognuna motivata da una misura;
   la scoperta centrale è che in Dask Bag ogni riduzione tipo-groupby collassa in **una
   sola partizione**, quindi la scelta della chiave di riduzione decide se il lavoro è
   fattibile. Run completo passato: 12.445.234 paragrafi, 785.753.529 occorrenze.
8. **Prima sessione sul cluster vero, fallita** — e per nessun motivo algoritmico:
   `cluster.txt` nel posto sbagliato (fallback silenzioso su una macchina sola), limiti
   di memoria assoluti ereditati da VM di taglia diversa, path relativi alla cartella da
   cui si lanciava. Da lì: configurazione sempre stampata, dimensionamento relativo, path
   risolti sulla radice della repo.
9. **Cambio di architettura (agosto 2026):** via l'NFS, un cluster per persona, dati
   replicati su ogni macchina (§5). E presa d'atto che l'impalcatura dei benchmark
   (`bench.py`) è cresciuta oltre lo scopo del corso: va rifatta (§9).

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

`DATA_DICTIONARY.md` riporta i conteggi del corpus completo, **rimisurati sui dati veri il
2026-08-13** (colonna per colonna). `silver/paragraphs` ha 1.979 file perché è scritto a
blocchi da partizioni-per-row-group (vedi §7): tante partizioni piccole e uniformi
sono volute, non un incidente.

### Garanzie di integrità (verificate dai sanity check di §11 del notebook)

- `papers.cord_uid` unico; `cord_uid` di authors/paragraphs/rollup ⊆ papers.
- **Prefer-pmc:** nessun paper compare in `paragraphs` con entrambe le sorgenti.
- Canonicalizzazione paesi: ~99% dei `country_raw` non nulli risolti a ISO3
  (`country_converter` + dizionario alias custom).

---

## 5 · Il cluster: perché è fatto così

I passi per accenderlo sono in `docs/SETUP_CLOUDVENETO.md`. Qui c'è solo il ragionamento
dietro ogni scelta — serve per difenderle all'orale e per non "migliorarle" per sbaglio.

```
                    gate.cloudveneto.it  (accesso umano, ProxyJump)
                            │
   macchina 1 = SOLO scheduler + il terminale da cui lanci      rete 10.67.22.x
   │   (copia locale dei dati in ~/mapd-data/silver)
   ├── worker-1   copia locale dei dati
   ├── worker-2   copia locale dei dati
   └── worker-N   3 macchine minimo, 4–5 di solito, tutte della STESSA taglia
                  (medium 4 GB RAM / large 8 GB, ~25 GB di disco)
```

**Un cluster per persona, non uno condiviso.** È la scelta che ha causato tutte le altre,
ed è organizzativa, non tecnica: un volume OpenStack si attacca a **una sola macchina per
volta**, quindi condividere i dati via NFS obbligava tutti e quattro a lavorare sullo
stesso identico cluster, coordinandosi su ogni run. In quattro non funziona. Adesso ognuno
sviluppa sul proprio e la forma del cluster (quante macchine, che taglia) si concorda
quando serve confrontare dei numeri.

**I dati si replicano, non si condividono.** Il `silver/` in Parquet è abbastanza piccolo
da stare sul disco di ogni macchina, e ci arriva dall'**immagine snapshot**: ogni VM nasce
già con la sua copia in `~/mapd-data/silver/`. Nessun file system di rete, nessuno script
di montaggio, nessun ordine di avvio da rispettare.
*Cosa costa:* prima c'era una copia sola e "identica ovunque" era gratis; adesso ci sono N
copie indipendenti. Il funzionamento dipende dal fatto che il **path sia identico su tutte
le macchine** e che i dati siano davvero gli stessi. Se un domani il `silver/` cambia, va
rifatta l'immagine: aggiornarne una sola darebbe risultati sbagliati **senza nessun
errore visibile**, che è il guasto peggiore da diagnosticare.

**La prima macchina fa solo da scheduler.** In `SSHCluster` il primo host della lista è
scheduler e basta; diventa anche worker solo se lo ripeti. Lo teniamo fuori dal pool per
tre motivi — nessuno dei quali è più l'NFS, che era la ragione originaria ed è morta con
lui:
1. quella macchina non è ferma: ci girano lo **scheduler** (il coordinatore) e il
   **processo da cui lanci**, quello che riceve i risultati finali;
2. su macchine da 4 GB un worker che sfonda il tetto viene ucciso — è già successo, tre
   worker su quattro a metà run (`local/NOTES.md` v6). Se succede sulla macchina che
   coordina, non perdi un worker: rischi il run intero;
3. i benchmark "tempo vs numero di worker" si interpretano solo se i worker sono
   intercambiabili. Uno che divide la macchina con scheduler e driver è sistematicamente
   più lento e piega la curva per motivi che non c'entrano con l'algoritmo.

**Macchine tutte della stessa taglia.** Dask assegna il lavoro dando per scontato che i
worker siano equivalenti: il più piccolo diventa il freno di tutti, e i tempi non si
sanno più spiegare.

**Niente Docker** (vincolo del corso): il cluster è reale, installato e gestito a mano.
**Il security group `pod-students`** permette già il traffico tra le macchine: si
seleziona e non si tocca, è condiviso con tutto il corso.

**Le macchine non si cancellano a fine sessione** — regola ribaltata rispetto alla fase
precedente. Senza volume condiviso i risultati stanno sui dischi delle macchine, quindi
distruggerle vuol dire buttare il lavoro. In compenso i risultati vanno **scaricati** sul
portatile: sono pochi megabyte, e finché stanno su una macchina sola sono a rischio.
Tensione da tenere presente: la guida del corso raccomanda di essere parsimoniosi perché
il pool di risorse è condiviso con tutti gli studenti.

**Cosa cambia per i benchmark, ed è la buona notizia.** Con l'NFS tutto l'I/O passava da
una macchina sola: accettabile per la conversione (un one-shot legato al disco), ma
avrebbe falsato le curve. Adesso **ogni worker legge dal proprio disco**, quindi la
lettura è parallela davvero e i benchmark misurano il calcolo invece della coda sulla
rete. È un punto da mettere nella relazione, non solo una nota tecnica.

**Il volume da 200 GB esiste ancora**, staccato, con i dati grezzi e i Parquet completi.
Non lo usa più nessuno: serviva alla conversione. Per la stessa ragione
`scripts/cluster_storage_up.sh` (montaggio + export NFS) **non si lancia più**.

---

## 6 · Come si esegue il codice

**I task si lanciano come file `.py`, da terminale.** I notebook (`Giulia/word_count.ipynb`,
`daniele/task_2_3_3_title_embeddings.ipynb`) sono **documentazione**: spiegano e articolano
le scelte, non sono il modo in cui il codice gira. Il `.py` è la fonte di verità e il
notebook lo **importa** — mai copia-incolla, mai `subprocess`.

```bash
source ~/pyvenv/bin/activate                       # sul Mac: conda activate mapd-covid
python Giulia/word_count.py ~/mapd-data/silver/paragraphs --out ~/mapd-out/word_count
```

**Dove gira lo decide `cluster.txt`, non il codice.** È la convenzione comune a tutti i
task, e nel codice non compare mai un IP:

| Cosa trova il codice | Cosa fa |
|---|---|
| `DASK_SCHEDULER` impostata | si collega a uno scheduler già acceso |
| `cluster.txt` (o `CORD19_HOSTS`) | avvia un `SSHCluster` sugli host elencati; **il primo è solo scheduler** |
| niente | `LocalCluster`, dimensionato sui core della macchina — sviluppo sul Mac |

All'avvio viene stampato un blocco con modalità, host, worker, thread e memoria:
**va letto**, perché è l'unica difesa contro un run che sembra "un cluster lento" mentre
in realtà sta girando su una macchina sola (§8.6).

### La pipeline di conversione — fase conclusa, qui per riferimento

`conversion_sanification.ipynb` ha prodotto il `silver/` e **non fa parte
dell'assignment**: non va rieseguita salvo che i dati debbano cambiare. Gira invariata su
Mac e cluster perché tutto passa da variabili d'ambiente:

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

Se un giorno va rieseguita, **si esegue headless su una copia usa-e-getta**, così il file
tracciato resta byte-pulito e `git pull` non confligge mai (il notebook committato è
output-free):

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

0. **Prima di proporre qualcosa, leggi `docs/DECISIONI.md`.** Le scelte già prese sono
   motivate e misurate: rimetterle in discussione senza un fatto nuovo fa buttare via una
   sessione di lavoro.
1. **La semplicità è un requisito, non un gusto.** Questo è un esercizio universitario da
   discutere all'orale: il codice va **saputo spiegare**, quindi la complessità che non si
   sa giustificare è un difetto e non una raffinatezza. C'è già un precedente costoso:
   `bench.py` è cresciuto oltre lo scopo del corso e va rifatto per questo motivo.
2. **Non rigenerare i dati.** `data/` sul Mac e `~/mapd-data/silver/` sul cluster sono
   l'output del run di conversione completo, già validato. Per provare la pipeline (se
   proprio serve): `CORD19_SAMPLE=N` → scrive in `data_sample/`.
3. **Niente numeri assoluti negli assert** (dump diversi danno conteggi diversi): solo
   garanzie strutturali; i check quantitativi sono già guardati da `if not SAMPLE`.
4. **Zero pandas lato driver:** il corpus non entra nel processo da cui lanci. Uniche
   raccolte ammesse (piccole e motivate): mappe di linkage, set `pmc_uids`, valori
   distinti dei paesi.
5. **Pitfall di dask-expr 2026.6** (tutti verificati sulla nostra versione):
   `value_counts().reset_index()` dentro un merge → `KeyError None` (usare
   `groupby.size().reset_index()`); `groupby.transform` dopo shuffle → errore di
   reindex (usare size+merge); `repartition(partition_size=...)` sotto un Client
   distribuito → **si impianta** (usare `row_group_size` + `split_row_groups`);
   lo **shuffle P2P** non riesce a ricostruire la propria spec quando il grafo nasce da
   un Bag (`RuntimeError: P2P … failed during transfer phase`) → forzare
   `dask.config.set({"dataframe.shuffle.method": "tasks"})`.
6. **Partizioni ≠ file.** `read_parquet` su `silver/paragraphs` restituisce **990
   partizioni da 1979 file**: l'ottimizzatore aggrega i file piccoli. Non è un dato
   mancante, ma i due numeri non vanno confusi quando si leggono i benchmark.
7. **Modello di memoria:** una lista costruita dentro una task NON è spillabile → il picco
   per-task si controlla con la granularità delle partizioni e con le thread, non con le
   soglie di spill. In Dask **Bag**, ogni operazione tipo-groupby (`foldby`,
   `frequencies`, `groupby`) collassa in **una sola partizione**: se la coda del calcolo è
   lenta e un solo worker lavora, è quello. Il DataFrame ha `split_out`, il Bag no.
8. **Portabilità:** mai hard-codare path o IP — gli host vivono in `cluster.txt`
   (git-ignored), i path relativi si risolvono sulla **radice della repo** e non sulla
   cartella da cui lanci. **Niente fallback silenziosi:** la configurazione del cluster va
   stampata sempre, perché un run su una macchina sola sembra un cluster lento e te ne
   accorgi a sessione bruciata.
9. **Niente di prezioso dentro la repo.** I risultati vanno in `~/mapd-out/`: sul cluster
   la repo si cancella e si riclona di continuo.
10. **Grafo piccolo:** i work-item del Bag sono **solo filename** (il path si ricostruisce
    nel worker); gli schemi Arrow si passano **espliciti** a `to_parquet` (aggira
    l'inferenza sulle partizioni all-null).
11. **Cluster:** non toccare il security group condiviso `pod-students`; macchine tutte
    della stessa taglia; la prima è solo scheduler; le VM **non** si cancellano a fine
    sessione (i risultati stanno sui loro dischi) ma i risultati si scaricano;
    `scripts/cluster_storage_up.sh` appartiene all'architettura NFS e non si lancia più.

---

## 9 · Stato attuale e prossimi passi

**Fatto:** esplorazione dei dati grezzi; architettura bronze/silver; conversione completata
sul corpus intero dopo tre round di fix OOM; dati validati e definitivi, oggi replicati su
ogni macchina; **task 1 (word count) finito e validato** sul corpus intero; architettura
del cluster rifatta (un cluster per persona, senza NFS).

**Prossimi passi, in ordine di importanza:**

1. **I benchmark obbligatori** (tempo vs numero di partizioni, tempo vs numero di worker).
   Senza, l'analisi è considerata incompleta dal corso. **L'impalcatura va rifatta da
   zero:** `bench.py` e `Giulia/bench_word_count.py` hanno accumulato complessità che il
   gruppo non è in grado di giustificare all'orale — non vanno estesi né usati come base.
   Prima si decide la forma della misura, poi si scrive il minimo che serve.
   Vincoli già noti e misurati: i benchmark si fanno **sul corpus vero** (sul campione una
   macchina batte quattro, perché a quella scala è tutto overhead); il sweep sulle
   partizioni usa `repartition(npartitions=k)` su una fetta **fissa** di dati, altrimenti
   si misura la quantità di dati invece del partizionamento; su `SSHCluster` il numero di
   worker si può solo ridurre rispetto alla lista di `cluster.txt`.
2. **Task 2.3.2 (paesi e istituti) da riscrivere:** l'unica versione esistente
   (`Giulia/old/scripts/task_2_3_2_affiliation_representation.py`) **non è distribuita** —
   è uno scan in un processo solo. In un esame di calcolo distribuito è il problema più
   grave che ci sia rimasto.
3. **Task 2.3.3 / 2.3.4 (title embeddings e cosine similarity):** esiste una prima
   soluzione in `daniele/`, documentata in `daniele/SOLUZIONE_2_3_3.md`.
4. **Applicare la convenzione `~/mapd-out`** ai default del codice: si fa insieme al
   rifacimento dei benchmark, perché tocca lo stesso file condiviso.
5. **Stop-word non inglesi** (direzione già scelta: dare al tedesco, francese, spagnolo e
   portoghese le loro liste invece di scartare i paper). Da fare dopo i benchmark.
