# Soluzione del punto 2.3.3 — Title embeddings (Dask)

Questa cartella (`MAPD-Project/daniele/`) contiene la soluzione del punto **2.3.3
"Obtaining Embeddings for Paper Titles"** dell'assignment. Tre file:

| File | Cosa è |
|---|---|
| `task_2_3_3_title_embeddings.ipynb` | Il notebook con la soluzione (codice e testo **in inglese**) |
| `requirements.txt` | Le versioni dei pacchetti usati — **già tutti installati** sulle VM, non serve installare nulla |
| `SOLUZIONE_2_3_3.md` | Questo documento |

---

## 1. Come si lancia (checklist)

1. Pusha questa cartella sul main; sullo scheduler aggiorna la copia del repo in `/data/MAPD-Project`.
2. **Aggiorna `cluster.txt`** nella root del repo **sulla VM** (è git-ignored, si edita
   direttamente lì — vedi §5): con gli IP attuali la riga è:
   ```
   10.67.22.118,10.67.22.206,10.67.22.53
   ```
   (primo host = scheduler, gli altri = worker; **non** ho messo lo scheduler anche
   come worker perché ha solo 3.8 GB e ospita già Jupyter + scheduler Dask).
3. Sullo scheduler: `bash scripts/cluster_storage_up.sh` (monta `/data` sui worker via NFS —
   ho verificato che al momento sui worker **non** è montato, quindi va rifatto a ogni sessione).
4. Avvia Jupyter dallo scheduler (dal venv: `source ~/pyvenv/bin/activate`), apri il
   notebook **dalla cartella del repo** (cwd = `/data/MAPD-Project/daniele` o la root del
   repo: `cluster.txt` viene cercato nella cwd e nella cartella padre) ed esegui le celle in ordine.
5. A fine lavoro esegui l'ultima cella (`client.close()` / `cluster.close()`).

Non c'è **nessun altro file da modificare** fuori da questa cartella: serve solo
l'aggiornamento di `cluster.txt` (punto 2), che comunque è git-ignored.

## 2. Struttura della soluzione e flusso logico

Il notebook è diviso in stage, ognuno cronometrato e campionato in memoria:

```
Stage A  silver/papers ──► filtro title_ok ──► tokenizzazione dei titoli
         (dd.read_parquet, solo 5 colonne)     (findall + explode + stop-word)
                                               ══► tokens = (cord_uid, word)   [persist]

Stage B  vocabolario = parole uniche dei titoli (al driver, ~10⁵ stringhe)
         modello .vec (4.5 GB) ──► dd.read_csv a blocchi da 64 MB
                               ──► filtro "tieni solo le parole del vocabolario"
                                               ══► model_f = (word, v0..v299)  [persist]

Stage C  tokens ⋈ model_f (inner join su word)
         ──► groupby(cord_uid).sum() + divisione = MEDIA dei vettori (mean pooling)
         ──► + title, is_title_unique   ──► Parquet in /data/output_2.3.3/run_N/
```

Scelte principali, e perché:

- **Input `silver/papers`** (vedi `DATA_DICTIONARY.md`): 406 211 righe, 9 partizioni.
  Uso `title_norm` (già minuscolo e ripulito) per la tokenizzazione e porto in output
  `title` e `is_title_unique`, che serviranno al punto 2.3.4 (i titoli duplicati danno
  similarità ~1 banali).
- **Tokenizzazione semplice**: `findall('[a-z]{2,}')` + explode + rimozione di ~60
  stop-word inglesi (lista hardcoded nel notebook: niente dipendenze extra tipo NLTK).
  Le parole che non esistono nel modello vengono **saltate** via inner join, come
  suggerisce l'assignment.
- **Output = un vettore da 300 numeri per titolo** (media dei vettori delle parole,
  "mean pooling"): è la rappresentazione "aggregated into a single vector" prevista
  dall'assignment, ha dimensione fissa ed è quella pratica per la cosine similarity
  del 2.3.4. La media è calcolata come **un'unica groupby-sum distribuita** (riduzione
  ad albero, stessa logica del `foldby` visto a lezione) più una divisione per il
  conteggio: un solo shuffle in tutto.
- **Formato output**: Parquet zstd, schema `cord_uid | title | is_title_unique |
  n_words | v0..v299` (`n_words` = quante parole del titolo hanno trovato un vettore).

## 3. La domanda importante: come fanno i worker a usare un modello da giga?

Risposta breve: **il modello non viene mai caricato in memoria, né sui worker né
altrove**. Va capito il cambio di prospettiva: il modello non è un "oggetto da
caricare", è un **dataset da leggere a pezzi** — esattamente come i dati.

Nel dettaglio:

1. Il file `/data/model/crawl-300d-2M-subword.vec` è **testo**: 2 milioni di righe
   `parola v1 v2 ... v300`. Sta sul volume, che i worker vedono via **NFS** allo
   stesso path (per questo serve `cluster_storage_up.sh`).
2. `dd.read_csv(..., blocksize="64MB")` lo spezza in **~70 blocchi da 64 MB**. Ogni
   blocco è una task: il worker che la esegue legge via NFS **solo quel blocco**, lo
   parsa (1 colonna stringa + 300 float32) e — prima che arrivi il blocco successivo —
   lo **filtra**, tenendo solo le righe la cui parola compare davvero nei titoli.
   Il picco di RAM per task è quindi ~100-150 MB, non 4.5 GB: i blocchi scartati
   vengono liberati subito.
3. Dei 2 000 000 di vettori ne sopravvivono solo quelli del **vocabolario dei titoli**
   (~10⁵): è questa "fetta" (~200 MB distribuiti tra i worker) l'unica cosa che resta
   in memoria (`persist`), ed è ciò che serve al join.
4. Quindi: la "piccola memoria locale dei worker" che hai visto (3.8 GB) ospita solo
   i blocchi in transito + la fetta filtrata del modello — mai il modello intero.

Nota sul file `.bin` (7.2 GB): quello sì andrebbe caricato **per intero** in RAM con
la libreria `fasttext` (è un modello binario completo, con le subword) — impossibile
sui nostri worker. È il motivo per cui la soluzione usa il `.vec`, che essendo testo
riga-per-riga si presta alla lettura distribuita. Stesso approccio del gruppo dello
scorso anno (loro con `wiki.en.vec` da 6.1 GB su worker da 4 GB).

### Il filtro "fast-isin" (collegamento col MEMORY_LEAK_REPORT)

Per filtrare i blocchi del modello serve un test "questa parola è nel vocabolario?"
contro un insieme di ~10⁵ stringhe. Il report del tuo collega
(`docs/MEMORY_LEAK_REPORT.md`) ha dimostrato — sullo stesso identico pattern, un
`isin` contro un set da 315k stringhe — che `Series.isin(set_python)` riconverte il
set **a ogni partizione**: lentezza (~0.7 s/task) + churn di memoria che l'allocatore
trattiene (il famoso "leak"). La cura documentata lì è quella adottata qui:

- il vocabolario è convertito **una volta sola sul driver** in `pyarrow.Array`;
- dentro ogni partizione il filtro usa il kernel vettoriale `pc.is_in`.

Dal report ho ripreso anche gli altri punti della "ricetta benchmark" (§7):
`MALLOC_TRIM_THRESHOLD_=0` e `MALLOC_ARENA_MAX=2` impostati via
`pre-spawn-environ` **prima** di creare il cluster (dopo non ha effetto), e la
`sweep()` (gc + pool Arrow + `malloc_trim`) chiamata **tra** gli stage, mai dentro
le regioni cronometrate.

## 4. Benchmark: cosa misura e come si usa

- Ogni stage è cronometrato (`timings`) e campionato con **`MemorySampler`**
  (memoria dell'intero cluster nel tempo, una curva per stage).
- **Tutti gli output finiscono sul volume**, mai nel repo, in una cartella che
  incorpora automaticamente il numero di worker rilevato dal client:
  ```
  /data/output_2.3.3/
  ├── run_2workers/
  │   ├── embeddings/          # il risultato (Parquet, 8 parti)
  │   ├── timings.csv          # tempi per stage
  │   ├── summary.json         # metadati del run (coverage, dimensioni, config)
  │   └── memory_usage.png     # memoria del cluster per stage
  └── scaling_workers.png      # confronto tra i run (generato dalla cella §10)
  ```
- **Per misurare lo scaling**: esegui il notebook com'è (2 worker) → poi togli un IP
  da `cluster.txt`, riavvia il kernel e riesegui → nasce `run_1workers/` accanto a
  `run_2workers/`. La cella §10 raccoglie tutti i `timings.csv` presenti e produce il
  grafico a barre tempo-per-stage vs n. worker. Se in futuro aggiungete una VM, basta
  aggiungere l'IP: la struttura si adatta da sola (`run_3workers/`, ...).
- La cella §9 è un **sanity check** qualitativo: due titoli sul coronavirus devono
  avere similarità coseno nettamente più alta di una coppia scorrelata.

## 5. Cose da sapere / limiti

- **`cluster.txt` sulla VM è vecchio** (l'ho verificato: contiene
  `10.67.22.205,224,113,234,100`): va sostituito con la riga del §1 punto 2, altrimenti
  `SSHCluster` prova a collegarsi a macchine che non esistono più.
- Il notebook è **idempotente**: rieseguire la cella del cluster chiude il precedente;
  rieseguire il run sovrascrive la stessa `run_Nworkers/` (`overwrite=True`).
- Se `cluster.txt` non c'è (es. prova locale), parte un `LocalCluster` di fallback.
- La copertura attesa non è il 100%: titoli senza parole nel modello (es. titoli non
  inglesi) non producono un embedding; i numeri esatti finiscono in `summary.json`
  (`titles_coverage`, `token_coverage`).
- Il modello contiene anche parole con maiuscole; noi tokenizziamo da `title_norm`
  (minuscolo), quindi facciamo match solo con le entrate minuscole del modello — è la
  scelta standard e coerente col pre-processing del silver.
